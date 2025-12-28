use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use quota_db::config::Config;
use quota_db::engine::ShardedDb;
use quota_db::metrics::{run_metrics_server, METRICS};
use quota_db::persistence::PersistenceManager;
use quota_db::replication::{ReplicationConfig, ReplicationManager};
use quota_db::server::Listener;

use tokio::sync::broadcast;
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse configuration
    let config = Config::parse_args();

    // Initialize tracing
    let log_level = match config.log_level.as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(log_level)
        .with_target(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Initialize metrics
    METRICS.init_start_time();

    info!("QuotaDB v{}", env!("CARGO_PKG_VERSION"));
    info!(
        "Configuration: {} shards, node_id={}, max_connections={}",
        config.shards, config.node_id, config.max_connections
    );

    // Get persistence config
    let persistence_config = config.persistence_config();

    // Create persistence manager and handle if enabled
    let (persistence_manager, persistence_handle) = if persistence_config.enabled {
        info!(
            "Persistence enabled, data dir: {:?}",
            persistence_config.data_dir
        );
        let (manager, handle) =
            PersistenceManager::new(persistence_config.clone(), config.shards);
        (Some(manager), Some(handle))
    } else {
        (None, None)
    };

    // Create the sharded database (with recovery if enabled)
    let db = Arc::new(ShardedDb::with_persistence(
        &config,
        &persistence_config,
        persistence_handle,
    ));

    // Start TTL expiration task if enabled
    if config.enable_ttl {
        let db_clone = db.clone();
        let tick_interval = Duration::from_millis(config.ttl_tick_ms);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tick_interval);
            loop {
                interval.tick().await;
                let current_ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                let expired = db_clone.expire_all(current_ts);
                if expired > 0 {
                    tracing::debug!("Expired {} entries", expired);
                }
            }
        });
    }

    // Start persistence manager if enabled
    if let Some(mut manager) = persistence_manager {
        let db_clone = db.clone();
        tokio::spawn(async move {
            // Create a closure that can get shard snapshots from the database
            let get_snapshot = move |shard_id: u16| db_clone.create_persistence_snapshot(shard_id);
            manager.run(get_snapshot).await;
        });
        info!("Persistence manager started");
    }

    // Set up replication if peers are configured
    let replication_handle = if !config.peers.is_empty() {
        // Filter out empty peer strings
        let peers: Vec<String> = config
            .peers
            .iter()
            .filter(|p| !p.is_empty())
            .cloned()
            .collect();

        if !peers.is_empty() {
            info!("Replication enabled with {} peers: {:?}", peers.len(), peers);

            // Resolve peer hostnames to addresses
            let mut peer_addrs = Vec::new();
            for peer in &peers {
                match tokio::net::lookup_host(peer).await {
                    Ok(mut addrs) => {
                        if let Some(addr) = addrs.next() {
                            info!("Resolved peer {} -> {}", peer, addr);
                            peer_addrs.push(addr);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to resolve peer {}: {}", peer, e);
                    }
                }
            }

            let rep_config = ReplicationConfig {
                node_id: config.node_id(),
                listen_port: config.replication_port,
                peers: peer_addrs,
                num_shards: config.shards,
                batch_max_size: config.batch_max_size,
                batch_max_delay: Duration::from_millis(config.batch_max_delay_ms),
                channel_capacity: config.replication_channel_size,
            };

            let (mut manager, handle) = ReplicationManager::new(rep_config);

            // Set db reference for string replication and other operations
            manager.set_db(db.clone());

            // Create apply_delta callback
            let db_clone = db.clone();
            let apply_delta = Arc::new(move |shard_id: u16, delta: quota_db::replication::Delta| {
                db_clone.apply_delta(shard_id, &delta);
            });

            // Start replication manager
            tokio::spawn(async move {
                manager.run(apply_delta).await;
            });

            Some(handle)
        } else {
            None
        }
    } else {
        info!("Replication disabled (no peers configured)");
        None
    };

    // Create shutdown broadcast channel
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    // Create and run the listener
    let listener = Listener::bind(&config, db.clone()).await?;
    let listener = if let Some(handle) = replication_handle {
        listener.with_replication(handle)
    } else {
        listener
    };

    info!("Ready to accept connections");

    // Spawn the listener task
    let listener_shutdown = shutdown_tx.subscribe();
    let listener_handle = tokio::spawn(async move {
        listener.run_with_shutdown(listener_shutdown).await
    });

    // Start metrics server if enabled
    if config.metrics {
        let metrics_addr = config.metrics_addr();
        tokio::spawn(async move {
            if let Err(e) = run_metrics_server(metrics_addr).await {
                warn!("Metrics server error: {}", e);
            }
        });
    }

    // Wait for shutdown signal
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received SIGINT (Ctrl+C), initiating graceful shutdown...");
        }
        _ = async {
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                let mut sigterm = signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
                sigterm.recv().await;
            }
            #[cfg(not(unix))]
            {
                // On non-Unix, just wait forever (ctrl_c will handle it)
                std::future::pending::<()>().await;
            }
        } => {
            info!("Received SIGTERM, initiating graceful shutdown...");
        }
    }

    // Signal shutdown to all components
    let _ = shutdown_tx.send(());

    // Give components time to gracefully shutdown
    info!("Waiting for connections to close (timeout: 5s)...");
    let shutdown_timeout = Duration::from_secs(5);

    match tokio::time::timeout(shutdown_timeout, listener_handle).await {
        Ok(result) => {
            match result {
                Ok(Ok(())) => info!("Server shut down gracefully"),
                Ok(Err(e)) => warn!("Server error during shutdown: {}", e),
                Err(e) => warn!("Server task panicked: {}", e),
            }
        }
        Err(_) => {
            warn!("Shutdown timed out, forcing exit");
        }
    }

    // Final cleanup: flush any remaining data
    info!("Performing final data sync...");
    // The ShardedDb will handle persistence flushing on drop

    info!("Shutdown complete");
    Ok(())
}
