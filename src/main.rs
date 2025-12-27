use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use quota_db::config::Config;
use quota_db::engine::ShardedDb;
use quota_db::metrics::METRICS;
use quota_db::persistence::PersistenceManager;
use quota_db::replication::{ReplicationConfig, ReplicationManager};
use quota_db::server::Listener;

use tracing::{info, Level};
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

            let (manager, handle) = ReplicationManager::new(rep_config);

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

    // Create and run the listener
    let listener = Listener::bind(&config, db).await?;
    let listener = if let Some(handle) = replication_handle {
        listener.with_replication(handle)
    } else {
        listener
    };

    info!("Ready to accept connections");
    listener.run().await?;

    Ok(())
}
