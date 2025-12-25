use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use quota_db::config::Config;
use quota_db::engine::ShardedDb;
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

    info!("QuotaDB v{}", env!("CARGO_PKG_VERSION"));
    info!(
        "Configuration: {} shards, node_id={}, max_connections={}",
        config.shards, config.node_id, config.max_connections
    );

    // Create the sharded database
    let db = Arc::new(ShardedDb::new(&config));

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

    // Create and run the listener
    let listener = Listener::bind(&config, db).await?;

    info!("Ready to accept connections");
    listener.run().await?;

    Ok(())
}
