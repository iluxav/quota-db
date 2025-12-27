use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Semaphore};
use tracing::{debug, error, info};

use crate::config::Config;
use crate::engine::ShardedDb;
use crate::metrics::METRICS;
use crate::replication::ReplicationHandle;
use crate::server::{Connection, Handler};

/// TCP listener that accepts connections and spawns handlers.
pub struct Listener {
    listener: TcpListener,
    db: Arc<ShardedDb>,
    connection_limit: Arc<Semaphore>,
    replication: Option<ReplicationHandle>,
}

impl Listener {
    /// Create a new Listener bound to the configured address.
    pub async fn bind(config: &Config, db: Arc<ShardedDb>) -> std::io::Result<Self> {
        let listener = TcpListener::bind(config.bind).await?;
        info!("Listening on {}", config.bind);

        Ok(Self {
            listener,
            db,
            connection_limit: Arc::new(Semaphore::new(config.max_connections)),
            replication: None,
        })
    }

    /// Set the replication handle for outgoing replication.
    pub fn with_replication(mut self, handle: ReplicationHandle) -> Self {
        self.replication = Some(handle);
        self
    }

    /// Run the accept loop, spawning a handler for each connection.
    pub async fn run(&self) -> std::io::Result<()> {
        loop {
            // Acquire a permit before accepting
            let permit = self
                .connection_limit
                .clone()
                .acquire_owned()
                .await
                .expect("semaphore closed");

            let (socket, addr) = self.listener.accept().await?;

            // Enable TCP_NODELAY for lower latency
            if let Err(e) = socket.set_nodelay(true) {
                error!("Failed to set TCP_NODELAY: {}", e);
            }

            let db = self.db.clone();
            let replication = self.replication.clone();

            tokio::spawn(async move {
                METRICS.connection_opened();

                let connection = Connection::new(socket);
                let mut handler = if let Some(rep) = replication {
                    Handler::with_replication(connection, db, rep)
                } else {
                    Handler::new(connection, db)
                };

                if let Err(e) = handler.run().await {
                    error!("Connection error from {}: {}", addr, e);
                }

                METRICS.connection_closed();
                // Permit is dropped here, releasing the semaphore
                drop(permit);
            });
        }
    }

    /// Run the accept loop with shutdown signal support.
    /// Stops accepting new connections when shutdown is received,
    /// and waits for existing connections to complete.
    pub async fn run_with_shutdown(
        self,
        mut shutdown: broadcast::Receiver<()>,
    ) -> std::io::Result<()> {
        // Track active connections for graceful shutdown
        let active_connections = Arc::new(tokio::sync::Notify::new());

        loop {
            tokio::select! {
                biased;

                // Check for shutdown signal first
                _ = shutdown.recv() => {
                    info!("Shutdown signal received, stopping new connections");
                    break;
                }

                // Try to acquire permit and accept connection
                result = async {
                    let permit = self
                        .connection_limit
                        .clone()
                        .acquire_owned()
                        .await
                        .expect("semaphore closed");

                    let accept_result = self.listener.accept().await;
                    (permit, accept_result)
                } => {
                    let (permit, accept_result) = result;
                    let (socket, addr) = accept_result?;

                    // Enable TCP_NODELAY for lower latency
                    if let Err(e) = socket.set_nodelay(true) {
                        error!("Failed to set TCP_NODELAY: {}", e);
                    }

                    let db = self.db.clone();
                    let replication = self.replication.clone();
                    let notify = active_connections.clone();

                    tokio::spawn(async move {
                        METRICS.connection_opened();

                        let connection = Connection::new(socket);
                        let mut handler = if let Some(rep) = replication {
                            Handler::with_replication(connection, db, rep)
                        } else {
                            Handler::new(connection, db)
                        };

                        if let Err(e) = handler.run().await {
                            debug!("Connection closed from {}: {}", addr, e);
                        }

                        METRICS.connection_closed();
                        // Permit is dropped here, releasing the semaphore
                        drop(permit);
                        // Notify that a connection finished
                        notify.notify_one();
                    });
                }
            }
        }

        // Wait for existing connections to drain (check via semaphore)
        // If all permits are available, no connections are active
        let max_permits = self.connection_limit.available_permits();
        let total_permits = max_permits; // This is approximate

        info!(
            "Waiting for {} active connections to finish",
            total_permits.saturating_sub(self.connection_limit.available_permits())
        );

        // Wait with timeout for connections to drain
        let drain_timeout = tokio::time::Duration::from_secs(4);
        let _ = tokio::time::timeout(drain_timeout, async {
            loop {
                // Check if all permits are available
                if self.connection_limit.available_permits() >= total_permits {
                    break;
                }
                // Wait for a connection to finish
                active_connections.notified().await;
            }
        })
        .await;

        let remaining = total_permits.saturating_sub(self.connection_limit.available_permits());
        if remaining > 0 {
            info!("{} connections did not finish in time", remaining);
        }

        Ok(())
    }
}
