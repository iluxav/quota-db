use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tracing::{error, info};

use crate::config::Config;
use crate::engine::ShardedDb;
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
                let connection = Connection::new(socket);
                let mut handler = if let Some(rep) = replication {
                    Handler::with_replication(connection, db, rep)
                } else {
                    Handler::new(connection, db)
                };

                if let Err(e) = handler.run().await {
                    error!("Connection error from {}: {}", addr, e);
                }

                // Permit is dropped here, releasing the semaphore
                drop(permit);
            });
        }
    }
}
