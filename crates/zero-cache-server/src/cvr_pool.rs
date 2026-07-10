//! Shared bounded PostgreSQL pool for the CVR store.
//!
//! Official zero-cache creates one CVR database pool per sync worker and passes
//! that shared pool to every client-group view-syncer. It does not retain one
//! PostgreSQL connection per WebSocket. This is the equivalent lifecycle for
//! the Rust server's single sync-service process: connections are acquired for
//! CVR load/flush work and returned immediately afterward.

use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_postgres::Client;

#[derive(Clone)]
pub struct CvrPool {
    inner: Arc<Inner>,
}

struct Inner {
    connection_string: String,
    idle: Mutex<Vec<Client>>,
    permits: Arc<Semaphore>,
    max_size: usize,
}

impl CvrPool {
    pub fn new(connection_string: impl Into<String>, max_size: usize) -> Self {
        let max_size = max_size.max(1);
        Self {
            inner: Arc::new(Inner {
                connection_string: connection_string.into(),
                idle: Mutex::new(Vec::with_capacity(max_size)),
                permits: Arc::new(Semaphore::new(max_size)),
                max_size,
            }),
        }
    }

    pub fn max_size(&self) -> usize {
        self.inner.max_size
    }

    pub async fn get(&self) -> Result<PooledCvrClient, String> {
        let permit = self
            .inner
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| "CVR connection pool closed".to_string())?;

        loop {
            let client = self
                .inner
                .idle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .pop();
            match client {
                Some(client) if !client.is_closed() => {
                    return Ok(PooledCvrClient {
                        client: Some(client),
                        pool: self.clone(),
                        _permit: permit,
                    });
                }
                Some(_) => continue,
                None => {
                    let client = zero_cache_change_source::pg_connection::connect(
                        &self.inner.connection_string,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                    return Ok(PooledCvrClient {
                        client: Some(client),
                        pool: self.clone(),
                        _permit: permit,
                    });
                }
            }
        }
    }
}

pub struct PooledCvrClient {
    client: Option<Client>,
    pool: CvrPool,
    _permit: OwnedSemaphorePermit,
}

impl Deref for PooledCvrClient {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        self.client.as_ref().expect("pooled CVR client missing")
    }
}

impl DerefMut for PooledCvrClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.client.as_mut().expect("pooled CVR client missing")
    }
}

impl Drop for PooledCvrClient {
    fn drop(&mut self) {
        if let Some(client) = self.client.take().filter(|client| !client.is_closed()) {
            self.pool
                .inner
                .idle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(client);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_uses_the_configured_bound() {
        let pool = CvrPool::new("host=unused", 30);
        assert_eq!(pool.max_size(), 30);
        assert_eq!(pool.inner.permits.available_permits(), 30);
    }
}
