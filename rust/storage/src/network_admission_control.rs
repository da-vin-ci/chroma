use crate::config::{CountBasedPolicyConfig, StorageAdmissionConfig};

use super::{GetError, Storage};
use async_trait::async_trait;
use chroma_config::Configurable;
use chroma_error::{ChromaError, ErrorCodes};
use futures::{future::Shared, FutureExt, StreamExt, TryFutureExt};
use parking_lot::Mutex;
use std::{collections::HashMap, future::Future, marker::PhantomData, pin::Pin, sync::Arc};
use thiserror::Error;
use tokio::sync::{Semaphore, SemaphorePermit};
use tracing::{Instrument, Span};

#[derive(Clone)]
pub struct NetworkAdmissionControl {
    storage: Storage,
    outstanding_requests: Arc<
        Mutex<
            HashMap<
                String,
                Shared<
                    Pin<
                        Box<
                            dyn Future<Output = Result<(), Box<NetworkAdmissionControlError>>>
                                + Send
                                + 'static,
                        >,
                    >,
                >,
            >,
        >,
    >,
    rate_limiter: Arc<RateLimitPolicy>,
}

#[derive(Error, Debug, Clone)]
pub enum NetworkAdmissionControlError {
    #[error("Error performing a get call from storage {0}")]
    StorageGetError(#[from] GetError),
    #[error("IO Error")]
    IOError,
    #[error("Error deserializing to block")]
    DeserializationError,
}

impl ChromaError for NetworkAdmissionControlError {
    fn code(&self) -> ErrorCodes {
        match self {
            NetworkAdmissionControlError::StorageGetError(e) => e.code(),
            NetworkAdmissionControlError::IOError => ErrorCodes::Internal,
            NetworkAdmissionControlError::DeserializationError => ErrorCodes::Internal,
        }
    }
}

impl NetworkAdmissionControl {
    pub fn new_with_default_policy(storage: Storage) -> Self {
        Self {
            storage,
            outstanding_requests: Arc::new(Mutex::new(HashMap::new())),
            rate_limiter: Arc::new(RateLimitPolicy::CountBasedPolicy(CountBasedPolicy::new(15))),
        }
    }
    pub fn new(storage: Storage, policy: RateLimitPolicy) -> Self {
        Self {
            storage,
            outstanding_requests: Arc::new(Mutex::new(HashMap::new())),
            rate_limiter: Arc::new(policy),
        }
    }

    pub async fn read_from_storage<F, R>(
        storage: Storage,
        key: String,
        f: F,
    ) -> Result<(), Box<NetworkAdmissionControlError>>
    where
        R: Future<Output = Result<(), Box<NetworkAdmissionControlError>>> + Send + 'static,
        F: (FnOnce(Vec<u8>) -> R) + Send + 'static,
    {
        let stream = storage
            .get(&key)
            .instrument(tracing::trace_span!(parent: Span::current(), "Storage get"))
            .await;
        match stream {
            Ok(mut bytes) => {
                let read_block_span =
                    tracing::trace_span!(parent: Span::current(), "Read bytes to end");
                let buf = read_block_span
                    .in_scope(|| async {
                        let mut buf: Vec<u8> = Vec::new();
                        while let Some(res) = bytes.next().await {
                            match res {
                                Ok(chunk) => {
                                    buf.extend(chunk);
                                }
                                Err(e) => {
                                    tracing::error!("Error reading from storage: {}", e);
                                    return Err(Box::new(
                                        NetworkAdmissionControlError::StorageGetError(e),
                                    ));
                                }
                            }
                        }
                        Ok(Some(buf))
                    })
                    .await?;
                let buf = match buf {
                    Some(buf) => buf,
                    None => {
                        // Buffer is empty. Nothing interesting to do.
                        return Ok(());
                    }
                };
                tracing::info!("Read {:?} bytes from s3", buf.len());
                return f(buf).await;
            }
            Err(e) => {
                tracing::error!("Error reading from storage: {}", e);
                return Err(Box::new(NetworkAdmissionControlError::StorageGetError(e)));
            }
        }
    }

    async fn enter(&self) -> SemaphorePermit<'_> {
        match &*self.rate_limiter {
            RateLimitPolicy::CountBasedPolicy(policy) => {
                return policy.acquire().await;
            }
        }
    }

    async fn exit(&self, permit: SemaphorePermit<'_>) {
        match &*self.rate_limiter {
            RateLimitPolicy::CountBasedPolicy(policy) => {
                policy.drop(permit).await;
            }
        }
    }

    pub async fn get<F, R>(
        &self,
        key: String,
        f: F,
    ) -> Result<(), Box<NetworkAdmissionControlError>>
    where
        R: Future<Output = Result<(), Box<NetworkAdmissionControlError>>> + Send + 'static,
        F: (FnOnce(Vec<u8>) -> R) + Send + 'static,
    {
        // Wait for permit.
        let permit = self.enter().await;
        let future_to_await;
        {
            let mut requests = self.outstanding_requests.lock();
            let maybe_inflight = requests.get(&key).map(|fut| fut.clone());
            future_to_await = match maybe_inflight {
                Some(fut) => fut,
                None => {
                    let get_storage_future = NetworkAdmissionControl::read_from_storage(
                        self.storage.clone(),
                        key.clone(),
                        f,
                    )
                    .boxed()
                    .shared();
                    requests.insert(key.clone(), get_storage_future.clone());
                    get_storage_future
                }
            };
        }
        let res = future_to_await.await;
        {
            let mut requests = self.outstanding_requests.lock();
            requests.remove(&key);
        }
        // Release permit.
        self.exit(permit).await;
        res
    }
}

// Prefer enum dispatch over dyn since there could
// only be a handful of these policies.
#[derive(Debug)]
enum RateLimitPolicy {
    CountBasedPolicy(CountBasedPolicy),
}

#[derive(Debug)]
struct CountBasedPolicy {
    max_allowed_outstanding: usize,
    remaining_tokens: Semaphore,
}

impl CountBasedPolicy {
    fn new(max_allowed_outstanding: usize) -> Self {
        Self {
            max_allowed_outstanding,
            remaining_tokens: Semaphore::new(max_allowed_outstanding),
        }
    }
    async fn acquire(&self) -> SemaphorePermit<'_> {
        let token_res = self.remaining_tokens.acquire().await;
        match token_res {
            Ok(token) => {
                return token;
            }
            Err(e) => panic!("AcquireToken Failed {}", e),
        }
    }
    async fn drop(&self, permit: SemaphorePermit<'_>) {
        drop(permit);
    }
}

pub async fn from_config(
    config: &StorageAdmissionConfig,
    storage: Storage,
) -> Result<NetworkAdmissionControl, Box<dyn ChromaError>> {
    match &config {
        StorageAdmissionConfig::CountBasedPolicy(policy) => Ok(NetworkAdmissionControl::new(
            storage,
            RateLimitPolicy::CountBasedPolicy(CountBasedPolicy::try_from_config(policy).await?),
        )),
    }
}

#[async_trait]
impl Configurable<CountBasedPolicyConfig> for CountBasedPolicy {
    async fn try_from_config(
        config: &CountBasedPolicyConfig,
    ) -> Result<Self, Box<dyn ChromaError>> {
        Ok(Self::new(config.max_concurrent_requests))
    }
}
