use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::consumer::{Consumer, ConsumerOptions};
use crate::error::Result;
use crate::handler::MessageHandler;
use crate::topic::{SequencedTopic, Topic};

use super::client::NatsClient;
use super::consumer::NatsConsumer;

pub struct NatsConsumerGroupConfig {
    pub prefetch_count: u16,
    pub min_consumers: u16,
    pub max_consumers: u16,
    pub max_retries: u32,
    pub handler_timeout: Option<Duration>,
    pub concurrent_processing: bool,
    pub max_pending_per_key: Option<usize>,
}

impl Default for NatsConsumerGroupConfig {
    fn default() -> Self {
        Self {
            prefetch_count: 10,
            min_consumers: 1,
            max_consumers: 4,
            max_retries: 10,
            handler_timeout: None,
            concurrent_processing: true,
            max_pending_per_key: None,
        }
    }
}

pub struct NatsConsumerGroup {
    client: NatsClient,
    config: NatsConsumerGroupConfig,
}

impl NatsConsumerGroup {
    pub fn new(client: NatsClient, config: NatsConsumerGroupConfig) -> Self {
        Self { client, config }
    }

    pub async fn run<T: Topic>(
        &self,
        handler: impl MessageHandler<T>,
        shutdown: CancellationToken,
    ) -> Result<()> {
        let handler = std::sync::Arc::new(handler);
        let mut tasks = Vec::new();

        for _ in 0..self.config.min_consumers {
            let consumer = NatsConsumer::new(self.client.clone());
            let handler = handler.clone();
            let options = ConsumerOptions::new(shutdown.clone())
                .with_max_retries(self.config.max_retries)
                .with_prefetch_count(self.config.prefetch_count);
            let options = match self.config.handler_timeout {
                Some(timeout) => options.with_handler_timeout(timeout),
                None => options,
            };

            tasks.push(tokio::spawn(async move {
                if let Err(e) = consumer.run::<T>(handler, options).await {
                    tracing::error!(error = %e, "consumer group member exited with error");
                }
            }));
        }

        for task in tasks {
            let _ = task.await;
        }

        Ok(())
    }

    pub async fn run_fifo<T: SequencedTopic>(
        &self,
        handler: impl MessageHandler<T>,
        shutdown: CancellationToken,
    ) -> Result<()> {
        let handler = std::sync::Arc::new(handler);
        let mut tasks = Vec::new();

        for _ in 0..self.config.min_consumers {
            let consumer = NatsConsumer::new(self.client.clone());
            let handler = handler.clone();
            let mut options = ConsumerOptions::new(shutdown.clone())
                .with_max_retries(self.config.max_retries)
                .with_prefetch_count(self.config.prefetch_count);
            if let Some(timeout) = self.config.handler_timeout {
                options = options.with_handler_timeout(timeout);
            }
            if let Some(limit) = self.config.max_pending_per_key {
                options = options.with_max_pending_per_key(limit);
            }

            tasks.push(tokio::spawn(async move {
                if let Err(e) = consumer.run_fifo::<T>(handler, options).await {
                    tracing::error!(error = %e, "consumer group FIFO member exited with error");
                }
            }));
        }

        for task in tasks {
            let _ = task.await;
        }

        Ok(())
    }
}