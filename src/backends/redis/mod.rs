//! Internal Redis Streams backend implementation.
//! Public surface lives in [`crate::redis`].

#![allow(unused_imports)] // Re-exports are consumed by tasks added in subsequent PRs.

mod autoscaler;
mod backend;
pub(super) mod client;
pub(super) mod constants;
mod consumer;
mod consumer_group;
mod maintenance;
mod publisher;
mod reaper;
mod requeue;
mod stream_id;
mod topology;

pub use autoscaler::{
    RedisAutoscalerBackend, RedisQueueStats, RedisQueueStatsProvider, XlenStatsProvider,
};
pub use client::{RedisClient, RedisConfig, RedisMode};
pub use consumer::RedisConsumer;
pub use consumer_group::{
    RedisConsumerGroup, RedisConsumerGroupConfig, RedisConsumerGroupRegistry,
};
pub use publisher::{RedisPublisher, shard_for_key};
#[doc(hidden)]
pub use reaper::spawn_reaper;
pub(crate) use requeue::{HoldEntry, enqueue_hold, spawn_requeuer};
pub use topology::RedisTopologyDeclarer;
