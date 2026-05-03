//! Internal Redis Streams backend implementation.
//! Public surface lives in [`crate::redis`].

#![allow(unused_imports)] // Re-exports are consumed by tasks added in subsequent PRs.

pub(super) mod constants;
pub(super) mod client;
mod topology;
mod publisher;
mod requeue;
mod consumer;

pub use client::{RedisClient, RedisConfig, RedisMode};
pub use topology::RedisTopologyDeclarer;
pub use publisher::{RedisPublisher, shard_for_key};
pub(crate) use requeue::{HoldEntry, enqueue_hold, spawn_requeuer};
pub use consumer::RedisConsumer;

// Sub-modules added in subsequent tasks:
// mod autoscaler;
// mod backend;
// mod consumer_group;
