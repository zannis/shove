//! Internal Redis Streams backend implementation.
//! Public surface lives in [`crate::redis`].

#![allow(unused_imports)] // Re-exports are consumed by tasks added in subsequent PRs.

pub(super) mod constants;
pub(super) mod client;

pub use client::{RedisClient, RedisConfig, RedisMode};

// Sub-modules added in subsequent tasks:
// mod autoscaler;
// mod backend;
// mod consumer;
// mod consumer_group;
// mod publisher;
// mod requeue;
// mod topology;
