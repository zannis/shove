//! Optional codec implementations gated behind cargo features.
//!
//! The default codecs (`JsonCodec`, `RawBytesCodec`) live in
//! [`crate::codec`] and are always available. This module hosts codecs that
//! pull in optional dependencies — enable the corresponding feature to use
//! them.

#[cfg(feature = "protobuf")]
#[cfg_attr(docsrs, doc(cfg(feature = "protobuf")))]
pub mod protobuf;

#[cfg(feature = "sbe")]
#[cfg_attr(docsrs, doc(cfg(feature = "sbe")))]
pub mod sbe;
