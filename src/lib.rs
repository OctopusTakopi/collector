//! Websocket recording plumbing, exposed as a library so other binaries can
//! reuse the connection, redundancy and readiness machinery without the file
//! writer.
//!
//! The venue modules (`binance*`, `bybit`, `hyperliquid`) are public because
//! the collector binary drives them; the pieces meant for reuse elsewhere are
//! [`ws`], [`dedup`], [`readiness`], [`quality`] and
//! [`hyperliquid::run_connection`].

use std::time::Duration;

pub mod binance;
pub mod binance_market;
pub mod binancefuturescm;
pub mod binancefuturesum;
pub mod bybit;
pub mod dedup;
pub mod error;
pub mod feed;
pub mod file;
pub mod handover;
pub mod hyperliquid;
pub mod quality;
pub mod readiness;
pub mod routing;
pub mod symbol;
pub mod throttler;
pub mod ws;

/// Per connection: the queue is shared by all of them, so redundancy does not
/// shrink the burst each one can absorb.
pub const WS_QUEUE_CAPACITY: usize = 16_384;
/// Redundant connections are opened this far apart.
///
/// Simultaneous handshakes are what a venue's connection rate limit notices,
/// and connections opened together are the ones most likely to be recycled
/// together — which is exactly the correlation redundancy is meant to avoid.
pub const CONNECT_STAGGER: Duration = Duration::from_secs(5);
