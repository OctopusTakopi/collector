//! The collector's modules, as a library for `src/main.rs`; nothing else
//! depends on it.

mod binance;
mod books;
mod bybit;
mod coinbase;
pub mod collection;
mod dedup;
mod error;
mod feed;
pub mod file;
pub mod handover;
mod hyperliquid;
mod kraken_futures;
mod kraken_spot;
mod listing;
pub mod marker;
pub mod quality;
pub mod readiness;
pub mod routing;
mod session;
mod snapshot;
pub mod symbol;
mod throttler;
mod ws;
