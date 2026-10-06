//! Coinbase Exchange spot over the public websocket feed.
//!
//! Trades (`matches`), `ticker` and `heartbeat` carry the product's
//! `sequence`, so each connection's copy of one is kept once. The public book
//! is batched on each connection's own 50 ms clock (checked live 2026-10-05,
//! 28 of 241 batches alike across two connections), so it is recorded from
//! one connection at a time; see [`crate::books`].

mod connection;

use std::{future::Future, sync::Arc};

use jiff::Timestamp;
use rustls::ClientConfig;
use tokio::task::JoinSet;

use crate::{
    books::Books,
    collection::{self, Context, Recorder, Venue},
    error::ConnectorError,
    routing,
    session::{Frame, Slot},
};

pub(crate) use connection::BOOK;

pub(crate) async fn collect(
    channels: &'static [&'static str],
    products: &[String],
    cx: Context,
    tls: Arc<ClientConfig>,
) -> anyhow::Result<()> {
    let products: Arc<[String]> = products
        .iter()
        .map(|product| product.to_ascii_uppercase())
        .collect();
    let coinbase = Coinbase {
        recorder: Recorder::new(cx.writer_tx.clone(), &products, cx.connections),
        books: Books::new(
            sources(&products),
            cx.connections.get(),
            cx.quality.clone(),
            Timestamp::now(),
        ),
        products,
        channels,
    };
    collection::run(coinbase, cx, tls, JoinSet::new()).await
}

/// Each product's name in quality reports.
fn sources(products: &[String]) -> Vec<String> {
    products
        .iter()
        .map(|product| format!("coinbase/{}", product.to_ascii_lowercase()))
        .collect()
}

/// The consumer of Coinbase's queue, which carries market data only.
struct Coinbase {
    products: Arc<[String]>,
    channels: &'static [&'static str],
    recorder: Recorder,
    books: Books,
}

impl Venue for Coinbase {
    fn label(&self) -> &'static str {
        "coinbase"
    }

    fn keep_connection(&self, slot: Slot) -> impl Future<Output = ()> + Send + 'static {
        connection::keep_connection(
            Arc::clone(&self.products),
            self.channels,
            self.books.lifecycle(),
            slot,
        )
    }

    async fn handle(&mut self, frame: Frame) -> Result<(), ConnectorError> {
        self.books.catch_up(frame.recv_time);
        let route = routing::coinbase_route(&frame.data)?;
        let Some(product_id) = route.product_id else {
            return Ok(());
        };
        let book = self
            .products
            .iter()
            .position(|product| product == product_id);
        let keep = match (route.kind, book) {
            (kind @ ("snapshot" | "l2update"), Some(product)) => self.books.admit(
                frame.socket.connection,
                product,
                kind == "snapshot",
                frame.recv_time,
            ),
            _ => match routing::coinbase_sequence_key(&route) {
                Some(key) => !self.recorder.dedup.is_duplicate_key(key),
                None => !self.recorder.dedup.is_duplicate(&frame.data),
            },
        };
        if !keep {
            return Ok(());
        }
        let symbol = self.recorder.symbols.resolve(product_id);
        self.recorder
            .write(frame.recv_time, symbol, frame.data)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use bytes::Bytes;
    use tokio::sync::mpsc::{Receiver, channel};

    use super::*;
    use crate::{books::Announced, file::WriteRecord, quality::QualityReporter};

    const CHANNELS: &[&str] = &[BOOK, "matches", "ticker", "heartbeat"];

    fn at(time: &str) -> Timestamp {
        time.parse().unwrap()
    }

    fn coinbase(connections: usize) -> (Coinbase, Receiver<WriteRecord>) {
        let (writer_tx, writer_rx) = channel(64);
        let products: Arc<[String]> = Arc::from(["BTC-USD".to_owned()]);
        let coinbase = Coinbase {
            recorder: Recorder::new(
                writer_tx,
                &products,
                NonZeroUsize::new(connections).unwrap(),
            ),
            books: Books::new(
                sources(&products),
                connections,
                QualityReporter::disabled(),
                at("2026-10-05T10:30:00Z"),
            ),
            products,
            channels: CHANNELS,
        };
        (coinbase, writer_rx)
    }

    fn written(writer_rx: &mut Receiver<WriteRecord>) -> Vec<Bytes> {
        std::iter::from_fn(|| writer_rx.try_recv().ok().map(|record| record.data)).collect()
    }

    impl Coinbase {
        async fn feed(&mut self, connection: usize, data: &str) {
            let frame = Frame {
                recv_time: at("2026-10-05T10:30:01Z"),
                ..Frame::on(connection, data.to_owned())
            };
            self.handle(frame).await.unwrap();
        }
    }

    const SNAPSHOT: &str = r#"{"type":"snapshot","product_id":"BTC-USD","asks":[["86125.10","0.1"]],"bids":[["86125.09","0.2"]]}"#;
    const UPDATE: &str = r#"{"type":"l2update","product_id":"BTC-USD","changes":[["buy","86100.00","1"]],"time":"2026-10-05T10:30:01Z"}"#;

    /// Copies as two connections sent them on 2026-10-05: the tickers differ
    /// in their rolling volumes, the rest byte for byte.
    #[tokio::test]
    async fn trades_tickers_and_heartbeats_are_kept_once() {
        let (mut coinbase, mut writer_rx) = coinbase(2);
        let ticker = |volume: &str| {
            format!(
                r#"{{"type":"ticker","sequence":137270364111,"product_id":"BTC-USD","price":"86125.09","volume_24h":"{volume}","volume_30d":"172298.7","trade_id":1102560089}}"#
            )
        };
        let trade = r#"{"type":"match","trade_id":1102560090,"maker_order_id":"a","taker_order_id":"b","side":"sell","size":"0.00002031","price":"86125.1","product_id":"BTC-USD","sequence":137270364525,"time":"2026-10-05T12:17:56.266162Z"}"#;
        let last_trade = trade.replace(r#""type":"match""#, r#""type":"last_match""#);
        let heartbeat = r#"{"type":"heartbeat","last_trade_id":1102560091,"product_id":"BTC-USD","sequence":137270364750,"time":"2026-10-05T12:17:57.000000Z"}"#;

        for (connection, frame) in [
            (0, ticker("4021.87")),
            (1, ticker("4021.88")),
            (0, trade.to_owned()),
            (1, last_trade),
            (1, heartbeat.to_owned()),
            (0, heartbeat.to_owned()),
        ] {
            coinbase.feed(connection, &frame).await;
        }

        assert_eq!(
            written(&mut writer_rx),
            [
                Bytes::from(ticker("4021.87")),
                Bytes::from(trade),
                Bytes::from(heartbeat)
            ]
        );
    }

    /// The book goes through leadership: one connection's book is recorded.
    #[tokio::test]
    async fn one_connections_book_is_recorded() {
        let (mut coinbase, mut writer_rx) = coinbase(2);
        let lifecycle = coinbase.books.lifecycle();
        let _sessions = (Announced::new(0, &lifecycle), Announced::new(1, &lifecycle));

        coinbase.feed(0, SNAPSHOT).await;
        coinbase.feed(1, SNAPSHOT).await;
        coinbase.feed(1, UPDATE).await;
        coinbase.feed(0, UPDATE).await;

        assert_eq!(
            written(&mut writer_rx),
            [Bytes::from(SNAPSHOT), Bytes::from(UPDATE)]
        );
    }
}
