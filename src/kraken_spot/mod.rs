//! Kraken spot over the public websocket v2.
//!
//! Connections' copies of a message are byte for byte the same until Kraken
//! conflates one connection's book updates (seen live 2026-10-05), after
//! which their books differ. So the book is recorded from one connection at
//! a time (see [`crate::books`]), and so is the ticker, a sample of the best
//! bid and offer; trades carry their ids and are kept once by payload.
//! Kraken names pairs `BTC/USD`; a file is named after the pair with `-` for
//! the slash, `btc-usd_<date>.zst`.

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
    symbol::Symbol,
};

pub(crate) async fn collect(
    channels: &'static [&'static str],
    pairs: &[String],
    cx: Context,
    tls: Arc<ClientConfig>,
) -> anyhow::Result<()> {
    let pairs: Arc<[String]> = pairs.iter().map(|pair| pair.to_ascii_uppercase()).collect();
    let file_names: Vec<String> = pairs.iter().map(|pair| file_name(pair)).collect();
    let mut recorder = Recorder::new(cx.writer_tx.clone(), &file_names, cx.connections);
    let symbols = pairs
        .iter()
        .zip(&file_names)
        .map(|(pair, name)| (pair.clone(), recorder.symbols.resolve(name)))
        .collect();
    let sources = file_names
        .iter()
        .map(|name| format!("krakenspot/{}", name.to_ascii_lowercase()))
        .collect();
    let kraken = KrakenSpot {
        channels,
        pairs,
        books: Books::new(
            sources,
            cx.connections.get(),
            cx.quality.clone(),
            Timestamp::now(),
        ),
        symbols,
        recorder,
    };
    collection::run(kraken, cx, tls, JoinSet::new()).await
}

/// `BTC/USD` as a file name: a slash would make it a directory.
fn file_name(pair: &str) -> String {
    pair.replace('/', "-")
}

/// The consumer of Kraken spot's queue, which carries market data only.
struct KrakenSpot {
    channels: &'static [&'static str],
    pairs: Arc<[String]>,
    books: Books,
    /// Each pair as Kraken names it, in the order of `pairs`,, with the symbol its file is named after.
    symbols: Vec<(String, Symbol)>,
    recorder: Recorder,
}

impl Venue for KrakenSpot {
    fn label(&self) -> &'static str {
        "krakenspot"
    }

    fn keep_connection(&self, slot: Slot) -> impl Future<Output = ()> + Send + 'static {
        connection::keep_connection(
            self.channels,
            Arc::clone(&self.pairs),
            self.books.lifecycle(),
            slot,
        )
    }

    async fn handle(&mut self, frame: Frame) -> Result<(), ConnectorError> {
        self.books.catch_up(frame.recv_time);
        let pair =
            routing::kraken_symbol(&frame.data).ok_or(ConnectorError::MissingField("symbol"))?;
        let index = self.symbols.iter().position(|(known, _)| known == pair);
        let keep = match (routing::kraken_channel(&frame.data), index) {
            (Some("book"), Some(index)) => self.books.admit(
                frame.connection,
                index,
                routing::kraken_snapshot(&frame.data),
                frame.recv_time,
            ),
            (Some("ticker"), Some(index)) => {
                self.books.leads(frame.connection, index)
                    && !self.recorder.dedup.is_duplicate(&frame.data)
            }
            _ => !self.recorder.dedup.is_duplicate(&frame.data),
        };
        if !keep {
            return Ok(());
        }
        let symbol = match index {
            Some(index) => Symbol::clone(&self.symbols[index].1),
            None => self.recorder.symbols.resolve(&file_name(pair)),
        };
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
    use crate::file::WriteRecord;

    fn kraken(connections: usize) -> (KrakenSpot, Receiver<WriteRecord>) {
        let (writer_tx, writer_rx) = channel(8);
        let mut recorder = Recorder::new(
            writer_tx,
            &["BTC-USD".to_owned()],
            NonZeroUsize::new(connections).unwrap(),
        );
        let symbols = vec![("BTC/USD".to_owned(), recorder.symbols.resolve("BTC-USD"))];
        let kraken = KrakenSpot {
            channels: &["book"],
            pairs: Arc::from(["BTC/USD".to_owned()]),
            books: Books::new(
                vec!["krakenspot/btc-usd".to_owned()],
                connections,
                crate::quality::QualityReporter::disabled(),
                Timestamp::now(),
            ),
            symbols,
            recorder,
        };
        (kraken, writer_rx)
    }

    /// One connection's book is recorded, under the pair's file name, and
    /// its ticker with it.
    #[tokio::test]
    async fn one_connections_book_and_ticker_are_recorded() {
        let (mut kraken, mut writer_rx) = kraken(2);
        let snapshot = Bytes::from_static(
            br#"{"channel":"book","type":"snapshot","data":[{"symbol":"BTC/USD","bids":[{"price":86100.0,"qty":0.5}],"asks":[],"checksum":1,"timestamp":"2026-10-05T12:17:56.551146Z"}]}"#,
        );
        let update = Bytes::from_static(
            br#"{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[{"price":86100.0,"qty":0.6}],"asks":[],"checksum":2,"timestamp":"2026-10-05T12:17:56.651146Z"}]}"#,
        );
        let ticker = |bid: &str| {
            Bytes::from(format!(
                r#"{{"channel":"ticker","type":"update","data":[{{"symbol":"BTC/USD","bid":{bid},"bid_qty":0.5,"ask":86101.0,"ask_qty":1.0}}]}}"#
            ))
        };

        for (connection, frame) in [
            (0, snapshot.clone()),
            (1, snapshot.clone()),
            (1, update.clone()),
            (0, update.clone()),
            (1, ticker("86099.0")),
            (0, ticker("86100.0")),
        ] {
            kraken.handle(Frame::on(connection, frame)).await.unwrap();
        }

        let records: Vec<_> = std::iter::from_fn(|| writer_rx.try_recv().ok()).collect();
        assert!(
            records
                .iter()
                .all(|record| record.symbol.as_ref() == "btc-usd")
        );
        let data: Vec<Bytes> = records.into_iter().map(|record| record.data).collect();
        assert_eq!(data, [snapshot, update, ticker("86100.0")]);
    }

    #[tokio::test]
    async fn trades_are_kept_once_by_payload() {
        let (mut kraken, mut writer_rx) = kraken(2);
        let trade = Bytes::from_static(
            br#"{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"buy","price":86100.0,"qty":0.1,"ord_type":"market","trade_id":1,"timestamp":"2026-10-05T12:17:56Z"}]}"#,
        );
        kraken.handle(Frame::on(0, trade.clone())).await.unwrap();
        kraken.handle(Frame::on(1, trade.clone())).await.unwrap();
        assert_eq!(writer_rx.try_recv().unwrap().data, trade);
        assert!(writer_rx.try_recv().is_err(), "the copy is dropped");
    }

    #[tokio::test]
    async fn an_unknown_pair_still_gets_a_file_of_its_own() {
        let (mut kraken, mut writer_rx) = kraken(1);
        kraken
            .handle(Frame::on(
                0,
                Bytes::from_static(
                    br#"{"channel":"trade","type":"update","data":[{"symbol":"ETH/USD","side":"buy","price":2716.0,"qty":0.1,"ord_type":"market","trade_id":1,"timestamp":"2026-10-05T12:17:56Z"}]}"#,
                ),
            ))
            .await
            .unwrap();
        assert_eq!(writer_rx.try_recv().unwrap().symbol.as_ref(), "eth-usd");
    }
}
