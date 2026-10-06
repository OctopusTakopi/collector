//! Kraken Futures perpetuals over the public websocket (v1).
//!
//! Connections' copies of a message are byte for byte the same until Kraken
//! conflates one connection's book updates and renumbers its `seq` from
//! there (seen live 2026-10-05: one connection 28 ahead of the other). So the
//! book is recorded from one connection at a time (see [`crate::books`]),
//! and a break in that connection's `seq` hands the book over too; so is the
//! ticker, a sample. Trades carry their own ids and are kept once by payload.

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
    session::{Frame, Slot, Socket},
};

pub(crate) use connection::BOOK;

pub(crate) async fn collect(
    feeds: &'static [&'static str],
    products: &[String],
    cx: Context,
    tls: Arc<ClientConfig>,
) -> anyhow::Result<()> {
    let products: Arc<[String]> = products
        .iter()
        .map(|product| product.to_ascii_uppercase())
        .collect();
    let sources = products
        .iter()
        .map(|product| format!("krakenfutures/{}", product.to_ascii_lowercase()))
        .collect();
    let kraken = KrakenFutures {
        recorder: Recorder::new(cx.writer_tx.clone(), &products, cx.connections),
        books: Books::new(
            sources,
            cx.connections.get(),
            cx.quality.clone(),
            Timestamp::now(),
        ),
        chain: vec![None; products.len()],
        feeds,
        products,
    };
    collection::run(kraken, cx, tls, JoinSet::new()).await
}

/// The consumer of Kraken Futures' queue, which carries market data only.
struct KrakenFutures {
    feeds: &'static [&'static str],
    products: Arc<[String]>,
    recorder: Recorder,
    books: Books,
    /// Per product: the last book `seq` recorded, from the leader's snapshot
    /// on.
    chain: Vec<Option<i64>>,
}

impl Venue for KrakenFutures {
    fn label(&self) -> &'static str {
        "krakenfutures"
    }

    fn keep_connection(&self, slot: Slot) -> impl Future<Output = ()> + Send + 'static {
        connection::keep_connection(
            self.feeds,
            Arc::clone(&self.products),
            self.books.lifecycle(),
            slot,
        )
    }

    async fn handle(&mut self, frame: Frame) -> Result<(), ConnectorError> {
        self.books.catch_up(frame.recv_time);
        let product_id = routing::kraken_futures_product(&frame.data)
            .ok_or(ConnectorError::MissingField("product_id"))?;
        let index = self
            .products
            .iter()
            .position(|product| product == product_id);
        let keep = match (routing::kraken_futures_book(&frame.data), index) {
            (Some((_, seq, snapshot)), Some(index)) => {
                self.book(frame.socket, index, seq, snapshot, frame.recv_time)
            }
            (None, Some(index)) if routing::kraken_futures_feed(&frame.data) == Some("ticker") => {
                self.books.leads(frame.socket, index)
                    && !self.recorder.dedup.is_duplicate(&frame.data)
            }
            _ => !self.recorder.dedup.is_duplicate(&frame.data),
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

impl KrakenFutures {
    /// Whether a book message is recorded: the leader's, with each delta's
    /// `seq` one above the last.
    fn book(
        &mut self,
        socket: Socket,
        product: usize,
        seq: i64,
        snapshot: bool,
        now: Timestamp,
    ) -> bool {
        if !self.books.admit(socket, product, snapshot, now) {
            return false;
        }
        match self.chain[product] {
            _ if snapshot => {}
            Some(last) if seq == last + 1 => {}
            last => {
                tracing::warn!(
                    product = %self.products[product],
                    connection = socket.connection,
                    ?last,
                    seq,
                    "book chain broken on the leader"
                );
                self.books.broken(socket, product, now);
                self.chain[product] = None;
                return false;
            }
        }
        self.chain[product] = Some(seq);
        true
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use bytes::Bytes;
    use tokio::sync::mpsc::{Receiver, channel};

    use super::*;
    use crate::{books::Announced, file::WriteRecord, quality::QualityReporter};

    fn kraken(connections: usize) -> (KrakenFutures, Receiver<WriteRecord>) {
        let (writer_tx, writer_rx) = channel(16);
        let products: Arc<[String]> = Arc::from(["PF_XBTUSD".to_owned()]);
        let kraken = KrakenFutures {
            recorder: Recorder::new(
                writer_tx,
                &products,
                NonZeroUsize::new(connections).unwrap(),
            ),
            books: Books::new(
                vec!["krakenfutures/pf_xbtusd".to_owned()],
                connections,
                QualityReporter::disabled(),
                Timestamp::now(),
            ),
            chain: vec![None],
            feeds: &[BOOK],
            products,
        };
        (kraken, writer_rx)
    }

    fn snapshot(seq: i64) -> Bytes {
        Bytes::from(format!(
            r#"{{"feed":"book_snapshot","product_id":"PF_XBTUSD","timestamp":1,"seq":{seq},"tickSize":null,"bids":[],"asks":[]}}"#
        ))
    }

    fn delta(seq: i64) -> Bytes {
        Bytes::from(format!(
            r#"{{"feed":"book","product_id":"PF_XBTUSD","side":"sell","seq":{seq},"price":86452.0,"qty":0.0028,"timestamp":1}}"#
        ))
    }

    fn written(writer_rx: &mut Receiver<WriteRecord>) -> Vec<Bytes> {
        std::iter::from_fn(|| writer_rx.try_recv().ok().map(|record| record.data)).collect()
    }

    #[tokio::test]
    async fn trades_are_recorded_by_product_once_across_connections() {
        let (mut kraken, mut writer_rx) = kraken(2);
        let trade = Bytes::from_static(
            br#"{"product_id":"PF_XBTUSD","feed":"trade","uid":"f3a03bd2","side":"buy","type":"fill","time":1791202676588,"qty":0.0202,"price":86120.0,"seq":995981}"#,
        );

        kraken.handle(Frame::on(0, trade.clone())).await.unwrap();
        kraken.handle(Frame::on(1, trade.clone())).await.unwrap();

        let record = writer_rx.try_recv().unwrap();
        assert_eq!(record.symbol.as_ref(), "pf_xbtusd");
        assert_eq!(record.data, trade);
        assert!(writer_rx.try_recv().is_err(), "the copy is dropped");
    }

    /// Two connections whose `seq` differ by 28, as on 2026-10-05: only the
    /// leader's book is recorded.
    #[tokio::test]
    async fn one_connections_book_is_recorded() {
        let (mut kraken, mut writer_rx) = kraken(2);
        let lifecycle = kraken.books.lifecycle();
        let _sessions = (
            Announced::new(
                Socket {
                    connection: 0,
                    session: 0,
                },
                &lifecycle,
            ),
            Announced::new(
                Socket {
                    connection: 1,
                    session: 0,
                },
                &lifecycle,
            ),
        );
        for (connection, frame) in [
            (0, snapshot(100)),
            (0, delta(101)),
            (1, snapshot(128)),
            (1, delta(129)),
            (0, delta(102)),
        ] {
            kraken.handle(Frame::on(connection, frame)).await.unwrap();
        }
        assert_eq!(
            written(&mut writer_rx),
            [snapshot(100), delta(101), delta(102)]
        );
    }

    /// A gap on the leader stops its book until a fresh snapshot, which here
    /// the leader itself is asked for.
    #[tokio::test]
    async fn a_break_on_the_leader_waits_for_a_fresh_snapshot() {
        let (mut kraken, mut writer_rx) = kraken(1);
        let lifecycle = kraken.books.lifecycle();
        let (_session, mut commands) = Announced::new(
            Socket {
                connection: 0,
                session: 0,
            },
            &lifecycle,
        );
        for seq in [100, 101] {
            let frame = if seq == 100 {
                snapshot(seq)
            } else {
                delta(seq)
            };
            kraken.handle(Frame::on(0, frame)).await.unwrap();
        }
        kraken.handle(Frame::on(0, delta(103))).await.unwrap();
        kraken.handle(Frame::on(0, delta(104))).await.unwrap();
        assert_eq!(commands.try_recv(), Ok(crate::books::Resnapshot(0)));
        kraken.handle(Frame::on(0, snapshot(110))).await.unwrap();
        kraken.handle(Frame::on(0, delta(111))).await.unwrap();

        assert_eq!(
            written(&mut writer_rx),
            [snapshot(100), delta(101), snapshot(110), delta(111)]
        );
    }

    #[tokio::test]
    async fn a_frame_without_a_product_is_not_recorded() {
        let (mut kraken, mut writer_rx) = kraken(1);
        assert!(matches!(
            kraken
                .handle(Frame::on(0, Bytes::from_static(br#"{"feed":"book"}"#)))
                .await,
            Err(ConnectorError::MissingField("product_id"))
        ));
        assert!(writer_rx.try_recv().is_err());
    }
}
