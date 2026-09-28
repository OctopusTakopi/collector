//! Bybit linear perpetuals over the v5 public websocket.

mod connection;

use std::{future::Future, sync::Arc};

use bytes::Bytes;
use jiff::Timestamp;
use rustls::ClientConfig;
use tokio::task::JoinSet;

use crate::{
    collection::{self, Context, Recorder, Venue},
    error::ConnectorError,
    routing::BybitMessage,
    session::Slot,
};

pub(crate) async fn collect(
    topics: &[&str],
    symbols: &[String],
    cx: Context,
    tls: Arc<ClientConfig>,
) -> anyhow::Result<()> {
    let bybit = Bybit {
        groups: connection::groups(topics, symbols),
        recorder: Recorder::new(cx.writer_tx.clone(), symbols, cx.connections),
    };
    collection::run(bybit, cx, tls, JoinSet::new()).await
}

/// The consumer of Bybit's queue, which carries market data only.
struct Bybit {
    groups: Arc<[connection::Group]>,
    recorder: Recorder,
}

impl Venue for Bybit {
    fn label(&self) -> &'static str {
        "bybit"
    }

    fn keep_connection(&self, slot: Slot) -> impl Future<Output = ()> + Send + 'static {
        connection::keep_connection(Arc::clone(&self.groups), slot)
    }

    async fn handle(&mut self, recv_time: Timestamp, data: Bytes) -> Result<(), ConnectorError> {
        // Before parsing: with N connections, N-1 of every N frames are copies.
        if self.recorder.dedup.is_duplicate(&data) {
            return Ok(());
        }
        let message: BybitMessage<'_> = serde_json::from_slice(&data)?;
        let Some(topic) = message.topic else {
            return Ok(());
        };
        let symbol = topic.rsplit_once('.').map_or(topic, |(_, symbol)| symbol);
        let symbol = self.recorder.symbols.resolve(symbol);
        self.recorder.write(recv_time, symbol, data).await
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use tokio::sync::mpsc::{Receiver, channel};

    use super::*;
    use crate::file::WriteRecord;

    fn bybit(connections: usize) -> (Bybit, Receiver<WriteRecord>) {
        let (writer_tx, writer_rx) = channel(8);
        let symbols = ["BTCUSDT".to_owned()];
        let bybit = Bybit {
            groups: connection::groups(&["publicTrade.$symbol"], &symbols),
            recorder: Recorder::new(writer_tx, &symbols, NonZeroUsize::new(connections).unwrap()),
        };
        (bybit, writer_rx)
    }

    #[tokio::test]
    async fn routes_all_liquidation_by_topic_symbol() {
        let (mut bybit, mut writer_rx) = bybit(1);
        let data = Bytes::from_static(
            br#"{"topic":"allLiquidation.BTCUSDT","type":"snapshot","ts":1739502303204,"data":[{"T":1739502302929,"s":"BTCUSDT","S":"Sell","v":"2.5","p":"95000"}]}"#,
        );

        bybit.handle(Timestamp::now(), data.clone()).await.unwrap();

        let WriteRecord {
            symbol,
            data: written,
            ..
        } = writer_rx.try_recv().unwrap();
        assert_eq!(symbol.as_ref(), "btcusdt");
        assert_eq!(written, data);
    }

    #[tokio::test]
    async fn redundant_copies_are_recorded_once() {
        let (mut bybit, mut writer_rx) = bybit(2);
        let trade = Bytes::from_static(
            br#"{"topic":"publicTrade.BTCUSDT","ts":1,"data":[{"i":"7","p":"95000","v":"1"}]}"#,
        );

        bybit.handle(Timestamp::now(), trade.clone()).await.unwrap();
        bybit.handle(Timestamp::now(), trade.clone()).await.unwrap();

        assert_eq!(writer_rx.try_recv().unwrap().data, trade);
        assert!(writer_rx.try_recv().is_err(), "the copy is dropped");
    }
}
