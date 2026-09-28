//! Hyperliquid perpetuals over the public websocket.

mod connection;

use std::{future::Future, sync::Arc, time::Duration};

use bytes::Bytes;
use jiff::Timestamp;
use rustls::ClientConfig;
use tokio::task::JoinSet;

use crate::{
    collection::{self, Context, Recorder, Venue},
    error::ConnectorError,
    routing::HyperliquidMessage,
    session::Slot,
};

/// Websocket subscriptions Hyperliquid allows per IP, across all connections.
const MAX_SUBSCRIPTIONS: usize = 1_000;

pub(crate) async fn collect(
    kinds: &'static [&'static str],
    coins: &[String],
    cx: Context,
    tls: Arc<ClientConfig>,
) -> anyhow::Result<()> {
    // Every redundant connection subscribes to the full set for itself.
    let per_connection = kinds.len() * coins.len();
    let total = per_connection * cx.connections.get();
    if total > MAX_SUBSCRIPTIONS {
        anyhow::bail!(
            "Hyperliquid allows at most {MAX_SUBSCRIPTIONS} websocket subscriptions per IP \
             across all connections; {} connection(s) x {per_connection} subscriptions = {total}",
            cx.connections
        );
    }
    let hyperliquid = Hyperliquid {
        subscriptions: connection::subscriptions(kinds, coins),
        pace: connection::subscribe_pace(cx.connections),
        recorder: Recorder::new(cx.writer_tx.clone(), coins, cx.connections),
    };
    collection::run(hyperliquid, cx, tls, JoinSet::new()).await
}

/// The consumer of Hyperliquid's queue, which carries market data only.
struct Hyperliquid {
    subscriptions: Arc<[connection::Subscription]>,
    pace: Duration,
    recorder: Recorder,
}

impl Venue for Hyperliquid {
    fn label(&self) -> &'static str {
        "hyperliquid"
    }

    fn keep_connection(&self, slot: Slot) -> impl Future<Output = ()> + Send + 'static {
        connection::keep_connection(Arc::clone(&self.subscriptions), self.pace, slot)
    }

    async fn handle(&mut self, recv_time: Timestamp, data: Bytes) -> Result<(), ConnectorError> {
        if self.recorder.dedup.is_duplicate(&data) {
            return Ok(());
        }
        let message: HyperliquidMessage<'_> = serde_json::from_slice(&data)?;
        let Some(coin) = message.coin else {
            return Ok(());
        };
        let symbol = self.recorder.symbols.resolve(coin);
        self.recorder.write(recv_time, symbol, data).await
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use tokio::sync::mpsc::{Receiver, channel};

    use super::*;
    use crate::file::WriteRecord;

    fn hyperliquid(connections: usize) -> (Hyperliquid, Receiver<WriteRecord>) {
        let (writer_tx, writer_rx) = channel(8);
        let coins = ["BTC".to_owned()];
        let connections = NonZeroUsize::new(connections).unwrap();
        let hyperliquid = Hyperliquid {
            subscriptions: connection::subscriptions(&["trades"], &coins),
            pace: connection::subscribe_pace(connections),
            recorder: Recorder::new(writer_tx, &coins, connections),
        };
        (hyperliquid, writer_rx)
    }

    #[tokio::test]
    async fn routes_trade_by_coin() {
        let (mut hyperliquid, mut writer_rx) = hyperliquid(1);
        let data = Bytes::from_static(
            br#"{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"1","sz":"2"}]}"#,
        );

        hyperliquid
            .handle(Timestamp::now(), data.clone())
            .await
            .unwrap();

        let WriteRecord {
            symbol,
            data: written,
            ..
        } = writer_rx.try_recv().unwrap();
        assert_eq!(symbol.as_ref(), "btc");
        assert_eq!(written, data);
    }

    #[tokio::test]
    async fn redundant_copies_are_recorded_once() {
        let (mut hyperliquid, mut writer_rx) = hyperliquid(2);
        let trade = Bytes::from_static(
            br#"{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"1","sz":"2"}]}"#,
        );

        hyperliquid
            .handle(Timestamp::now(), trade.clone())
            .await
            .unwrap();
        hyperliquid
            .handle(Timestamp::now(), trade.clone())
            .await
            .unwrap();

        assert_eq!(writer_rx.try_recv().unwrap().data, trade);
        assert!(writer_rx.try_recv().is_err(), "the copy is dropped");
    }

    #[tokio::test]
    async fn too_many_subscriptions_for_one_ip_are_refused_up_front() {
        let (writer_tx, _writer_rx) = channel(1);
        let (_shutdown_tx, shutdown) = tokio::sync::watch::channel(false);
        let coins: Vec<String> = (0..200).map(|index| format!("C{index}")).collect();
        let cx = Context {
            writer_tx,
            shutdown,
            connections: NonZeroUsize::new(2).unwrap(),
            quality: crate::quality::QualityReporter::disabled(),
            readiness: crate::readiness::Readiness::new(1).0,
        };

        let error = collect(
            &["trades", "l2Book", "bbo"],
            &coins,
            cx,
            crate::ws::tls_config(),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("= 1200"), "{error}");
    }
}
