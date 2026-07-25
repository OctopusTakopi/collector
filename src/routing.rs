use serde::{
    Deserialize, Deserializer,
    de::{Error as _, IgnoredAny, MapAccess, SeqAccess, Visitor},
};
use serde_json::value::RawValue;
use std::fmt;

#[derive(Deserialize)]
pub struct BinanceMessage<'a> {
    #[serde(borrow)]
    pub stream: Option<&'a str>,
    #[serde(borrow)]
    pub data: Option<BinanceEvent<'a>>,
}

#[derive(Deserialize)]
pub struct BinanceEvent<'a> {
    #[serde(rename = "e", borrow)]
    pub event: Option<&'a str>,
    pub u: Option<i64>,
    #[serde(rename = "U")]
    pub first_update_id: Option<i64>,
    pub pu: Option<i64>,
}

impl BinanceMessage<'_> {
    pub fn symbol(&self) -> Option<&str> {
        self.stream?.split_once('@').map(|(symbol, _)| symbol)
    }
}

#[derive(Deserialize)]
pub struct BybitMessage<'a> {
    #[serde(borrow)]
    pub topic: Option<&'a str>,
    #[serde(borrow)]
    pub op: Option<&'a str>,
    #[serde(borrow)]
    pub ret_msg: Option<&'a str>,
    #[serde(borrow)]
    pub req_id: Option<&'a str>,
    pub success: Option<bool>,
}

pub struct HyperliquidMessage<'a> {
    pub channel: &'a str,
    symbol: Option<&'a str>,
}

#[derive(Deserialize)]
struct HyperliquidCoin<'a> {
    #[serde(borrow)]
    coin: Option<&'a str>,
}

struct FirstHyperliquidCoin<'a>(Option<&'a str>);

impl<'de> Deserialize<'de> for FirstHyperliquidCoin<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FirstCoinVisitor;

        impl<'de> Visitor<'de> for FirstCoinVisitor {
            type Value = FirstHyperliquidCoin<'de>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an array of Hyperliquid trade objects")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let coin = sequence
                    .next_element::<HyperliquidCoin<'de>>()?
                    .and_then(|trade| trade.coin);
                while sequence.next_element::<IgnoredAny>()?.is_some() {}
                Ok(FirstHyperliquidCoin(coin))
            }
        }

        deserializer.deserialize_seq(FirstCoinVisitor)
    }
}

/// Resolve the symbol from an already-captured `data` payload.
///
/// This is the only place unknown channels are interpreted; the fast path in
/// `visit_map` below defers anything it does not recognise to here, so a new
/// subscription type only has to be taught to one dispatch.
fn hyperliquid_symbol_from_raw<'a>(
    channel: &str,
    data: &'a RawValue,
) -> Result<Option<&'a str>, serde_json::Error> {
    match channel {
        "trades" => {
            let FirstHyperliquidCoin(coin) = serde_json::from_str(data.get())?;
            Ok(coin)
        }
        "l2Book" | "bbo" => {
            let data: HyperliquidCoin<'a> = serde_json::from_str(data.get())?;
            Ok(data.coin)
        }
        // Route any other channel on a `coin` field if it has one. Payloads that
        // are not objects at all (the `error` channel carries a bare string) are
        // simply unroutable, not malformed.
        _ => Ok(serde_json::from_str::<HyperliquidCoin<'a>>(data.get())
            .ok()
            .and_then(|data| data.coin)),
    }
}

impl<'de> Deserialize<'de> for HyperliquidMessage<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct HyperliquidVisitor;

        impl<'de> Visitor<'de> for HyperliquidVisitor {
            type Value = HyperliquidMessage<'de>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a Hyperliquid websocket message")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut channel = None;
                let mut symbol = None;
                let mut deferred_data: Option<Option<&'de RawValue>> = None;

                while let Some(key) = map.next_key::<&str>()? {
                    match key {
                        "channel" => channel = Some(map.next_value()?),
                        // Fast path: with `channel` already seen (the normal wire
                        // order) the payload is read straight into the shape that
                        // channel uses, without materialising it. Anything else
                        // is captured raw and handed to
                        // `hyperliquid_symbol_from_raw` after the loop.
                        "data" => match channel {
                            Some("trades") => {
                                symbol = map.next_value::<FirstHyperliquidCoin<'de>>()?.0
                            }
                            Some("l2Book") | Some("bbo") => {
                                symbol = map.next_value::<HyperliquidCoin<'de>>()?.coin
                            }
                            _ => deferred_data = Some(map.next_value()?),
                        },
                        _ => {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                }

                let channel = channel.ok_or_else(|| A::Error::missing_field("channel"))?;
                if symbol.is_none()
                    && let Some(Some(data)) = deferred_data
                {
                    symbol =
                        hyperliquid_symbol_from_raw(channel, data).map_err(A::Error::custom)?;
                }

                Ok(HyperliquidMessage { channel, symbol })
            }
        }

        deserializer.deserialize_map(HyperliquidVisitor)
    }
}

impl<'a> HyperliquidMessage<'a> {
    pub fn symbol(&self) -> Option<&'a str> {
        self.symbol
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binance_partial_parse_ignores_payload_arrays() {
        let raw = br#"{"stream":"btcusdt@depth@0ms","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":2,"u":3,"b":[["1","2"],["3","4"]],"a":[["5","6"]]}}"#;
        let message: BinanceMessage<'_> = serde_json::from_slice(raw).unwrap();
        let event = message.data.as_ref().unwrap();

        assert_eq!(message.symbol(), Some("btcusdt"));
        assert_eq!(event.event, Some("depthUpdate"));
        assert_eq!(event.first_update_id, Some(2));
        assert_eq!(event.u, Some(3));
    }

    #[test]
    fn binance_book_ticker_does_not_require_event_type() {
        let raw = br#"{"stream":"btcusdt@bookTicker","data":{"u":400900217,"s":"BTCUSDT","b":"25.35190000","B":"31.21000000","a":"25.36520000","A":"40.66000000"}}"#;
        let message: BinanceMessage<'_> = serde_json::from_slice(raw).unwrap();

        assert_eq!(message.symbol(), Some("btcusdt"));
        assert_eq!(message.data.unwrap().event, None);
    }

    #[test]
    fn binance_control_message_without_stream_is_ignored() {
        let raw = br#"{"result":null,"id":1}"#;
        let message: BinanceMessage<'_> = serde_json::from_slice(raw).unwrap();

        assert_eq!(message.symbol(), None);
    }

    #[test]
    fn hyperliquid_routes_without_materializing_market_data() {
        let trade = br#"{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"1","sz":"2"},{"coin":"BTC","side":"A","px":"3","sz":"4"}]}"#;
        let trade: HyperliquidMessage<'_> = serde_json::from_slice(trade).unwrap();
        assert_eq!(trade.symbol(), Some("BTC"));

        let book = br#"{"channel":"l2Book","data":{"coin":"ETH","time":1,"levels":[[{"px":"1","sz":"2","n":3}],[]]}}"#;
        let book: HyperliquidMessage<'_> = serde_json::from_slice(book).unwrap();
        assert_eq!(book.symbol(), Some("ETH"));
    }

    #[test]
    fn hyperliquid_routes_when_data_precedes_channel() {
        let raw = br#"{"data":{"coin":"ETH","time":1,"levels":[]},"channel":"l2Book"}"#;
        let message: HyperliquidMessage<'_> = serde_json::from_slice(raw).unwrap();

        assert_eq!(message.symbol(), Some("ETH"));
    }

    #[test]
    fn hyperliquid_unknown_non_object_payload_is_ignored() {
        let raw = br#"{"channel":"error","data":"subscription failed"}"#;
        let message: HyperliquidMessage<'_> = serde_json::from_slice(raw).unwrap();

        assert_eq!(message.symbol(), None);
    }

    #[test]
    fn hyperliquid_unknown_channel_still_routes_on_coin() {
        // A channel this router does not know about is routed rather than
        // silently dropped, as long as its payload carries a `coin`.
        for raw in [
            br#"{"channel":"activeAssetCtx","data":{"coin":"SOL","ctx":{"funding":"1"}}}"#
                .as_slice(),
            br#"{"data":{"coin":"SOL","ctx":{"funding":"1"}},"channel":"activeAssetCtx"}"#
                .as_slice(),
        ] {
            let message: HyperliquidMessage<'_> = serde_json::from_slice(raw).unwrap();
            assert_eq!(
                message.symbol(),
                Some("SOL"),
                "{}",
                String::from_utf8_lossy(raw)
            );
        }
    }
}
