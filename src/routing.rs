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

/// Fields the collector needs from a combined-stream frame. The rest of the
/// payload — including depth `b`/`a` arrays — is left unparsed and written as-is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BinanceRoute<'a> {
    pub symbol: &'a str,
    pub event: Option<&'a str>,
    pub u: Option<i64>,
    pub first_update_id: Option<i64>,
    pub pu: Option<i64>,
}

/// Combined-stream envelope scan. Returns `None` when the cheap layout does
/// not match (control frames, unusual whitespace, `data` before `stream` with
/// a large body) so the caller can fall back to serde.
///
/// Depth ids are read only from the header before `"b":[` / `"a":[`, so a
/// book-sized payload does not get its price strings walked.
pub fn parse_binance_route(payload: &[u8]) -> Option<BinanceRoute<'_>> {
    const STREAM_PREFIX: usize = 256;
    let stream = json_unescaped_string(
        payload.get(..STREAM_PREFIX.min(payload.len()))?,
        b"\"stream\":\"",
    )?;
    let symbol = stream.split_once('@')?.0;
    if symbol.is_empty() {
        return None;
    }

    if !stream.contains("@depth") {
        return Some(BinanceRoute {
            symbol,
            event: None,
            u: None,
            first_update_id: None,
            pu: None,
        });
    }

    let header = depth_header_prefix(payload);
    Some(BinanceRoute {
        symbol,
        event: json_unescaped_string(header, b"\"e\":\""),
        u: json_i64(header, b"\"u\":"),
        first_update_id: json_i64(header, b"\"U\":"),
        pu: json_i64(header, b"\"pu\":"),
    })
}

fn depth_header_prefix(payload: &[u8]) -> &[u8] {
    const MAX: usize = 1024;
    let head = payload.get(..MAX.min(payload.len())).unwrap_or(payload);
    for needle in [b"\"b\":[".as_slice(), b"\"a\":[".as_slice()] {
        if let Some(index) = find(head, needle) {
            return &head[..index];
        }
    }
    head
}

/// Where `needle` first occurs in `haystack`.
pub(crate) fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

pub(crate) fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    find(haystack, needle).is_some()
}

fn json_unescaped_string<'a>(hay: &'a [u8], key: &[u8]) -> Option<&'a str> {
    let start = find(hay, key)? + key.len();
    let end = start + hay[start..].iter().position(|&byte| byte == b'"')?;
    std::str::from_utf8(&hay[start..end]).ok()
}

fn json_i64(hay: &[u8], key: &[u8]) -> Option<i64> {
    let mut index = find(hay, key)? + key.len();
    while index < hay.len() && hay[index] == b' ' {
        index += 1;
    }
    let bytes = hay.get(index..)?;
    let mut value: i64 = 0;
    let mut seen = false;
    let mut negative = false;
    let rest = if bytes.first() == Some(&b'-') {
        negative = true;
        &bytes[1..]
    } else {
        bytes
    };
    for &byte in rest {
        if !byte.is_ascii_digit() {
            break;
        }
        seen = true;
        value = value.checked_mul(10)?.checked_add(i64::from(byte - b'0'))?;
    }
    if !seen {
        return None;
    }
    Some(if negative { -value } else { value })
}

/// Dedup key for a Binance combined-stream frame.
///
/// Binance spot stamps the event time `E` per connection at serialisation, so
/// the two connections' copies of one event land tens of microseconds apart
/// differing only in `E`, and a whole-payload key records both (measured over
/// two days of redundant collection: a few percent of spot trades). Cutting
/// `E` out of the key restores suppression. That cannot merge two distinct
/// events: every Binance market event carries an id that is unique per event
/// (`u`, `t`, `a`) outside `E`. Futures copies are byte-identical, so for
/// them this changes nothing.
///
/// Depth copies that were coalesced *differently* still differ beyond `E` and
/// stay recorded; payload-keyed dedup deliberately does not interpret ids.
pub fn binance_dedup_key(payload: &[u8]) -> u128 {
    let Some((pre, rest)) = cut_event_time(payload) else {
        return xxhash_rust::xxh3::xxh3_128(payload);
    };
    let mut hasher = xxhash_rust::xxh3::Xxh3Default::new();
    hasher.update(pre);
    hasher.update(rest);
    hasher.digest128()
}

/// Split `payload` around its first `"E":<integer>` field, the per-connection
/// stamp; anything else leaves the payload untouched. A quoted `"E":` cannot
/// occur inside a JSON string value (quotes there are escaped), so a byte
/// scan finds exactly the key.
fn cut_event_time(payload: &[u8]) -> Option<(&[u8], &[u8])> {
    const NEEDLE: &[u8] = b"\"E\":";
    let pos = find(payload, NEEDLE)?;
    let mut end = pos + NEEDLE.len();
    if end < payload.len() && payload[end] == b'-' {
        end += 1;
    }
    let digits = end;
    while end < payload.len() && payload[end].is_ascii_digit() {
        end += 1;
    }
    if end == digits {
        return None;
    }
    Some((&payload[..pos], &payload[end..]))
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

        assert_eq!(
            parse_binance_route(raw),
            Some(BinanceRoute {
                symbol: "btcusdt",
                event: Some("depthUpdate"),
                u: Some(3),
                first_update_id: Some(2),
                pu: None,
            })
        );
    }

    /// COIN-M depth since the CM-UM integration: `T` and `ps` sit among the
    /// ids, and `st` trails the book arrays.
    #[test]
    fn binance_route_reads_coin_m_depth_header() {
        let raw = br#"{"stream":"btcusd_perp@depth@0ms","data":{"e":"depthUpdate","E":1791026681837,"T":1791026681811,"s":"BTCUSD_PERP","ps":"BTCUSD","U":11726048432971,"u":11726048432978,"pu":11726048432171,"b":[["76119.9","1521"]],"a":[],"st":2}}"#;

        assert_eq!(
            parse_binance_route(raw),
            Some(BinanceRoute {
                symbol: "btcusd_perp",
                event: Some("depthUpdate"),
                u: Some(11726048432978),
                first_update_id: Some(11726048432971),
                pu: Some(11726048432171),
            })
        );
    }

    #[test]
    fn binance_route_scan_stops_before_book_arrays() {
        let mut raw = br#"{"stream":"btcusdt@depth","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":2,"u":3,"pu":1,"b":["#
            .to_vec();
        for _ in 0..8_000 {
            raw.extend_from_slice(br#"["12345.67","890.12"],"#);
        }
        raw.extend_from_slice(br#"[]]}}"#);

        assert_eq!(
            parse_binance_route(&raw),
            Some(BinanceRoute {
                symbol: "btcusdt",
                event: Some("depthUpdate"),
                u: Some(3),
                first_update_id: Some(2),
                pu: Some(1),
            })
        );
    }

    #[test]
    fn binance_route_scan_skips_non_depth_bodies() {
        let raw = br#"{"stream":"btcusdt@bookTicker","data":{"u":400900217,"s":"BTCUSDT","b":"25.35190000","B":"31.21000000","a":"25.36520000","A":"40.66000000"}}"#;
        assert_eq!(
            parse_binance_route(raw),
            Some(BinanceRoute {
                symbol: "btcusdt",
                event: None,
                u: None,
                first_update_id: None,
                pu: None,
            })
        );
        assert!(parse_binance_route(br#"{"result":null,"id":1}"#).is_none());
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

    #[test]
    fn a_quoted_topic_key_separates_bybit_market_data_from_control() {
        let market = br#"{"topic":"orderbook.1.BTCUSDT","type":"delta","data":{}}"#;
        let ack = br#"{"success":true,"ret_msg":"subscribe","op":"subscribe","req_id":"BTCUSDT"}"#;
        let rejection = br#"{"success":false,"ret_msg":"error:handler not found,topic:orderbook.1.FOO","op":"subscribe","req_id":"FOO"}"#;

        assert!(contains(market, br#""topic""#));
        assert!(!contains(ack, br#""topic""#));
        // The rejection mentions `topic:` in prose but has no `"topic"` field.
        assert!(!contains(rejection, br#""topic""#));
    }

    /// The two connections' copies of a spot event differ only in the
    /// per-connection `E` stamp, so the key must not see `E` at all.
    #[test]
    fn dedup_key_ignores_the_per_connection_event_time() {
        let a = br#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1785671271715,"s":"BTCUSDT","t":6551542138,"p":"63160.00000000","q":"0.12952000","T":1785671271715,"m":false,"M":true}}"#;
        let b = br#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1785671271716,"s":"BTCUSDT","t":6551542138,"p":"63160.00000000","q":"0.12952000","T":1785671271715,"m":false,"M":true}}"#;
        assert_eq!(binance_dedup_key(a), binance_dedup_key(b));
        assert_ne!(
            binance_dedup_key(a),
            xxhash_rust::xxh3::xxh3_128(a),
            "the stamp must actually be cut out"
        );
    }

    /// The cut must not merge distinct events: ids outside `E` still separate
    /// them.
    #[test]
    fn dedup_key_keeps_distinct_events_apart() {
        let a = br#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1,"t":10}}"#;
        let b = br#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1,"t":11}}"#;
        assert_ne!(binance_dedup_key(a), binance_dedup_key(b));
    }

    /// Frames without `E` (spot bookTicker) hash exactly as the plain payload.
    #[test]
    fn dedup_key_without_event_time_is_the_plain_hash() {
        let frame = br#"{"stream":"btcusdt@bookTicker","data":{"u":1,"s":"BTCUSDT","b":"1","B":"1","a":"2","A":"1"}}"#;
        assert_eq!(binance_dedup_key(frame), xxhash_rust::xxh3::xxh3_128(frame));
    }
}
