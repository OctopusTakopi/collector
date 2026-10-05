//! Cheap routing of raw frames: which symbol a frame belongs to, and the few
//! fields the consumers act on, without materialising market data.

use std::fmt;

use serde::{
    Deserialize, Deserializer,
    de::{Error as _, IgnoredAny, MapAccess, SeqAccess, Visitor},
};
use serde_json::value::RawValue;

/// The Binance event fields the consumer acts on.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
pub struct BinanceEvent<'a> {
    /// `e`; absent on spot's bookTicker.
    #[serde(rename = "e", borrow)]
    pub kind: Option<&'a str>,
    pub u: Option<i64>,
    #[serde(rename = "U")]
    pub first_update_id: Option<i64>,
    pub pu: Option<i64>,
}

impl BinanceEvent<'_> {
    pub fn is_depth_update(&self) -> bool {
        self.kind == Some("depthUpdate")
    }
}

/// A combined-stream frame's symbol and event fields. The rest of the payload,
/// depth `b`/`a` arrays included, is left unparsed and written as-is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BinanceRoute<'a> {
    pub symbol: &'a str,
    pub event: BinanceEvent<'a>,
}

/// Routes a combined-stream frame, or `None` for a control frame without one.
/// Scans the envelope and falls back to serde when the layout is unusual.
pub fn binance_route(payload: &[u8]) -> Result<Option<BinanceRoute<'_>>, serde_json::Error> {
    if let Some(route) = scan_binance_route(payload) {
        return Ok(Some(route));
    }
    let message: BinanceMessage<'_> = serde_json::from_slice(payload)?;
    Ok(message.route())
}

#[derive(Deserialize)]
struct BinanceMessage<'a> {
    #[serde(borrow)]
    stream: Option<&'a str>,
    #[serde(borrow)]
    data: Option<BinanceEvent<'a>>,
}

impl<'a> BinanceMessage<'a> {
    fn route(&self) -> Option<BinanceRoute<'a>> {
        let (symbol, _) = self.stream?.split_once('@')?;
        Some(BinanceRoute {
            symbol,
            event: self.data?,
        })
    }
}

/// Scans the envelope for `stream` within its first bytes, and for a depth
/// stream reads the ids from the header before `"b":[` / `"a":[`, so a
/// book-sized payload never has its price strings walked. `None` when the
/// layout does not match (control frames, unusual whitespace, a large `data`
/// before `stream`).
fn scan_binance_route(payload: &[u8]) -> Option<BinanceRoute<'_>> {
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
            event: BinanceEvent::default(),
        });
    }

    let header = depth_header_prefix(payload);
    Some(BinanceRoute {
        symbol,
        event: BinanceEvent {
            kind: json_unescaped_string(header, b"\"e\":\""),
            u: json_i64(header, b"\"u\":"),
            first_update_id: json_i64(header, b"\"U\":"),
            pu: json_i64(header, b"\"pu\":"),
        },
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

/// A Coinbase Exchange feed message's type and product, and the product's
/// `sequence` where the message carries one (`match`, `ticker`, `heartbeat`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct CoinbaseRoute<'a> {
    #[serde(rename = "type", borrow)]
    pub kind: &'a str,
    #[serde(borrow)]
    pub product_id: Option<&'a str>,
    pub sequence: Option<i64>,
}

/// A Coinbase feed message's `type`, which the venue puts first.
pub(crate) fn coinbase_kind(payload: &[u8]) -> Option<&str> {
    json_unescaped_string(&payload[..payload.len().min(64)], b"\"type\":\"")
}

/// Routes a Coinbase feed message. Scans the head of the message, where the
/// venue puts these fields, so a multi-megabyte book snapshot is never walked;
/// falls back to serde when the layout is unusual.
pub fn coinbase_route(payload: &[u8]) -> Result<CoinbaseRoute<'_>, serde_json::Error> {
    const HEAD: usize = 512;
    let head = &payload[..HEAD.min(payload.len())];
    if let Some(kind) = json_unescaped_string(head, b"\"type\":\"")
        && let Some(product_id) = json_unescaped_string(head, b"\"product_id\":\"")
    {
        return Ok(CoinbaseRoute {
            kind,
            product_id: Some(product_id),
            sequence: json_i64(head, b"\"sequence\":"),
        });
    }
    serde_json::from_slice(payload)
}

/// Dedup key for a Coinbase message keyed by its product's `sequence`:
/// trades and tickers, whose copies from two connections can differ elsewhere
/// (a ticker's `volume_24h` and `volume_30d`, checked live 2026-10-05). `None`
/// for any other message, which is keyed by its payload.
pub fn coinbase_sequence_key(route: &CoinbaseRoute<'_>) -> Option<u128> {
    let class = match route.kind {
        // A `last_match` repeats a trade some connection may already have
        // sent as a `match`.
        "match" | "last_match" => b'm',
        "ticker" => b't',
        _ => return None,
    };
    let (product, sequence) = (route.product_id?, route.sequence?);
    let mut hasher = xxhash_rust::xxh3::Xxh3Default::new();
    hasher.update(&[class]);
    hasher.update(product.as_bytes());
    hasher.update(&sequence.to_le_bytes());
    Some(hasher.digest128())
}

/// The `channel` of a Kraken spot (websocket v2) message, which the venue puts
/// first; answers to requests (`{"method":...}`) have none.
pub(crate) fn kraken_channel(payload: &[u8]) -> Option<&str> {
    json_unescaped_string(&payload[..payload.len().min(32)], b"\"channel\":\"")
}

/// Whether a Kraken spot message is a snapshot: its `type`, right after the
/// channel.
pub(crate) fn kraken_snapshot(payload: &[u8]) -> bool {
    json_unescaped_string(&payload[..payload.len().min(48)], b"\"type\":\"") == Some("snapshot")
}

/// The pair a Kraken spot message is about: its first `symbol`, at the head
/// of `data` in every channel, so a book snapshot is never walked.
pub fn kraken_symbol(payload: &[u8]) -> Option<&str> {
    json_unescaped_string(&payload[..payload.len().min(128)], b"\"symbol\":\"")
}

/// The `product_id` of a Kraken Futures message, within its first fields in
/// every feed.
pub fn kraken_futures_product(payload: &[u8]) -> Option<&str> {
    json_unescaped_string(&payload[..payload.len().min(128)], b"\"product_id\":\"")
}

/// The `feed` of a Kraken Futures message. A ticker names it after its
/// funding fields, 278 to 302 bytes in (2026-10-05).
pub(crate) fn kraken_futures_feed(payload: &[u8]) -> Option<&str> {
    json_unescaped_string(&payload[..payload.len().min(512)], b"\"feed\":\"")
}

/// A Kraken Futures book message's product and `seq`, and whether it is the
/// snapshot that restarts the chain; `None` for any other feed.
pub(crate) fn kraken_futures_book(payload: &[u8]) -> Option<(&str, i64, bool)> {
    let snapshot = if payload.starts_with(br#"{"feed":"book_snapshot""#) {
        true
    } else if payload.starts_with(br#"{"feed":"book""#) {
        false
    } else {
        return None;
    };
    let head = &payload[..payload.len().min(160)];
    Some((
        kraken_futures_product(head)?,
        json_i64(head, b"\"seq\":")?,
        snapshot,
    ))
}

/// The `channel` of a Hyperliquid frame, found by scanning rather than
/// parsing; the venue puts it first.
pub(crate) fn hyperliquid_channel(payload: &[u8]) -> Option<&str> {
    json_unescaped_string(payload, b"\"channel\":\"")
}

#[derive(Deserialize)]
pub(crate) struct BybitMessage<'a> {
    #[serde(borrow)]
    pub(crate) topic: Option<&'a str>,
    #[serde(borrow)]
    pub(crate) op: Option<&'a str>,
    #[serde(borrow)]
    pub(crate) ret_msg: Option<&'a str>,
    #[serde(borrow)]
    pub(crate) req_id: Option<&'a str>,
    pub(crate) success: Option<bool>,
    #[serde(borrow)]
    pub(crate) data: Option<&'a RawValue>,
}

/// The update id in a Bybit book frame's `data`.
#[derive(Deserialize)]
pub(crate) struct BybitBookUpdate {
    pub(crate) u: u64,
}

/// The coin a Hyperliquid frame is about.
pub(crate) struct HyperliquidMessage<'a> {
    pub(crate) coin: Option<&'a str>,
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

/// The coin of a `data` payload captured raw, because it preceded `channel` or
/// its channel is not one the fast path knows.
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
        // Any other channel routes on a `coin` field if it has one; a payload
        // that is not an object (`error` carries a string) is unroutable.
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
                        // With `channel` already seen, the normal wire order,
                        // read the payload straight into its channel's shape.
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

                Ok(HyperliquidMessage { coin: symbol })
            }
        }

        deserializer.deserialize_map(HyperliquidVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn depth(u: i64, first_update_id: i64, pu: Option<i64>) -> BinanceEvent<'static> {
        BinanceEvent {
            kind: Some("depthUpdate"),
            u: Some(u),
            first_update_id: Some(first_update_id),
            pu,
        }
    }

    /// The scan and the serde fallback must agree on every field.
    #[test]
    fn binance_scan_and_serde_agree_and_skip_payload_arrays() {
        let raw = br#"{"stream":"btcusdt@depth@0ms","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":2,"u":3,"b":[["1","2"],["3","4"]],"a":[["5","6"]]}}"#;
        let expected = BinanceRoute {
            symbol: "btcusdt",
            event: depth(3, 2, None),
        };

        assert_eq!(scan_binance_route(raw), Some(expected));
        let message: BinanceMessage<'_> = serde_json::from_slice(raw).unwrap();
        assert_eq!(message.route(), Some(expected));
    }

    /// `data` before a large `stream`-less prefix defeats the scan; serde
    /// still routes it.
    #[test]
    fn binance_falls_back_to_serde_when_the_scan_misses() {
        let raw = format!(
            r#"{{"data":{{"e":"depthUpdate","E":1,"pad":"{}","U":2,"u":3,"pu":1}},"stream":"btcusdt@depth"}}"#,
            "x".repeat(300)
        );
        assert_eq!(scan_binance_route(raw.as_bytes()), None);
        assert_eq!(
            binance_route(raw.as_bytes()).unwrap(),
            Some(BinanceRoute {
                symbol: "btcusdt",
                event: depth(3, 2, Some(1)),
            })
        );
    }

    /// COIN-M depth since the CM-UM integration: `T` and `ps` sit among the
    /// ids, and `st` trails the book arrays.
    #[test]
    fn binance_route_reads_coin_m_depth_header() {
        let raw = br#"{"stream":"btcusd_perp@depth@0ms","data":{"e":"depthUpdate","E":1791026681837,"T":1791026681811,"s":"BTCUSD_PERP","ps":"BTCUSD","U":11726048432971,"u":11726048432978,"pu":11726048432171,"b":[["76119.9","1521"]],"a":[],"st":2}}"#;

        assert_eq!(
            scan_binance_route(raw),
            Some(BinanceRoute {
                symbol: "btcusd_perp",
                event: depth(11726048432978, 11726048432971, Some(11726048432171)),
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
            scan_binance_route(&raw),
            Some(BinanceRoute {
                symbol: "btcusdt",
                event: depth(3, 2, Some(1)),
            })
        );
    }

    #[test]
    fn binance_route_scan_skips_non_depth_bodies() {
        let raw = br#"{"stream":"btcusdt@bookTicker","data":{"u":400900217,"s":"BTCUSDT","b":"25.35190000","B":"31.21000000","a":"25.36520000","A":"40.66000000"}}"#;
        assert_eq!(
            scan_binance_route(raw),
            Some(BinanceRoute {
                symbol: "btcusdt",
                event: BinanceEvent::default(),
            })
        );
        assert!(scan_binance_route(br#"{"result":null,"id":1}"#).is_none());
    }

    #[test]
    fn binance_book_ticker_does_not_require_event_type() {
        let raw = br#"{"stream":"btcusdt@bookTicker","data":{"u":400900217,"s":"BTCUSDT","b":"25.35190000","B":"31.21000000","a":"25.36520000","A":"40.66000000"}}"#;
        let message: BinanceMessage<'_> = serde_json::from_slice(raw).unwrap();
        let route = message.route().unwrap();

        assert_eq!(route.symbol, "btcusdt");
        assert_eq!(route.event.kind, None);
        assert!(!route.event.is_depth_update());
    }

    #[test]
    fn binance_control_message_without_stream_is_ignored() {
        assert_eq!(binance_route(br#"{"result":null,"id":1}"#).unwrap(), None);
    }

    /// Message layouts as served on 2026-10-05.
    #[test]
    fn coinbase_messages_route_on_type_product_and_sequence() {
        fn route(raw: &[u8]) -> CoinbaseRoute<'_> {
            coinbase_route(raw).unwrap()
        }
        let ticker = br#"{"type":"ticker","sequence":137270364111,"product_id":"BTC-USD","price":"86125.09","volume_24h":"4021.87"}"#;
        assert_eq!(
            route(ticker),
            CoinbaseRoute {
                kind: "ticker",
                product_id: Some("BTC-USD"),
                sequence: Some(137270364111),
            }
        );
        let matched = br#"{"type":"match","trade_id":1102560090,"maker_order_id":"ddc1e417-15aa-489e-92ad-eb286169748a","taker_order_id":"995766dc-a994-4337-ae9c-795900b48d7f","side":"sell","size":"0.00002031","price":"86125.1","product_id":"BTC-USD","sequence":137270364525,"time":"2026-10-05T12:17:56.266162Z"}"#;
        assert_eq!(route(matched).sequence, Some(137270364525));
        let update = br#"{"type":"l2update","product_id":"ETH-USD","changes":[["buy","2716.3","0.5"]],"time":"2026-10-05T12:17:56.020423Z"}"#;
        assert_eq!(
            route(update),
            CoinbaseRoute {
                kind: "l2update",
                product_id: Some("ETH-USD"),
                sequence: None,
            }
        );
    }

    /// A snapshot's book arrays are not walked, however long.
    #[test]
    fn a_coinbase_snapshot_routes_from_its_head() {
        let mut raw = br#"{"type":"snapshot","product_id":"BTC-USD","asks":["#.to_vec();
        for _ in 0..40_000 {
            raw.extend_from_slice(br#"["86125.10","0.13458813"],"#);
        }
        raw.extend_from_slice(br#"["1","1"]],"bids":[]}"#);
        let route = coinbase_route(&raw).unwrap();
        assert_eq!(
            (route.kind, route.product_id),
            ("snapshot", Some("BTC-USD"))
        );
    }

    #[test]
    fn a_coinbase_message_with_an_unusual_layout_falls_back_to_serde() {
        let raw = format!(
            r#"{{"pad":"{}","product_id":"BTC-USD","sequence":5,"type":"heartbeat"}}"#,
            "x".repeat(600)
        );
        assert_eq!(
            coinbase_route(raw.as_bytes()).unwrap(),
            CoinbaseRoute {
                kind: "heartbeat",
                product_id: Some("BTC-USD"),
                sequence: Some(5),
            }
        );
        // Control messages name no product.
        assert_eq!(
            coinbase_route(br#"{"type":"subscriptions","channels":[]}"#)
                .unwrap()
                .product_id,
            None
        );
    }

    /// Layouts as served on 2026-10-05.
    #[test]
    fn kraken_spot_messages_route_on_channel_and_symbol() {
        let book = br#"{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[{"price":86100.0,"qty":0.5}],"asks":[],"checksum":3129212817,"timestamp":"2026-10-05T12:17:56.551146Z"}]}"#;
        assert_eq!(kraken_channel(book), Some("book"));
        assert_eq!(kraken_symbol(book), Some("BTC/USD"));
        let ticker = br#"{"channel":"ticker","type":"snapshot","data":[{"symbol":"ETH/USD","bid":2716.03,"bid_qty":0.38996494,"ask":2716.04}]}"#;
        assert_eq!(kraken_symbol(ticker), Some("ETH/USD"));
        assert!(kraken_snapshot(ticker));
        assert!(!kraken_snapshot(book));
        for control in [
            br#"{"channel":"heartbeat"}"#.as_slice(),
            br#"{"method":"pong","time_in":"2026-10-05T12:18:06.683913Z","time_out":"2026-10-05T12:18:06.683928Z"}"#,
            br#"{"error":"Currency pair not supported XDG/USD","method":"subscribe","req_id":7,"success":false,"symbol":"XDG/USD"}"#,
        ] {
            assert_ne!(kraken_channel(control), Some("book"));
        }
        assert_eq!(kraken_channel(br#"{"method":"pong"}"#), None);
    }

    #[test]
    fn kraken_futures_messages_route_on_product_and_book_seq() {
        let delta = br#"{"feed":"book","product_id":"PF_XBTUSD","side":"sell","seq":210276307,"price":86452.0,"qty":0.0028,"timestamp":1791203430554}"#;
        assert_eq!(
            kraken_futures_book(delta),
            Some(("PF_XBTUSD", 210276307, false))
        );
        let snapshot = br#"{"feed":"book_snapshot","product_id":"PF_ETHUSD","timestamp":1791202675592,"seq":83132213,"tickSize":null,"bids":[{"price":2716.4,"qty":1.215}],"asks":[]}"#;
        assert_eq!(
            kraken_futures_book(snapshot),
            Some(("PF_ETHUSD", 83132213, true))
        );
        let trade = br#"{"product_id":"PF_XBTUSD","feed":"trade","uid":"f3a03bd2","side":"buy","type":"fill","time":1791202676588,"qty":0.0202,"price":86120.0,"seq":995981}"#;
        assert_eq!(kraken_futures_book(trade), None);
        assert_eq!(kraken_futures_product(trade), Some("PF_XBTUSD"));
        assert_eq!(kraken_futures_feed(trade), Some("trade"));
        let ticker = br#"{"time":1791204461715,"product_id":"PF_XBTUSD","funding_rate":0.462044140725,"funding_rate_prediction":0.32861499669002864,"relative_funding_rate":5.3608125e-6,"relative_funding_rate_prediction":3.827166666667e-6,"next_funding_rate_time":1791205200000,"leverage":"100x","premium":-0.0,"feed":"ticker","bid":85884.0,"ask":85888.0,"bid_size":0.028,"ask_size":0.1918,"volume":5495.0364,"dtm":0,"index":85888.1,"last":85888.0,"change":0.87,"suspended":false,"tag":"perpetual","pair":"XBT:USD","openInterest":2135.6829,"markPrice":85891.15455643082,"maturityTime":0,"post_only":false,"volumeQuote":472012710.4708,"open":85146.0,"high":86990.0,"low":85078.0}"#;
        assert_eq!(kraken_futures_product(ticker), Some("PF_XBTUSD"));
        assert_eq!(kraken_futures_book(ticker), None);
        assert_eq!(kraken_futures_feed(ticker), Some("ticker"));
    }

    #[test]
    fn hyperliquid_channel_is_found_without_parsing() {
        assert_eq!(
            hyperliquid_channel(br#"{"channel":"l2Book","data":{"coin":"BTC"}}"#),
            Some("l2Book")
        );
        assert_eq!(hyperliquid_channel(br#"{"method":"pong"}"#), None);
    }

    #[test]
    fn hyperliquid_routes_without_materializing_market_data() {
        let trade = br#"{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"1","sz":"2"},{"coin":"BTC","side":"A","px":"3","sz":"4"}]}"#;
        let trade: HyperliquidMessage<'_> = serde_json::from_slice(trade).unwrap();
        assert_eq!(trade.coin, Some("BTC"));

        let book = br#"{"channel":"l2Book","data":{"coin":"ETH","time":1,"levels":[[{"px":"1","sz":"2","n":3}],[]]}}"#;
        let book: HyperliquidMessage<'_> = serde_json::from_slice(book).unwrap();
        assert_eq!(book.coin, Some("ETH"));
    }

    #[test]
    fn hyperliquid_routes_when_data_precedes_channel() {
        let raw = br#"{"data":{"coin":"ETH","time":1,"levels":[]},"channel":"l2Book"}"#;
        let message: HyperliquidMessage<'_> = serde_json::from_slice(raw).unwrap();

        assert_eq!(message.coin, Some("ETH"));
    }

    #[test]
    fn hyperliquid_unknown_non_object_payload_is_ignored() {
        let raw = br#"{"channel":"error","data":"subscription failed"}"#;
        let message: HyperliquidMessage<'_> = serde_json::from_slice(raw).unwrap();

        assert_eq!(message.coin, None);
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
                message.coin,
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
