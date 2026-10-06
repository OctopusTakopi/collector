//! The startup check of the command line's symbols against the venue's own
//! listings. A symbol the venue does not list would otherwise leave every
//! session settled incomplete, never ready, with a rolling update stalled
//! behind it; checked first, it stops the process with the venue's answer
//! before anything starts.

use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use serde::Deserialize;
use tokio::time::Instant;
use tracing::warn;

use crate::collection::Exchange;

/// How long a venue whose listings cannot be had is asked again before the
/// process gives up: long enough to ride out a REST blip at a cold start.
const PATIENCE: Duration = Duration::from_secs(120);

/// The first pause between attempts, doubled up to [`MAX_RETRY_DELAY`].
const RETRY_DELAY: Duration = Duration::from_secs(2);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

const BINANCE_SPOT: &str = "https://api.binance.com/api/v3/exchangeInfo?symbol=";
const BINANCE_USDM: &str = "https://fapi.binance.com/fapi/v1/exchangeInfo";
const BINANCE_COINM: &str = "https://dapi.binance.com/dapi/v1/exchangeInfo";
const BYBIT: &str = "https://api.bybit.com/v5/market/instruments-info?category=linear&symbol=";
const HYPERLIQUID: &str = "https://api.hyperliquid.xyz/info";
const COINBASE: &str = "https://api.exchange.coinbase.com/products/";
const KRAKEN_SPOT: &str = "https://api.kraken.com/0/public/AssetPairs?pair=";
const KRAKEN_FUTURES: &str = "https://futures.kraken.com/derivatives/api/v3/instruments";

/// What a venue said of the symbols: the ones it does not list, each with
/// its reason. An `Err` is no answer at all, worth asking again.
type Answer = anyhow::Result<Vec<String>>;

/// Checks `symbols` against `exchange`'s listings.
pub(crate) async fn check(exchange: Exchange, symbols: &[String]) -> anyhow::Result<()> {
    // Coinbase answers 400 to a request without one.
    let client = reqwest::Client::builder()
        .user_agent(concat!(
            env!("CARGO_PKG_NAME"),
            "/",
            env!("CARGO_PKG_VERSION")
        ))
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .build()?;
    let unlisted = patiently(PATIENCE, || unlisted(&client, exchange, symbols)).await?;
    if unlisted.is_empty() {
        Ok(())
    } else {
        bail!("the venue does not list {}", unlisted.join("; "))
    }
}

/// Asks `ask` until it answers, pausing longer each time, for at most
/// `patience`.
async fn patiently<F, Fut>(patience: Duration, mut ask: F) -> Answer
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Answer>,
{
    let deadline = Instant::now() + patience;
    let mut delay = RETRY_DELAY;
    loop {
        match ask().await {
            Ok(unlisted) => return Ok(unlisted),
            Err(error) if Instant::now() + delay < deadline => {
                warn!(%error, ?delay, "cannot read the venue's listings; asking again");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(MAX_RETRY_DELAY);
            }
            Err(error) => {
                return Err(error.context(format!(
                    "cannot check the symbols against the venue's listings within {patience:?}"
                )));
            }
        }
    }
}

async fn unlisted(client: &reqwest::Client, exchange: Exchange, symbols: &[String]) -> Answer {
    match exchange {
        Exchange::BinanceSpot => {
            let mut unlisted = Vec::new();
            for symbol in symbols {
                let symbol = symbol.to_ascii_uppercase();
                let (status, body) = get(client, &format!("{BINANCE_SPOT}{symbol}")).await?;
                unlisted.extend(binance_spot(&symbol, status, &body)?);
            }
            Ok(unlisted)
        }
        Exchange::BinanceFuturesUm | Exchange::BinanceFuturesCm => {
            let url = match exchange {
                Exchange::BinanceFuturesUm => BINANCE_USDM,
                _ => BINANCE_COINM,
            };
            let body = get_ok(client, url).await?;
            binance_futures(symbols, &body)
        }
        Exchange::Bybit => {
            let mut unlisted = Vec::new();
            for symbol in symbols {
                let symbol = symbol.to_ascii_uppercase();
                let body = get_ok(client, &format!("{BYBIT}{symbol}")).await?;
                unlisted.extend(bybit(&symbol, &body)?);
            }
            Ok(unlisted)
        }
        Exchange::Hyperliquid => {
            let mut bodies = Vec::new();
            for kind in ["meta", "spotMeta"] {
                let response = client
                    .post(HYPERLIQUID)
                    .header("content-type", "application/json")
                    .body(serde_json::json!({ "type": kind }).to_string())
                    .send()
                    .await?;
                bodies.push(ok(response).await?);
            }
            hyperliquid(symbols, &bodies[0], &bodies[1])
        }
        Exchange::Coinbase => {
            let mut unlisted = Vec::new();
            for product in symbols {
                let product = product.to_ascii_uppercase();
                let (status, body) = get(client, &format!("{COINBASE}{product}")).await?;
                unlisted.extend(coinbase(&product, status, &body)?);
            }
            Ok(unlisted)
        }
        Exchange::KrakenSpot => {
            let pairs: Vec<String> = symbols
                .iter()
                .map(|pair| pair.to_ascii_uppercase())
                .collect();
            let body = get_ok(client, &format!("{KRAKEN_SPOT}{}", pairs.join(","))).await?;
            match kraken_spot(&pairs, &body)? {
                KrakenSpot::Listed => Ok(Vec::new()),
                // Which pair Kraken does not know, it does not say.
                KrakenSpot::Unknown => {
                    let mut unlisted = Vec::new();
                    for pair in &pairs {
                        let body = get_ok(client, &format!("{KRAKEN_SPOT}{pair}")).await?;
                        if let KrakenSpot::Unknown = kraken_spot(std::slice::from_ref(pair), &body)?
                        {
                            unlisted.push(format!("{pair} (unknown asset pair)"));
                        }
                    }
                    Ok(unlisted)
                }
            }
        }
        Exchange::KrakenFutures => {
            let body = get_ok(client, KRAKEN_FUTURES).await?;
            kraken_futures(symbols, &body)
        }
    }
}

async fn get(client: &reqwest::Client, url: &str) -> anyhow::Result<(u16, Vec<u8>)> {
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    let status = response.status().as_u16();
    Ok((status, response.bytes().await?.to_vec()))
}

/// A body from a 2xx answer; anything else is no answer.
async fn get_ok(client: &reqwest::Client, url: &str) -> anyhow::Result<Vec<u8>> {
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    ok(response).await
}

async fn ok(response: reqwest::Response) -> anyhow::Result<Vec<u8>> {
    let status = response.status();
    let url = response.url().clone();
    let body = response.bytes().await?;
    if !status.is_success() {
        bail!("{url} answered {status}: {}", preview(&body));
    }
    Ok(body.to_vec())
}

fn preview(body: &[u8]) -> String {
    String::from_utf8_lossy(&body[..body.len().min(256)]).into_owned()
}

/// `exchangeInfo?symbol=`: the symbol, or `-1121 Invalid symbol`.
fn binance_spot(symbol: &str, status: u16, body: &[u8]) -> Answer {
    #[derive(Deserialize)]
    struct Error {
        code: i64,
    }
    match status {
        200..=299 => Ok(Vec::new()),
        400 if serde_json::from_slice::<Error>(body).is_ok_and(|error| error.code == -1121) => {
            Ok(vec![format!("{symbol} ({})", preview(body))])
        }
        _ => Err(anyhow!("exchangeInfo answered {status}: {}", preview(body))),
    }
}

/// A futures `exchangeInfo`: every symbol the market lists.
fn binance_futures(symbols: &[String], body: &[u8]) -> Answer {
    #[derive(Deserialize)]
    struct Info {
        symbols: Vec<Listed>,
    }
    #[derive(Deserialize)]
    struct Listed {
        symbol: String,
    }
    let info: Info = serde_json::from_slice(body).context("unreadable exchangeInfo")?;
    Ok(symbols
        .iter()
        .filter(|symbol| {
            !info
                .symbols
                .iter()
                .any(|listed| listed.symbol.eq_ignore_ascii_case(symbol))
        })
        .map(|symbol| format!("{symbol} (not in exchangeInfo)"))
        .collect())
}

/// `instruments-info`: the instrument, or `10001 params error: symbol
/// invalid`. Any other code, a rate limit among them, is no answer.
fn bybit(symbol: &str, body: &[u8]) -> Answer {
    #[derive(Deserialize)]
    struct Reply {
        #[serde(rename = "retCode")]
        ret_code: i64,
        #[serde(rename = "retMsg")]
        ret_msg: String,
        result: Option<Instruments>,
    }
    #[derive(Deserialize)]
    struct Instruments {
        list: Vec<serde_json::Value>,
    }
    let reply: Reply = serde_json::from_slice(body).context("unreadable instruments-info")?;
    match reply.ret_code {
        0 if reply.result.is_some_and(|result| !result.list.is_empty()) => Ok(Vec::new()),
        0 | 10001 => Ok(vec![format!("{symbol} ({})", reply.ret_msg)]),
        code => Err(anyhow!(
            "instruments-info answered {code}: {}",
            reply.ret_msg
        )),
    }
}

/// `meta` and `spotMeta`: perpetuals by name, spot pairs by name and by
/// `@<index>`. Names are compared exactly: `kPEPE` is not `KPEPE`.
fn hyperliquid(coins: &[String], meta: &[u8], spot_meta: &[u8]) -> Answer {
    #[derive(Deserialize)]
    struct Universe {
        universe: Vec<Asset>,
    }
    #[derive(Deserialize)]
    struct Asset {
        name: String,
        index: Option<usize>,
    }
    let perps: Universe = serde_json::from_slice(meta).context("unreadable meta")?;
    let spot: Universe = serde_json::from_slice(spot_meta).context("unreadable spotMeta")?;
    let listed = |coin: &str| {
        perps.universe.iter().any(|asset| asset.name == coin)
            || spot.universe.iter().any(|asset| {
                asset.name == coin
                    || asset
                        .index
                        .is_some_and(|index| coin.strip_prefix('@') == Some(&index.to_string()))
            })
    };
    Ok(coins
        .iter()
        .filter(|coin| !listed(coin))
        .map(|coin| format!("{coin} (in neither meta nor spotMeta)"))
        .collect())
}

/// `products/<id>`: the product, or `404 NotFound`.
fn coinbase(product: &str, status: u16, body: &[u8]) -> Answer {
    match status {
        200..=299 => Ok(Vec::new()),
        404 => Ok(vec![format!("{product} ({})", preview(body))]),
        _ => Err(anyhow!("products answered {status}: {}", preview(body))),
    }
}

/// What `AssetPairs` said of a set of pairs.
enum KrakenSpot {
    Listed,
    /// At least one of them is not a pair Kraken knows.
    Unknown,
}

/// `AssetPairs?pair=`: every pair keyed as asked, or `EQuery:Unknown asset
/// pair` for the whole request. Any other error is no answer.
fn kraken_spot(pairs: &[String], body: &[u8]) -> anyhow::Result<KrakenSpot> {
    #[derive(Deserialize)]
    struct Reply {
        error: Vec<String>,
        #[serde(default)]
        result: serde_json::Map<String, serde_json::Value>,
    }
    let reply: Reply = serde_json::from_slice(body).context("unreadable AssetPairs")?;
    if reply
        .error
        .iter()
        .any(|error| error == "EQuery:Unknown asset pair")
    {
        return Ok(KrakenSpot::Unknown);
    }
    if !reply.error.is_empty() {
        bail!("AssetPairs answered {}", reply.error.join(", "));
    }
    Ok(
        if pairs.iter().all(|pair| reply.result.contains_key(pair)) {
            KrakenSpot::Listed
        } else {
            KrakenSpot::Unknown
        },
    )
}

/// `instruments`: every contract Kraken Futures lists.
fn kraken_futures(products: &[String], body: &[u8]) -> Answer {
    #[derive(Deserialize)]
    struct Reply {
        instruments: Vec<Instrument>,
    }
    #[derive(Deserialize)]
    struct Instrument {
        symbol: String,
    }
    let reply: Reply = serde_json::from_slice(body).context("unreadable instruments")?;
    Ok(products
        .iter()
        .filter(|product| {
            !reply
                .instruments
                .iter()
                .any(|instrument| instrument.symbol.eq_ignore_ascii_case(product))
        })
        .map(|product| format!("{product} (not in instruments)"))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn symbols(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    /// Replies as served on 2026-10-07.
    #[test]
    fn binance_spot_names_an_invalid_symbol_and_retries_anything_else() {
        assert!(
            binance_spot("BTCUSDT", 200, br#"{"symbols":[]}"#)
                .unwrap()
                .is_empty()
        );
        let unlisted =
            binance_spot("FOOBAR", 400, br#"{"code":-1121,"msg":"Invalid symbol."}"#).unwrap();
        assert_eq!(
            unlisted,
            [r#"FOOBAR ({"code":-1121,"msg":"Invalid symbol."})"#]
        );
        assert!(binance_spot("BTCUSDT", 418, br#"{"code":-1003}"#).is_err());
        assert!(binance_spot("BTCUSDT", 400, br#"{"code":-1100}"#).is_err());
    }

    #[test]
    fn binance_futures_finds_symbols_in_any_case() {
        let body = br#"{"symbols":[{"symbol":"BTCUSDT","status":"TRADING"},{"symbol":"ETHUSDT"}]}"#;
        assert_eq!(
            binance_futures(&symbols(&["btcusdt", "ETHUSDT", "fooUSDT"]), body).unwrap(),
            ["fooUSDT (not in exchangeInfo)"]
        );
    }

    #[test]
    fn bybit_tells_an_invalid_symbol_from_a_rate_limit() {
        assert!(
            bybit(
                "BTCUSDT",
                br#"{"retCode":0,"retMsg":"OK","result":{"list":[{"symbol":"BTCUSDT"}]}}"#
            )
            .unwrap()
            .is_empty()
        );
        assert_eq!(
            bybit("FOOUSDT", br#"{"retCode":10001,"retMsg":"params error: symbol invalid","result":{"list":[]}}"#)
                .unwrap(),
            ["FOOUSDT (params error: symbol invalid)"]
        );
        assert!(
            bybit(
                "BTCUSDT",
                br#"{"retCode":10006,"retMsg":"Too many visits!"}"#
            )
            .is_err()
        );
    }

    #[test]
    fn hyperliquid_knows_perpetuals_spot_pairs_and_their_indices() {
        let meta = br#"{"universe":[{"name":"BTC"},{"name":"kPEPE"}]}"#;
        let spot = br#"{"universe":[{"name":"PURR/USDC","index":0},{"name":"@1","index":1},{"name":"@107","index":107}]}"#;
        assert_eq!(
            hyperliquid(
                &symbols(&["BTC", "kPEPE", "KPEPE", "PURR/USDC", "@107", "@108"]),
                meta,
                spot
            )
            .unwrap(),
            [
                "KPEPE (in neither meta nor spotMeta)",
                "@108 (in neither meta nor spotMeta)"
            ]
        );
    }

    #[test]
    fn coinbase_names_a_missing_product() {
        assert!(coinbase("BTC-USD", 200, b"{}").unwrap().is_empty());
        assert_eq!(
            coinbase("FOO-USD", 404, br#"{"message":"NotFound"}"#).unwrap(),
            [r#"FOO-USD ({"message":"NotFound"})"#]
        );
        assert!(coinbase("BTC-USD", 503, b"").is_err());
    }

    #[test]
    fn kraken_spot_reports_an_unknown_pair_for_the_whole_request() {
        let pairs = symbols(&["BTC/USD", "DOGE/USD"]);
        assert!(matches!(
            kraken_spot(
                &pairs,
                br#"{"error":[],"result":{"BTC/USD":{"wsname":"XBT/USD"},"DOGE/USD":{"wsname":"XDG/USD"}}}"#
            )
            .unwrap(),
            KrakenSpot::Listed
        ));
        assert!(matches!(
            kraken_spot(&pairs, br#"{"error":["EQuery:Unknown asset pair"]}"#).unwrap(),
            KrakenSpot::Unknown
        ));
        assert!(kraken_spot(&pairs, br#"{"error":["EGeneral:Too many requests"]}"#).is_err());
    }

    #[test]
    fn kraken_futures_finds_contracts_in_any_case() {
        let body =
            br#"{"result":"success","instruments":[{"symbol":"PF_XBTUSD","tradeable":true}]}"#;
        assert_eq!(
            kraken_futures(&symbols(&["pf_xbtusd", "PF_FOOUSD"]), body).unwrap(),
            ["PF_FOOUSD (not in instruments)"]
        );
    }

    /// No answer is asked again, pausing longer each time, until one comes.
    #[tokio::test(start_paused = true)]
    async fn an_unreachable_venue_is_asked_again_until_it_answers() {
        let mut attempts = 0;
        let start = Instant::now();
        let answer = patiently(PATIENCE, || {
            attempts += 1;
            let answer = if attempts < 4 {
                Err(anyhow!("connection refused"))
            } else {
                Ok(vec!["FOO (unknown)".to_owned()])
            };
            async move { answer }
        })
        .await
        .unwrap();
        assert_eq!(answer, ["FOO (unknown)"]);
        assert_eq!(Instant::now() - start, RETRY_DELAY * (1 + 2 + 4));
    }

    #[tokio::test(start_paused = true)]
    async fn a_venue_that_never_answers_is_given_up_within_the_patience() {
        let start = Instant::now();
        let error = patiently(PATIENCE, || async { Err(anyhow!("connection refused")) })
            .await
            .unwrap_err();
        assert!(Instant::now() - start <= PATIENCE);
        assert!(
            format!("{error:#}").contains("connection refused"),
            "{error:#}"
        );
    }
}
