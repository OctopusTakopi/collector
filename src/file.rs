use std::{borrow::Cow, collections::HashMap, fs::File, io, io::Write, time::Duration};

use bytes::BufMut;
use jiff::Timestamp;
use tracing::{error, info, warn};
use zstd::stream::write::Encoder as ZstdEncoder;

use crate::symbol::Symbol;

pub type WriteRecord = (Timestamp, Symbol, bytes::Bytes);

const SYNC_ATTEMPTS: u32 = 3;

/// Characters left as-is in a filename.
///
/// Deliberately permissive: the point is to keep the exchange's own identifier
/// readable on disk, so only genuinely path-hostile bytes get escaped. `@` is
/// the obvious one to allow — Hyperliquid names 315 of its 316 spot pairs `@1`
/// … `@315` — and `+`/`=` show up in dated contracts elsewhere. All of these
/// are legal filename characters on Linux, macOS and Windows alike.
fn is_safe_in_filename(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'@' | b'+' | b'=')
}

/// Encode an exchange-supplied symbol into a filename component.
///
/// Some symbols are not valid path components — Hyperliquid reports spot pairs
/// as `PURR/USDC`, which would open a file inside a directory that was never
/// created and take the whole collector down with it.
///
/// The encoding must be **injective**. Folding unsafe bytes to a single `_`
/// would map `purr/usdc` and `purr_usdc` to the same name, and since each
/// symbol gets its own `RotatingFile`, two independent zstd encoders would
/// append interleaved frames to one file and render it undecodable. Percent
/// escaping avoids that: `%` is itself unsafe, so it is always escaped and no
/// two distinct symbols can collide.
fn encode_symbol(symbol: &str) -> Cow<'_, str> {
    if symbol.bytes().all(is_safe_in_filename) {
        return Cow::Borrowed(symbol);
    }
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(symbol.len() + 8);
    for byte in symbol.bytes() {
        if is_safe_in_filename(byte) {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push(HEX[(byte >> 4) as usize] as char);
            encoded.push(HEX[(byte & 0x0f) as usize] as char);
        }
    }
    Cow::Owned(encoded)
}

pub struct RotatingFile {
    next_rotation: i64,
    path: String,
    file: Option<ZstdEncoder<'static, File>>,
    buf: bytes::BytesMut,
    /// Set when a rotation could not be finalized, so the already-rotated file
    /// may be missing its zstd footer. Collection continues, but the process
    /// must not report a clean exit.
    degraded: bool,
}

impl RotatingFile {
    fn create(
        timestamp: Timestamp,
        path: &str,
    ) -> Result<(ZstdEncoder<'static, File>, i64), io::Error> {
        let zoned = timestamp.to_zoned(jiff::tz::TimeZone::UTC);
        let date_str = zoned.date().strftime("%Y%m%d");
        let file = File::options()
            .create(true)
            .append(true)
            .open(format!("{path}_{date_str}.zst"))?;

        let next_rotation = zoned
            .date()
            .tomorrow()
            .map_err(io::Error::other)?
            .at(0, 0, 0, 0)
            .to_zoned(jiff::tz::TimeZone::UTC)
            .map_err(io::Error::other)?
            .timestamp()
            .as_nanosecond();

        Ok((ZstdEncoder::new(file, 1)?, next_rotation as i64))
    }

    pub fn new(timestamp: Timestamp, path: String) -> Result<Self, io::Error> {
        let (file, next_rotation) = Self::create(timestamp, &path)?;
        Ok(Self {
            next_rotation,
            file: Some(file),
            path,
            buf: bytes::BytesMut::with_capacity(8 * 1024),
            degraded: false,
        })
    }

    pub fn finalize(&mut self) -> io::Result<()> {
        let Some(encoder) = self.file.take() else {
            return Ok(());
        };

        let file = encoder.finish().map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("failed to finish zstd stream {}: {error}", self.path),
            )
        })?;

        // Retry a transient fsync failure, then let the last attempt speak for
        // itself — no unreachable arm to fall out of sync with the bound.
        for attempt in 1..SYNC_ATTEMPTS {
            match file.sync_all() {
                Ok(()) => return Ok(()),
                Err(error) => {
                    warn!(path = %self.path, attempt, %error, "sync_all failed; retrying");
                    std::thread::sleep(Duration::from_millis(25 * u64::from(attempt)));
                }
            }
        }
        file.sync_all().map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "failed to sync {} after {SYNC_ATTEMPTS} attempts: {error}",
                    self.path
                ),
            )
        })
    }

    pub fn write(&mut self, timestamp: Timestamp, data: bytes::Bytes) -> Result<(), io::Error> {
        let ts_nanos = timestamp.as_nanosecond();
        if ts_nanos >= self.next_rotation as i128 {
            if let Err(error) = self.finalize() {
                // Failing to close yesterday's file must not stop today's data
                // for this symbol, let alone for every other symbol sharing the
                // writer thread. `degraded` carries the failure to the exit code.
                error!(
                    path = %self.path,
                    %error,
                    "failed to finalize file on rotation; continuing with the new file"
                );
                self.degraded = true;
            }
            let (new_file, next_rotation) = Self::create(timestamp, &self.path)?;
            self.file = Some(new_file);
            self.next_rotation = next_rotation;
            info!(%self.path, "date is changed, file rotated");
        }

        self.buf.clear();
        let mut itoa_buf = itoa::Buffer::new();
        self.buf
            .put_slice(itoa_buf.format(ts_nanos as i64).as_bytes());

        self.buf.put_u8(b' ');
        self.buf.put(data);
        self.buf.put_u8(b'\n');

        // Never `unwrap`: the release profile is `panic = "abort"`, so a panic
        // here would skip every `Drop` and truncate all the other symbols' files.
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| io::Error::other(format!("{} has no open file", self.path)))?;
        file.write_all(&self.buf)
    }
}

impl Drop for RotatingFile {
    fn drop(&mut self) {
        if let Err(error) = self.finalize() {
            warn!(path = %self.path, %error, "failed to finalize file on drop");
        }
    }
}

pub struct Writer {
    path: String,
    files: HashMap<Symbol, RotatingFile>,
}

impl Writer {
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
            files: Default::default(),
        }
    }

    pub fn write(
        &mut self,
        recv_time: Timestamp,
        symbol: Symbol,
        data: bytes::Bytes,
    ) -> Result<(), anyhow::Error> {
        // Keyed by the encoded name, not the raw symbol: one `RotatingFile` per
        // file on disk is what keeps two encoders from ever sharing an fd.
        let name = encode_symbol(&symbol);
        if let Some(rotating_file) = self.files.get_mut(name.as_ref()) {
            rotating_file.write(recv_time, data)?;
        } else {
            let path = format!("{}/{}", self.path, name);
            let mut rotating_file = RotatingFile::new(recv_time, path)?;
            rotating_file.write(recv_time, data)?;
            self.files
                .insert(Symbol::from(name.as_ref()), rotating_file);
        }
        Ok(())
    }

    pub fn close(&mut self) -> Result<(), anyhow::Error> {
        let mut result = Ok(());
        for (symbol, file) in &mut self.files {
            let degraded = file.degraded;
            match file.finalize() {
                Ok(()) if degraded => {
                    error!(
                        symbol = %symbol,
                        "file closed, but an earlier rotation could not be finalized"
                    );
                    if result.is_ok() {
                        result = Err(anyhow::anyhow!(
                            "{symbol}: an earlier rotation could not be finalized"
                        ));
                    }
                }
                Ok(()) => info!(symbol = %symbol, "file closed cleanly"),
                Err(error) => {
                    error!(symbol = %symbol, %error, "failed to close file");
                    if result.is_ok() {
                        result = Err(error.into());
                    }
                }
            }
        }
        self.files.clear();
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every symbol shape the collector actually sees must survive untouched —
    /// no allocation, and the exchange's own identifier readable on disk.
    #[test]
    fn real_exchange_symbols_are_not_rewritten() {
        for symbol in [
            "btcusdt",     // Binance spot / Bybit
            "btcusd_perp", // Binance COIN-M
            "btc-usdt",    // OKX style
            "btc",         // Hyperliquid perp
            "@1",          // Hyperliquid spot, 315 of its 316 pairs
            "@315",
        ] {
            assert!(
                matches!(encode_symbol(symbol), Cow::Borrowed(_)),
                "{symbol} should pass through unchanged"
            );
            assert_eq!(encode_symbol(symbol), symbol);
        }
    }

    #[test]
    fn path_separators_in_symbols_are_escaped() {
        assert_eq!(encode_symbol("purr/usdc"), "purr%2Fusdc");
        assert_eq!(encode_symbol("../../etc/passwd"), "..%2F..%2Fetc%2Fpasswd");
    }

    /// Two distinct symbols must never produce the same filename: they each get
    /// their own zstd encoder, and sharing a file would interleave frames.
    #[test]
    fn encoding_is_injective() {
        let symbols = [
            "purr/usdc",
            "purr_usdc",
            "purr%2Fusdc",
            "purr%usdc",
            "PURR/USDC",
            "purr usdc",
            "purr-usdc",
            "",
        ];
        let mut encoded: Vec<String> = symbols
            .iter()
            .map(|symbol| encode_symbol(symbol).into_owned())
            .collect();
        let total = encoded.len();
        encoded.sort();
        encoded.dedup();
        assert_eq!(encoded.len(), total, "collision: {encoded:?}");
    }

    #[test]
    fn colliding_symbols_get_separate_files() {
        let dir = std::env::temp_dir().join(format!(
            "collector-collision-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();

        let mut writer = Writer::new(dir.to_str().unwrap());
        for symbol in ["purr/usdc", "purr_usdc"] {
            writer
                .write(
                    Timestamp::now(),
                    Symbol::from(symbol),
                    bytes::Bytes::from_static(b"{}"),
                )
                .unwrap();
        }
        writer.close().unwrap();

        let mut written: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        written.sort();
        assert_eq!(written.len(), 2, "{written:?}");
        assert!(written[0].starts_with("purr%2Fusdc_"), "{written:?}");
        assert!(written[1].starts_with("purr_usdc_"), "{written:?}");

        // Both must be independently decodable.
        for name in &written {
            let bytes = std::fs::read(dir.join(name)).unwrap();
            assert!(zstd::decode_all(bytes.as_slice()).is_ok(), "{name}");
        }

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn writes_a_symbol_containing_a_path_separator() {
        let dir = std::env::temp_dir().join(format!(
            "collector-file-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();

        let mut writer = Writer::new(dir.to_str().unwrap());
        writer
            .write(
                Timestamp::now(),
                Symbol::from("purr/usdc"),
                bytes::Bytes::from_static(b"{}"),
            )
            .unwrap();
        writer.close().unwrap();

        let written: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(written.len(), 1, "{written:?}");
        assert!(written[0].starts_with("purr%2Fusdc_"), "{written:?}");

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
