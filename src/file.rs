use std::{
    borrow::Cow, collections::HashMap, fs::File, io, io::Write, path::PathBuf, time::Duration,
};

use bytes::BufMut;
use jiff::Timestamp;
use tracing::{error, info, warn};
use zstd::stream::write::Encoder as ZstdEncoder;

use crate::quality::{QualityEvent, QualityReporter};
use crate::symbol::Symbol;

pub type WriteRecord = (Timestamp, Symbol, bytes::Bytes);

const SYNC_ATTEMPTS: u32 = 3;
const SEGMENT_NANOS: i64 = 60_000_000_000;

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
    run_id: String,
    late_sequence: u64,
    file: Option<ZstdEncoder<'static, File>>,
    active_path: Option<PathBuf>,
    final_path: Option<PathBuf>,
    buf: bytes::BytesMut,
    /// Set when a rotation could not be finalized, so the already-rotated file
    /// may be missing its zstd footer. Collection continues, but the process
    /// must not report a clean exit.
    degraded: bool,
    quality: QualityReporter,
}

impl RotatingFile {
    fn create(
        timestamp: Timestamp,
        path: &str,
        run_id: &str,
    ) -> Result<(ZstdEncoder<'static, File>, PathBuf, PathBuf, i64), io::Error> {
        let timestamp_ns = timestamp.as_nanosecond() as i64;
        let segment_start = timestamp_ns.div_euclid(SEGMENT_NANOS) * SEGMENT_NANOS;
        let next_rotation = segment_start.saturating_add(SEGMENT_NANOS);
        let zoned = timestamp.to_zoned(jiff::tz::TimeZone::UTC);
        let date_str = zoned.date().strftime("%Y%m%d");
        let final_path = PathBuf::from(format!("{path}_{date_str}_{segment_start}_{run_id}.zst"));
        let active_path = PathBuf::from(format!("{}.part", final_path.display()));
        let file = File::options()
            .create_new(true)
            .write(true)
            .open(&active_path)?;

        Ok((
            ZstdEncoder::new(file, 1)?,
            active_path,
            final_path,
            next_rotation,
        ))
    }

    pub fn new(
        timestamp: Timestamp,
        path: String,
        run_id: String,
        quality: QualityReporter,
    ) -> Result<Self, io::Error> {
        let (file, active_path, final_path, next_rotation) =
            Self::create(timestamp, &path, &run_id)?;
        Ok(Self {
            next_rotation,
            file: Some(file),
            path,
            run_id,
            late_sequence: 0,
            active_path: Some(active_path),
            final_path: Some(final_path),
            buf: bytes::BytesMut::with_capacity(8 * 1024),
            degraded: false,
            quality,
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
        let mut synced = false;
        for attempt in 1..SYNC_ATTEMPTS {
            match file.sync_all() {
                Ok(()) => {
                    synced = true;
                    break;
                }
                Err(error) => {
                    warn!(path = %self.path, attempt, %error, "sync_all failed; retrying");
                    std::thread::sleep(Duration::from_millis(25 * u64::from(attempt)));
                }
            }
        }
        if !synced && let Err(error) = file.sync_all() {
            return Err(io::Error::new(
                error.kind(),
                format!(
                    "failed to sync {} after {SYNC_ATTEMPTS} attempts: {error}",
                    self.path
                ),
            ));
        }
        drop(file);

        let active_path = self
            .active_path
            .take()
            .ok_or_else(|| io::Error::other("active segment path is missing"))?;
        let final_path = self
            .final_path
            .take()
            .ok_or_else(|| io::Error::other("final segment path is missing"))?;
        std::fs::rename(&active_path, &final_path)?;
        if let Some(parent) = final_path.parent() {
            File::open(parent)?.sync_all()?;
        }
        Ok(())
    }

    fn finalize_or_degrade(&mut self, trigger: &'static str) -> bool {
        if let Err(error) = self.finalize() {
            error!(
                path = %self.path,
                %error,
                trigger,
                "failed to finalize recording segment; collection is degraded"
            );
            self.degraded = true;
            self.quality.report(QualityEvent::StorageDegraded {
                at_ns: QualityEvent::now_ns(),
                target: self.path.clone(),
                error: error.to_string(),
            });
            false
        } else {
            true
        }
    }

    fn finalize_if_expired(&mut self, now: Timestamp) {
        if self.file.is_some()
            && now.as_nanosecond() >= i128::from(self.next_rotation)
            && self.finalize_or_degrade("wall_clock")
        {
            info!(path = %self.path, "idle recording segment finalized");
        }
    }

    fn open_late_segment(&mut self, timestamp: Timestamp) -> io::Result<()> {
        self.late_sequence = self.late_sequence.saturating_add(1);
        let run_token = format!("{}.late{}", self.run_id, self.late_sequence);
        let (file, active_path, final_path, next_rotation) =
            Self::create(timestamp, &self.path, &run_token)?;
        self.file = Some(file);
        self.active_path = Some(active_path);
        self.final_path = Some(final_path);
        self.next_rotation = next_rotation;
        info!(path = %self.path, "opened a late-record segment after wall-clock finalization");
        Ok(())
    }

    pub fn write(&mut self, timestamp: Timestamp, data: bytes::Bytes) -> Result<(), io::Error> {
        let ts_nanos = timestamp.as_nanosecond();
        // A wall-clock tick may have finalized this minute before a record
        // timestamped in it traversed both bounded queues. Keep the completed
        // file immutable and put such records in another independently durable
        // segment instead of terminating the collector.
        if self.file.is_none() && ts_nanos < i128::from(self.next_rotation) {
            self.open_late_segment(timestamp)?;
        }
        if ts_nanos >= self.next_rotation as i128 {
            // Failing to close one segment must not stop data for every other
            // symbol. `degraded` carries the failure to the process exit code.
            let _ = self.finalize_or_degrade("record");
            let (new_file, active_path, final_path, next_rotation) =
                Self::create(timestamp, &self.path, &self.run_id)?;
            self.file = Some(new_file);
            self.active_path = Some(active_path);
            self.final_path = Some(final_path);
            self.next_rotation = next_rotation;
            info!(%self.path, "recording segment rotated");
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
    run_id: String,
    files: HashMap<Symbol, RotatingFile>,
    quality: QualityReporter,
}

impl Writer {
    pub fn new(path: &str, run_id: &str, quality: QualityReporter) -> Self {
        Self {
            path: path.to_string(),
            run_id: run_id.to_owned(),
            files: Default::default(),
            quality,
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
            let mut rotating_file =
                RotatingFile::new(recv_time, path, self.run_id.clone(), self.quality.clone())?;
            rotating_file.write(recv_time, data)?;
            self.files
                .insert(Symbol::from(name.as_ref()), rotating_file);
        }
        Ok(())
    }

    /// Finalize completed minute segments even when no later record arrives.
    pub fn finalize_expired(&mut self, now: Timestamp) {
        for file in self.files.values_mut() {
            file.finalize_if_expired(now);
        }
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

        let mut writer = Writer::new(
            dir.to_str().unwrap(),
            "test-run",
            QualityReporter::disabled(),
        );
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

        let mut writer = Writer::new(
            dir.to_str().unwrap(),
            "test-run",
            QualityReporter::disabled(),
        );
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

    #[test]
    fn completed_minutes_are_independent_durable_segments() {
        let dir = std::env::temp_dir().join(format!(
            "collector-segment-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut writer = Writer::new(
            dir.to_str().unwrap(),
            "segment-run",
            QualityReporter::disabled(),
        );
        let first = Timestamp::from_nanosecond(1_800_000_000_000_000_000).unwrap();
        let second = first
            .checked_add(jiff::Span::new().try_minutes(1).unwrap())
            .unwrap();
        for recv_time in [first, second] {
            writer
                .write(
                    recv_time,
                    Symbol::from("btcusdt"),
                    bytes::Bytes::from_static(b"{}"),
                )
                .unwrap();
        }
        writer.close().unwrap();

        let paths: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        assert_eq!(paths.len(), 2, "{paths:?}");
        assert!(paths.iter().all(|path| path.extension().unwrap() == "zst"));
        for path in paths {
            let bytes = std::fs::read(path).unwrap();
            assert!(zstd::decode_all(bytes.as_slice()).is_ok());
        }

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn idle_completed_minute_is_finalized_by_wall_clock() {
        let dir = std::env::temp_dir().join(format!(
            "collector-idle-segment-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut writer = Writer::new(
            dir.to_str().unwrap(),
            "idle-run",
            QualityReporter::disabled(),
        );
        let first = Timestamp::from_nanosecond(1_800_000_000_000_000_000).unwrap();
        writer
            .write(
                first,
                Symbol::from("btcusdt"),
                bytes::Bytes::from_static(b"{}"),
            )
            .unwrap();
        let after_boundary = first
            .checked_add(jiff::Span::new().try_minutes(1).unwrap())
            .unwrap();
        // Another symbol remains active; expiration must not depend on the
        // whole writer queue becoming idle.
        writer
            .write(
                after_boundary,
                Symbol::from("ethusdt"),
                bytes::Bytes::from_static(b"{}"),
            )
            .unwrap();
        writer.finalize_expired(after_boundary);

        let paths: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        assert_eq!(paths.len(), 2, "{paths:?}");
        let btc = paths
            .iter()
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("btcusdt_")
            })
            .unwrap();
        let eth = paths
            .iter()
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("ethusdt_")
            })
            .unwrap();
        assert_eq!(btc.extension().unwrap(), "zst");
        assert_eq!(eth.extension().unwrap(), "part");
        assert!(zstd::decode_all(std::fs::read(btc).unwrap().as_slice()).is_ok());
        writer.close().unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn delayed_record_reopens_a_finalized_minute() {
        let dir = std::env::temp_dir().join(format!(
            "collector-late-segment-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut writer = Writer::new(
            dir.to_str().unwrap(),
            "late-run",
            QualityReporter::disabled(),
        );
        let before_boundary = Timestamp::from_nanosecond(1_800_000_059_000_000_000).unwrap();
        let after_boundary = Timestamp::from_nanosecond(1_800_000_060_000_000_000).unwrap();
        writer
            .write(
                before_boundary,
                Symbol::from("btcusdt"),
                bytes::Bytes::from_static(b"{\"n\":1}"),
            )
            .unwrap();
        writer.finalize_expired(after_boundary);
        writer
            .write(
                before_boundary,
                Symbol::from("btcusdt"),
                bytes::Bytes::from_static(b"{\"n\":2}"),
            )
            .unwrap();
        writer.close().unwrap();

        let paths: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        assert_eq!(paths.len(), 2, "{paths:?}");
        assert!(paths.iter().all(|path| path.extension().unwrap() == "zst"));
        assert!(
            paths
                .iter()
                .any(|path| path.to_string_lossy().contains(".late1.zst"))
        );
        for path in paths {
            assert!(zstd::decode_all(std::fs::read(path).unwrap().as_slice()).is_ok());
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
}
