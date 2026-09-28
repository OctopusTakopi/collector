use std::{
    borrow::Cow,
    collections::HashMap,
    fs::File,
    io::{self, Seek, SeekFrom, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{FileExt, MetadataExt},
    },
    time::{Duration, Instant},
};

use bytes::BufMut;
use jiff::Timestamp;
use tracing::{error, info, warn};
use zstd::stream::write::Encoder as ZstdEncoder;

use crate::quality::{QualityEvent, QualityReporter};
use crate::symbol::Symbol;

/// One received frame on its way to the writer thread.
pub struct WriteRecord {
    pub recv_time: Timestamp,
    pub symbol: Symbol,
    pub data: bytes::Bytes,
}

/// Uncompressed bytes accumulated before they are handed to zstd. Matches the
/// compressor's block size so we do not call into it once per websocket frame.
const ZSTD_BATCH_BYTES: usize = 128 * 1024;
/// How long a failed sidecar merge waits before the next attempt. An attempt
/// walks and fsyncs the daily file, so retrying on every record would stall
/// the writer for every symbol.
const MERGE_RETRY_BACKOFF: Duration = Duration::from_secs(5);
const ZSTD_MAGIC: u32 = 0xFD2F_B528;
const SKIPPABLE_MAGIC: u32 = 0x184D_2A50;
const SKIPPABLE_MAGIC_MASK: u32 = 0xFFFF_FFF0;

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

fn try_exclusive_lock(file: &File) -> io::Result<bool> {
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        return Ok(true);
    }
    let err = io::Error::last_os_error();
    if err.kind() == io::ErrorKind::WouldBlock {
        Ok(false)
    } else {
        Err(err)
    }
}

fn lock_exclusively(file: &File, path: &str) -> io::Result<()> {
    if try_exclusive_lock(file)? {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            format!("{path} is locked by another writer"),
        ))
    }
}

/// Whether `path` still names the inode `file` refers to. A lock taken on a
/// file that was renamed away in the meantime guards nothing.
fn names_file(path: &str, file: &File) -> io::Result<bool> {
    let held = file.metadata()?;
    match std::fs::metadata(path) {
        Ok(named) => Ok(named.dev() == held.dev() && named.ino() == held.ino()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

/// Whether `file` ends exactly where a complete zstd frame does.
///
/// A writer killed mid-frame leaves a tail no decoder can step past, so every
/// frame appended after it would be lost to readers. Only frame and block
/// headers are read — one small `pread` per block, no decompression.
fn ends_on_frame_boundary(file: &File) -> io::Result<bool> {
    let len = file.metadata()?.len();
    let read_le = |pos: u64, width: usize| -> io::Result<Option<u32>> {
        if pos.saturating_add(width as u64) > len {
            return Ok(None);
        }
        let mut bytes = [0u8; 4];
        file.read_exact_at(&mut bytes[..width], pos)?;
        Ok(Some(u32::from_le_bytes(bytes)))
    };
    let mut pos = 0u64;
    while pos < len {
        let Some(magic) = read_le(pos, 4)? else {
            return Ok(false);
        };
        pos += 4;
        if magic & SKIPPABLE_MAGIC_MASK == SKIPPABLE_MAGIC {
            let Some(size) = read_le(pos, 4)? else {
                return Ok(false);
            };
            pos += 4 + u64::from(size);
            continue;
        }
        if magic != ZSTD_MAGIC {
            return Ok(false);
        }
        let Some(descriptor) = read_le(pos, 1)? else {
            return Ok(false);
        };
        let single_segment = descriptor & 0x20 != 0;
        let window = u64::from(!single_segment);
        let dictionary: u64 = [0, 1, 2, 4][(descriptor & 0x03) as usize];
        let content_size = match descriptor >> 6 {
            0 => u64::from(single_segment),
            1 => 2,
            2 => 4,
            _ => 8,
        };
        pos += 1 + window + dictionary + content_size;
        loop {
            let Some(block) = read_le(pos, 3)? else {
                return Ok(false);
            };
            pos += 3;
            pos += match (block >> 1) & 0x03 {
                1 => 1,                // RLE: one byte, repeated
                3 => return Ok(false), // reserved block type
                _ => u64::from(block >> 3),
            };
            if block & 1 != 0 {
                break;
            }
        }
        if descriptor & 0x04 != 0 {
            pos += 4; // content checksum
        }
    }
    Ok(pos == len)
}

fn rollback_append(canonical: &mut File, start: u64) -> io::Result<()> {
    canonical.set_len(start)?;
    canonical.seek(SeekFrom::Start(start))?;
    canonical.sync_all()
}

pub struct RotatingFile {
    next_rotation: i64,
    /// UTC `YYYYMMDD` of the file currently open. The daily-file lock is
    /// always probed for this day, never for a record's own timestamp.
    day: String,
    path: String,
    run_id: String,
    /// Set while this process is writing a run-local overlap file because the
    /// canonical daily path is locked by another writer. Cleared after the
    /// switch: the sidecar has been appended into the daily file and removed.
    sidecar_path: Option<String>,
    /// Sidecars sealed at UTC midnight that still belong to this process and
    /// will be appended when that day's daily file lock is free.
    pending_sidecars: Vec<(String, String)>,
    file: Option<ZstdEncoder<'static, File>>,
    buf: bytes::BytesMut,
    /// Set when a rotation could not be finalized, so the already-rotated file
    /// may be missing its zstd footer. Collection continues, but the process
    /// must not report a clean exit.
    degraded: bool,
    /// Set after a sidecar merge failed; no merge is attempted before then.
    merge_retry_at: Option<Instant>,
    quality: QualityReporter,
}

impl RotatingFile {
    fn open_locked(canonical: &str) -> io::Result<Option<File>> {
        let file = File::options()
            .create(true)
            .read(true)
            .append(true)
            .open(canonical)?;
        Ok((try_exclusive_lock(&file)? && names_file(canonical, &file)?).then_some(file))
    }

    fn try_lock_canonical_file(path: &str, date_str: &str) -> Result<Option<File>, io::Error> {
        let canonical = format!("{path}_{date_str}.zst");
        let Some(primary) = Self::open_locked(&canonical)? else {
            return Ok(None);
        };
        if ends_on_frame_boundary(&primary)? {
            return Ok(Some(primary));
        }
        // The last writer died mid-frame. Appending after that would make every
        // later frame undecodable, so set the damaged file aside — readable up
        // to where it stops — and start the day again in a fresh file. The old
        // inode stays locked until the fresh one is.
        let salvage = format!(
            "{path}_{date_str}_unterminated-{}.zst",
            Timestamp::now().as_nanosecond()
        );
        std::fs::rename(&canonical, &salvage)?;
        warn!(
            path = %canonical,
            %salvage,
            "daily file ends mid-frame; moved it aside instead of appending to it"
        );
        Self::open_locked(&canonical)
    }

    fn try_lock_canonical(
        path: &str,
        date_str: &str,
    ) -> Result<Option<ZstdEncoder<'static, File>>, io::Error> {
        match Self::try_lock_canonical_file(path, date_str)? {
            Some(file) => Ok(Some(ZstdEncoder::new(file, 1)?)),
            None => Ok(None),
        }
    }

    fn append_sidecar(canonical: &mut File, sidecar: &str) -> io::Result<()> {
        let start = canonical.metadata()?.len();
        let copied = (|| {
            let mut src = File::open(sidecar)?;
            io::copy(&mut src, canonical)?;
            canonical.sync_all()
        })();
        if let Err(error) = copied {
            return match rollback_append(canonical, start) {
                Ok(()) => Err(error),
                Err(rollback) => Err(io::Error::new(
                    error.kind(),
                    format!("{error}; also failed to roll back partial append: {rollback}"),
                )),
            };
        }
        Ok(())
    }

    fn open_sidecar(
        path: &str,
        date_str: &str,
        run_id: &str,
    ) -> Result<(ZstdEncoder<'static, File>, String), io::Error> {
        let sidecar = format!("{path}_{date_str}_{run_id}.zst");
        let file = File::options()
            .create_new(true)
            .write(true)
            .open(&sidecar)?;
        lock_exclusively(&file, &sidecar)?;
        warn!(
            path = %sidecar,
            "daily file busy; writing a run-local sidecar until the incumbent releases it"
        );
        Ok((ZstdEncoder::new(file, 1)?, sidecar))
    }

    fn open_daily(
        path: &str,
        date_str: &str,
        run_id: &str,
    ) -> Result<(ZstdEncoder<'static, File>, Option<String>), io::Error> {
        if let Some(encoder) = Self::try_lock_canonical(path, date_str)? {
            return Ok((encoder, None));
        }
        let (encoder, sidecar) = Self::open_sidecar(path, date_str, run_id)?;
        Ok((encoder, Some(sidecar)))
    }

    fn create(
        timestamp: Timestamp,
        path: &str,
        run_id: &str,
    ) -> Result<(ZstdEncoder<'static, File>, i64, String, Option<String>), io::Error> {
        let zoned = timestamp.to_zoned(jiff::tz::TimeZone::UTC);
        let day = zoned.date().strftime("%Y%m%d").to_string();
        let (encoder, sidecar_path) = Self::open_daily(path, &day, run_id)?;

        let next_rotation = zoned
            .date()
            .tomorrow()
            .map_err(io::Error::other)?
            .at(0, 0, 0, 0)
            .to_zoned(jiff::tz::TimeZone::UTC)
            .map_err(io::Error::other)?
            .timestamp()
            .as_nanosecond();

        Ok((encoder, next_rotation as i64, day, sidecar_path))
    }

    pub fn new(
        timestamp: Timestamp,
        path: String,
        run_id: String,
        quality: QualityReporter,
    ) -> Result<Self, io::Error> {
        let (file, next_rotation, day, sidecar_path) = Self::create(timestamp, &path, &run_id)?;
        Ok(Self {
            next_rotation,
            day,
            file: Some(file),
            path,
            run_id,
            sidecar_path,
            pending_sidecars: Vec::new(),
            buf: bytes::BytesMut::with_capacity(ZSTD_BATCH_BYTES),
            degraded: false,
            merge_retry_at: None,
            quality,
        })
    }

    fn mark_degraded(&mut self, target: String, error: &io::Error) {
        self.degraded = true;
        self.quality.report(QualityEvent::StorageDegraded {
            at_ns: QualityEvent::now_ns(),
            target,
            error: error.to_string(),
        });
    }

    fn finalize_or_degrade(&mut self) -> bool {
        match self.finalize() {
            Ok(()) => true,
            Err(error) => {
                error!(
                    path = %self.path,
                    %error,
                    "failed to finalize file on rotation; continuing with the new file"
                );
                self.mark_degraded(self.path.clone(), &error);
                false
            }
        }
    }

    fn resume_sidecar_encoder(sidecar: &str) -> io::Result<ZstdEncoder<'static, File>> {
        let file = File::options().append(true).open(sidecar)?;
        lock_exclusively(&file, sidecar)?;
        ZstdEncoder::new(file, 1)
    }

    fn append_and_remove_sidecar(canonical: &mut File, sidecar: &str) -> io::Result<()> {
        Self::append_sidecar(canonical, sidecar)?;
        match std::fs::remove_file(sidecar) {
            Ok(()) => info!(
                sidecar,
                "switch complete; merged and removed overlap sidecar"
            ),
            Err(error) => warn!(
                sidecar,
                %error,
                "switch complete; merged overlap sidecar but failed to remove it"
            ),
        }
        Ok(())
    }

    fn try_merge_owned_sidecar(path: &str, date_str: &str, sidecar: &str) -> io::Result<bool> {
        let Some(mut canonical) = Self::try_lock_canonical_file(path, date_str)? else {
            return Ok(false);
        };
        Self::append_and_remove_sidecar(&mut canonical, sidecar)?;
        Ok(true)
    }

    fn merge_pending_sidecars(&mut self) -> io::Result<()> {
        let pending = std::mem::take(&mut self.pending_sidecars);
        for (date_str, sidecar) in pending {
            match Self::try_merge_owned_sidecar(&self.path, &date_str, &sidecar) {
                Ok(true) => {}
                Ok(false) => self.pending_sidecars.push((date_str, sidecar)),
                Err(error) => {
                    error!(
                        path = %sidecar,
                        %error,
                        "failed to append overlap sidecar after switch"
                    );
                    self.mark_degraded(sidecar.clone(), &error);
                    self.merge_retry_at = Some(Instant::now() + MERGE_RETRY_BACKOFF);
                    self.pending_sidecars.push((date_str, sidecar));
                }
            }
        }
        Ok(())
    }

    /// After a failed append of a *finished* sidecar: keep writing into it so
    /// a later attempt can merge it, or queue it if it cannot be reopened.
    fn resume_or_abandon_sidecar(
        &mut self,
        sidecar: String,
        date_str: &str,
        keep_writing: bool,
    ) -> io::Result<()> {
        match Self::resume_sidecar_encoder(&sidecar) {
            Ok(encoder) => {
                self.file = Some(encoder);
                Ok(())
            }
            Err(resume_error) => {
                error!(
                    path = %sidecar,
                    %resume_error,
                    "failed to resume overlap sidecar after a failed merge"
                );
                let Some(mut canonical) = Self::try_lock_canonical_file(&self.path, date_str)?
                else {
                    if keep_writing {
                        return Err(resume_error);
                    }
                    self.pending_sidecars.push((date_str.to_string(), sidecar));
                    self.sidecar_path = None;
                    return Ok(());
                };
                if let Err(error) = Self::append_and_remove_sidecar(&mut canonical, &sidecar) {
                    self.mark_degraded(sidecar.clone(), &error);
                    if keep_writing {
                        return Err(error);
                    }
                    self.pending_sidecars.push((date_str.to_string(), sidecar));
                    self.sidecar_path = None;
                    return Ok(());
                }
                self.sidecar_path = None;
                if keep_writing {
                    self.file = Some(ZstdEncoder::new(canonical, 1)?);
                }
                Ok(())
            }
        }
    }

    /// After the incumbent releases the daily file lock, append this process's
    /// sidecar and delete it. That is the only time the sidecar is removed.
    ///
    /// `keep_writing`: wrap the canonical file so later records of this UTC day
    /// can follow. Pass false when rotating or closing — otherwise we would
    /// finish an empty extra frame.
    ///
    /// The lock is probed for the open file's day (`self.day`), never for a
    /// record's timestamp: a late pre-midnight record must not switch today's
    /// sidecar into yesterday's file.
    fn reclaim_canonical_if_free(&mut self, keep_writing: bool) -> io::Result<()> {
        let Some(sidecar) = self.sidecar_path.as_deref() else {
            return Ok(());
        };
        let Some(mut canonical) = Self::try_lock_canonical_file(&self.path, &self.day)? else {
            return Ok(());
        };
        let sidecar = sidecar.to_owned();
        if let Err(error) = self.finalize() {
            error!(
                path = %sidecar,
                %error,
                "failed to finalize handover sidecar before switching to the daily file"
            );
            self.mark_degraded(sidecar.clone(), &error);
            // An unfinished frame must never be appended: everything after it
            // in the daily file would be undecodable. Leave the sidecar on disk
            // as it is and carry on in the daily file, whose lock is held here.
            warn!(path = %sidecar, "leaving unfinished overlap sidecar on disk");
            self.sidecar_path = None;
            // Drop what is left of the sidecar encoder (and its lock); records
            // still buffered go to the daily file instead.
            self.file = None;
            if keep_writing || !self.buf.is_empty() {
                self.file = Some(ZstdEncoder::new(canonical, 1)?);
            }
            return Ok(());
        }
        if let Err(error) = Self::append_and_remove_sidecar(&mut canonical, &sidecar) {
            error!(
                path = %sidecar,
                %error,
                "failed to append overlap sidecar after switch"
            );
            self.mark_degraded(sidecar.clone(), &error);
            self.merge_retry_at = Some(Instant::now() + MERGE_RETRY_BACKOFF);
            drop(canonical);
            let day = self.day.clone();
            return self.resume_or_abandon_sidecar(sidecar, &day, keep_writing);
        }
        self.sidecar_path = None;
        if keep_writing {
            self.file = Some(ZstdEncoder::new(canonical, 1)?);
        }
        Ok(())
    }

    fn flush_pending(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| io::Error::other(format!("{} has no open file", self.path)))?;
        file.write_all(&self.buf)?;
        self.buf.clear();
        Ok(())
    }

    pub fn finalize(&mut self) -> io::Result<()> {
        self.flush_pending()?;
        let Some(encoder) = self.file.take() else {
            return Ok(());
        };

        let file = encoder.finish().map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("failed to finish zstd stream {}: {error}", self.path),
            )
        })?;

        // A failed fsync may already have dropped the dirty pages and marked
        // them clean, so a retry could succeed with the data lost.
        file.sync_all().map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("failed to sync {}: {error}", self.path),
            )
        })?;
        let _ = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
        Ok(())
    }

    pub fn write(&mut self, timestamp: Timestamp, data: bytes::Bytes) -> Result<(), io::Error> {
        let ts_nanos = timestamp.as_nanosecond();
        if ts_nanos >= self.next_rotation as i128 {
            self.reclaim_canonical_if_free(false)?;
            let finalized = self.finalize_or_degrade();
            if let Some(sidecar) = self.sidecar_path.take() {
                if finalized {
                    warn!(
                        path = %sidecar,
                        "UTC date changed before switch; keeping the overlap sidecar until the daily file is free"
                    );
                    self.pending_sidecars.push((self.day.clone(), sidecar));
                } else {
                    warn!(
                        path = %sidecar,
                        "UTC date changed before switch; leaving incomplete overlap sidecar on disk"
                    );
                }
            }
            let (new_file, next_rotation, day, sidecar_path) =
                Self::create(timestamp, &self.path, &self.run_id)?;
            self.buf.clear();
            self.file = Some(new_file);
            self.next_rotation = next_rotation;
            self.day = day;
            self.sidecar_path = sidecar_path;
            info!(%self.path, "date changed, file rotated");
        }

        if self
            .merge_retry_at
            .is_none_or(|retry_at| Instant::now() >= retry_at)
        {
            self.merge_retry_at = None;
            self.reclaim_canonical_if_free(true)?;
            self.merge_pending_sidecars()?;
        }

        if self.file.is_none() {
            return Err(io::Error::other(format!("{} has no open file", self.path)));
        }

        let mut itoa_buf = itoa::Buffer::new();
        self.buf
            .put_slice(itoa_buf.format(ts_nanos as i64).as_bytes());
        self.buf.put_u8(b' ');
        self.buf.put(data);
        self.buf.put_u8(b'\n');

        if self.buf.len() >= ZSTD_BATCH_BYTES {
            self.flush_pending()?;
        }
        Ok(())
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

    /// If the switch has finished, any sidecar this process owns is appended
    /// and deleted first. If the incumbent still holds the daily file, the
    /// sidecar is sealed and left on disk.
    pub fn close(&mut self) -> Result<(), anyhow::Error> {
        let mut result = Ok(());
        for (symbol, file) in &mut self.files {
            if let Err(error) = file.reclaim_canonical_if_free(false) {
                error!(
                    symbol = %symbol,
                    %error,
                    "failed to merge overlap sidecar after switch"
                );
                if result.is_ok() {
                    result = Err(error.into());
                }
            }
            if let Err(error) = file.merge_pending_sidecars() {
                error!(
                    symbol = %symbol,
                    %error,
                    "failed to merge overlap sidecar after switch"
                );
                if result.is_ok() {
                    result = Err(error.into());
                }
            }
            if file.sidecar_path.is_some() || !file.pending_sidecars.is_empty() {
                info!(
                    symbol = %symbol,
                    "closing before switch; leaving overlap sidecar on disk"
                );
            }
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

    fn zstd_frame_count(bytes: &[u8]) -> usize {
        const MAGIC: &[u8; 4] = &[0x28, 0xB5, 0x2F, 0xFD];
        bytes.windows(4).filter(|window| *window == MAGIC).count()
    }

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

        for name in &written {
            let bytes = std::fs::read(dir.join(name)).unwrap();
            assert!(zstd::decode_all(bytes.as_slice()).is_ok(), "{name}");
            let stem = name.trim_end_matches(".zst");
            let date = stem.rsplit_once('_').map(|(_, date)| date).unwrap();
            assert!(
                date.len() == 8 && date.bytes().all(|b| b.is_ascii_digit()),
                "expected daily symbol_YYYYMMDD.zst, got {name}"
            );
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
    fn same_day_writes_share_one_file() {
        let dir = std::env::temp_dir().join(format!(
            "collector-daily-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut writer = Writer::new(
            dir.to_str().unwrap(),
            "daily-run",
            QualityReporter::disabled(),
        );
        let first = Timestamp::from_nanosecond(1_800_000_000_000_000_000).unwrap();
        let later_same_day = first
            .checked_add(jiff::Span::new().try_hours(3).unwrap())
            .unwrap();
        for recv_time in [first, later_same_day] {
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
        assert_eq!(paths.len(), 1, "{paths:?}");
        assert!(zstd::decode_all(std::fs::read(&paths[0]).unwrap().as_slice()).is_ok());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn date_boundary_opens_a_new_daily_file() {
        let dir = std::env::temp_dir().join(format!(
            "collector-day-boundary-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut writer = Writer::new(
            dir.to_str().unwrap(),
            "boundary-run",
            QualityReporter::disabled(),
        );
        let before = Timestamp::from_nanosecond(1_800_000_000_000_000_000).unwrap();
        let after = Timestamp::from_nanosecond(1_800_086_400_000_000_000).unwrap();
        for recv_time in [before, after] {
            writer
                .write(
                    recv_time,
                    Symbol::from("btcusdt"),
                    bytes::Bytes::from_static(b"{}"),
                )
                .unwrap();
        }
        writer.close().unwrap();

        let mut names: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        assert_eq!(names.len(), 2, "{names:?}");
        assert_ne!(names[0], names[1]);
        for name in &names {
            assert!(zstd::decode_all(std::fs::read(dir.join(name)).unwrap().as_slice()).is_ok());
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn busy_daily_file_falls_back_to_run_sidecar() {
        let dir = std::env::temp_dir().join(format!(
            "collector-sidecar-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let now = Timestamp::now();
        let mut incumbent = Writer::new(
            dir.to_str().unwrap(),
            "incumbent",
            QualityReporter::disabled(),
        );
        incumbent
            .write(
                now,
                Symbol::from("btcusdt"),
                bytes::Bytes::from_static(b"{\"n\":1}"),
            )
            .unwrap();

        let mut challenger = Writer::new(
            dir.to_str().unwrap(),
            "challenger",
            QualityReporter::disabled(),
        );
        challenger
            .write(
                now,
                Symbol::from("btcusdt"),
                bytes::Bytes::from_static(b"{\"n\":2}"),
            )
            .unwrap();
        assert!(
            challenger.files[&Symbol::from("btcusdt")]
                .sidecar_path
                .is_some(),
            "challenger must use a sidecar while the daily file is locked"
        );

        incumbent.close().unwrap();
        challenger
            .write(
                now,
                Symbol::from("btcusdt"),
                bytes::Bytes::from_static(b"{\"n\":3}"),
            )
            .unwrap();
        assert!(
            challenger.files[&Symbol::from("btcusdt")]
                .sidecar_path
                .is_none(),
            "after the incumbent exits, writes must reclaim the canonical daily file"
        );
        challenger.close().unwrap();

        let mut names: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        assert!(
            names.iter().all(|n| !n.contains("_challenger.zst")),
            "overlap sidecar must be removed after a successful handover: {names:?}"
        );
        assert_eq!(names.len(), 1, "{names:?}");
        assert!(
            names[0].starts_with("btcusdt_") && names[0].ends_with(".zst"),
            "canonical daily file must exist: {names:?}"
        );
        let decoded =
            zstd::decode_all(std::fs::read(dir.join(&names[0])).unwrap().as_slice()).unwrap();
        let text = String::from_utf8_lossy(&decoded);
        assert!(text.contains("{\"n\":1}"), "{text}");
        assert!(
            text.contains("{\"n\":2}"),
            "sidecar overlap missing from daily file: {text}"
        );
        assert!(text.contains("{\"n\":3}"), "{text}");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn close_merges_sidecar_without_another_write() {
        let dir = std::env::temp_dir().join(format!(
            "collector-close-merge-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let now = Timestamp::now();
        let mut incumbent = Writer::new(
            dir.to_str().unwrap(),
            "incumbent",
            QualityReporter::disabled(),
        );
        incumbent
            .write(
                now,
                Symbol::from("btcusdt"),
                bytes::Bytes::from_static(b"{\"n\":1}"),
            )
            .unwrap();
        let mut challenger = Writer::new(
            dir.to_str().unwrap(),
            "challenger",
            QualityReporter::disabled(),
        );
        challenger
            .write(
                now,
                Symbol::from("btcusdt"),
                bytes::Bytes::from_static(b"{\"n\":2}"),
            )
            .unwrap();
        incumbent.close().unwrap();
        assert!(
            std::fs::read_dir(&dir).unwrap().any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .contains("_challenger.zst")
            }),
            "incumbent close must not steal a live overlap sidecar"
        );
        challenger.close().unwrap();

        let mut names: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        assert!(
            names.iter().all(|n| !n.contains("_challenger.zst")),
            "close must merge and remove the overlap sidecar: {names:?}"
        );
        assert_eq!(names.len(), 1, "{names:?}");
        let merged_bytes = std::fs::read(dir.join(&names[0])).unwrap();
        assert_eq!(
            zstd_frame_count(&merged_bytes),
            2,
            "close should not append an empty extra zstd frame"
        );
        let decoded = zstd::decode_all(merged_bytes.as_slice()).unwrap();
        let text = String::from_utf8_lossy(&decoded);
        assert!(text.contains("{\"n\":1}"), "{text}");
        assert!(
            text.contains("{\"n\":2}"),
            "sidecar overlap missing from daily file: {text}"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn close_leaves_sidecar_when_daily_file_is_still_locked() {
        let dir = std::env::temp_dir().join(format!(
            "collector-close-leave-sidecar-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let now = Timestamp::now();
        let mut incumbent = Writer::new(
            dir.to_str().unwrap(),
            "incumbent",
            QualityReporter::disabled(),
        );
        incumbent
            .write(
                now,
                Symbol::from("btcusdt"),
                bytes::Bytes::from_static(b"{\"n\":1}"),
            )
            .unwrap();
        let mut challenger = Writer::new(
            dir.to_str().unwrap(),
            "challenger",
            QualityReporter::disabled(),
        );
        challenger
            .write(
                now,
                Symbol::from("btcusdt"),
                bytes::Bytes::from_static(b"{\"n\":2}"),
            )
            .unwrap();
        challenger.close().unwrap();
        assert!(
            std::fs::read_dir(&dir).unwrap().any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .contains("_challenger.zst")
            }),
            "challenger must leave its sidecar when the incumbent still holds the daily file"
        );
        incumbent.close().unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn midnight_reclaim_merges_sidecar_after_incumbent_exits() {
        let dir = std::env::temp_dir().join(format!(
            "collector-midnight-merge-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let before = Timestamp::from_nanosecond(1_800_000_000_000_000_000).unwrap();
        let after = Timestamp::from_nanosecond(1_800_086_400_000_000_000).unwrap();
        let mut incumbent = Writer::new(
            dir.to_str().unwrap(),
            "incumbent",
            QualityReporter::disabled(),
        );
        incumbent
            .write(
                before,
                Symbol::from("btcusdt"),
                bytes::Bytes::from_static(b"{\"n\":1}"),
            )
            .unwrap();
        let mut challenger = Writer::new(
            dir.to_str().unwrap(),
            "challenger",
            QualityReporter::disabled(),
        );
        challenger
            .write(
                before,
                Symbol::from("btcusdt"),
                bytes::Bytes::from_static(b"{\"n\":2}"),
            )
            .unwrap();
        incumbent.close().unwrap();
        challenger
            .write(
                after,
                Symbol::from("btcusdt"),
                bytes::Bytes::from_static(b"{\"n\":3}"),
            )
            .unwrap();
        challenger.close().unwrap();

        let mut names: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        assert!(
            names.iter().all(|n| !n.contains("_challenger.zst")),
            "midnight rotation must merge yesterday's sidecar: {names:?}"
        );
        assert_eq!(names.len(), 2, "{names:?}");
        let day_one_bytes = std::fs::read(dir.join(&names[0])).unwrap();
        assert_eq!(
            zstd_frame_count(&day_one_bytes),
            2,
            "yesterday should be incumbent frame + sidecar frame, not an extra empty frame"
        );
        let day_one = String::from_utf8_lossy(&zstd::decode_all(day_one_bytes.as_slice()).unwrap())
            .into_owned();
        let day_two = String::from_utf8_lossy(
            &zstd::decode_all(std::fs::read(dir.join(&names[1])).unwrap().as_slice()).unwrap(),
        )
        .into_owned();
        assert!(day_one.contains("{\"n\":1}"), "{day_one}");
        assert!(
            day_one.contains("{\"n\":2}"),
            "sidecar overlap missing: {day_one}"
        );
        assert!(day_two.contains("{\"n\":3}"), "{day_two}");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn midnight_keeps_sidecar_while_incumbent_lock_is_held() {
        let dir = std::env::temp_dir().join(format!(
            "collector-midnight-lock-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let before = Timestamp::from_nanosecond(1_800_000_000_000_000_000).unwrap();
        let after = Timestamp::from_nanosecond(1_800_086_400_000_000_000).unwrap();
        let mut incumbent = Writer::new(
            dir.to_str().unwrap(),
            "incumbent",
            QualityReporter::disabled(),
        );
        incumbent
            .write(
                before,
                Symbol::from("btcusdt"),
                bytes::Bytes::from_static(b"{\"n\":1}"),
            )
            .unwrap();
        let mut challenger = Writer::new(
            dir.to_str().unwrap(),
            "challenger",
            QualityReporter::disabled(),
        );
        challenger
            .write(
                before,
                Symbol::from("btcusdt"),
                bytes::Bytes::from_static(b"{\"n\":2}"),
            )
            .unwrap();
        challenger
            .write(
                after,
                Symbol::from("btcusdt"),
                bytes::Bytes::from_static(b"{\"n\":3}"),
            )
            .unwrap();
        let btc = Symbol::from("btcusdt");
        assert!(
            !challenger.files[&btc].pending_sidecars.is_empty()
                || std::fs::read_dir(&dir).unwrap().any(|entry| entry
                    .unwrap()
                    .file_name()
                    .into_string()
                    .unwrap()
                    .contains("_challenger.zst")),
            "overlap sidecar must remain until the incumbent releases yesterday's lock"
        );

        incumbent.close().unwrap();
        challenger.close().unwrap();

        let mut names: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        assert!(
            names.iter().all(|n| !n.contains("_challenger.zst")),
            "close must merge the leftover midnight sidecar: {names:?}"
        );
        assert_eq!(names.len(), 2, "{names:?}");
        let day_one = String::from_utf8_lossy(
            &zstd::decode_all(std::fs::read(dir.join(&names[0])).unwrap().as_slice()).unwrap(),
        )
        .into_owned();
        let day_two = String::from_utf8_lossy(
            &zstd::decode_all(std::fs::read(dir.join(&names[1])).unwrap().as_slice()).unwrap(),
        )
        .into_owned();
        assert!(day_one.contains("{\"n\":1}"), "{day_one}");
        assert!(
            day_one.contains("{\"n\":2}"),
            "sidecar overlap missing: {day_one}"
        );
        assert!(day_two.contains("{\"n\":3}"), "{day_two}");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn duplicate_run_id_cannot_share_a_sidecar() {
        let dir = std::env::temp_dir().join(format!(
            "collector-sidecar-exclusive-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let now = Timestamp::now();
        let mut incumbent = Writer::new(
            dir.to_str().unwrap(),
            "incumbent",
            QualityReporter::disabled(),
        );
        incumbent
            .write(
                now,
                Symbol::from("btcusdt"),
                bytes::Bytes::from_static(b"{\"n\":1}"),
            )
            .unwrap();
        let mut first = Writer::new(
            dir.to_str().unwrap(),
            "same-run",
            QualityReporter::disabled(),
        );
        first
            .write(
                now,
                Symbol::from("btcusdt"),
                bytes::Bytes::from_static(b"{\"n\":2}"),
            )
            .unwrap();
        let mut second = Writer::new(
            dir.to_str().unwrap(),
            "same-run",
            QualityReporter::disabled(),
        );
        let err = second
            .write(
                now,
                Symbol::from("btcusdt"),
                bytes::Bytes::from_static(b"{\"n\":3}"),
            )
            .unwrap_err();
        assert!(
            err.to_string().contains("already exists")
                || err
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::AlreadyExists),
            "second writer with the same run id must not share the sidecar: {err}"
        );
        first.close().ok();
        incumbent.close().ok();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    fn test_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "collector-{tag}-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn put(writer: &mut Writer, recv_time: Timestamp, data: &'static [u8]) {
        writer
            .write(
                recv_time,
                Symbol::from("btcusdt"),
                bytes::Bytes::from_static(data),
            )
            .unwrap();
    }

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }

    #[test]
    fn frame_walk_accepts_whole_frames_and_rejects_a_truncated_tail() {
        let dir = test_dir("frame-walk");
        let path = dir.join("walk.zst");
        let check = |bytes: &[u8]| {
            std::fs::write(&path, bytes).unwrap();
            ends_on_frame_boundary(&File::open(&path).unwrap()).unwrap()
        };
        let mut streamed = ZstdEncoder::new(Vec::new(), 1).unwrap();
        streamed.write_all(b"streamed, no content size").unwrap();
        let streamed = streamed.finish().unwrap();
        let whole = [
            zstd::encode_all(&b"single segment"[..], 1).unwrap(),
            zstd::encode_all(&vec![0u8; 300_000][..], 1).unwrap(),
            streamed,
        ]
        .concat();

        assert!(check(b""));
        assert!(check(&whole));
        assert!(!check(&whole[..whole.len() - 1]));
        let mut garbage = whole.clone();
        garbage.extend_from_slice(b"not zstd");
        assert!(!check(&garbage));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn unterminated_daily_file_is_set_aside_not_appended_to() {
        let dir = test_dir("unterminated");
        let now = Timestamp::from_nanosecond(1_800_000_000_000_000_000).unwrap();
        let canonical = dir.join("btcusdt_20270115.zst");
        let crashed = zstd::encode_all(&b"before-crash"[..], 1).unwrap();
        std::fs::write(&canonical, &crashed[..crashed.len() - 2]).unwrap();

        let mut writer = Writer::new(
            dir.to_str().unwrap(),
            "restart",
            QualityReporter::disabled(),
        );
        put(&mut writer, now, b"after-restart");
        writer.close().unwrap();

        let fresh = zstd::decode_all(std::fs::read(&canonical).unwrap().as_slice()).unwrap();
        assert!(contains(&fresh, b"after-restart"));
        let names: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        let salvage = names
            .iter()
            .find(|name| name.starts_with("btcusdt_20270115_unterminated-"))
            .unwrap_or_else(|| panic!("damaged file must be kept aside: {names:?}"));
        assert_eq!(
            std::fs::read(dir.join(salvage)).unwrap(),
            &crashed[..crashed.len() - 2]
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn late_record_does_not_switch_into_yesterdays_file() {
        let dir = test_dir("late-record");
        let before = Timestamp::from_nanosecond(1_800_000_000_000_000_000).unwrap();
        let after = Timestamp::from_nanosecond(1_800_086_400_000_000_000).unwrap();
        let mut incumbent = Writer::new(
            dir.to_str().unwrap(),
            "incumbent",
            QualityReporter::disabled(),
        );
        put(&mut incumbent, before, b"incumbent-day-one");
        put(&mut incumbent, after, b"incumbent-day-two");
        let mut challenger = Writer::new(
            dir.to_str().unwrap(),
            "challenger",
            QualityReporter::disabled(),
        );
        put(&mut challenger, after, b"sidecar-day-two");
        incumbent.close().unwrap();
        // Stamped before midnight, delivered after the incumbent let go.
        put(&mut challenger, before, b"late");
        challenger.close().unwrap();

        let day_one = zstd::decode_all(
            std::fs::read(dir.join("btcusdt_20270115.zst"))
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        let day_two = zstd::decode_all(
            std::fs::read(dir.join("btcusdt_20270116.zst"))
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        assert!(!contains(&day_one, b"sidecar-day-two"));
        assert!(!contains(&day_one, b"late"));
        for payload in [&b"incumbent-day-two"[..], b"sidecar-day-two", b"late"] {
            assert!(contains(&day_two, payload), "day two missing {payload:?}");
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// fsync(2) on /dev/null fails with EINVAL, standing in for a disk error.
    #[cfg(target_os = "linux")]
    #[test]
    fn failed_fsync_is_reported_and_fails_close() {
        let dir = test_dir("fsync");
        let (quality, events) = QualityReporter::test_channel();
        let mut writer = Writer::new(dir.to_str().unwrap(), "fsync", quality);
        put(
            &mut writer,
            Timestamp::from_nanosecond(1_800_000_000_000_000_000).unwrap(),
            b"a",
        );
        let dev_null = File::options().write(true).open("/dev/null").unwrap();
        writer.files.get_mut("btcusdt").unwrap().file =
            Some(ZstdEncoder::new(dev_null, 1).unwrap());
        put(
            &mut writer,
            Timestamp::from_nanosecond(1_800_086_400_000_000_000).unwrap(),
            b"b",
        );
        assert!(
            matches!(events.try_recv(), Ok(QualityEvent::StorageDegraded { error, .. }) if error.contains("failed to sync")),
            "a failed fsync must be reported"
        );
        assert!(
            writer.close().is_err(),
            "close must not report a clean exit"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn rollback_append_restores_prior_length() {
        use std::io::Write;
        let dir = std::env::temp_dir().join(format!(
            "collector-rollback-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("dst");
        let mut file = File::options()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .unwrap();
        file.write_all(b"keep").unwrap();
        file.sync_all().unwrap();
        let start = file.metadata().unwrap().len();
        file.write_all(b"junk").unwrap();
        rollback_append(&mut file, start).unwrap();
        drop(file);
        assert_eq!(std::fs::read(&path).unwrap(), b"keep");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
