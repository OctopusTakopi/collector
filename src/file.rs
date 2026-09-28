use std::{
    borrow::Cow,
    collections::HashMap,
    fmt::{self, Write as _},
    fs::{self, File, TryLockError},
    io::{self, Seek, SeekFrom, Write},
    mem,
    os::unix::fs::{FileExt, MetadataExt},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use bytes::{BufMut, Bytes, BytesMut};
use jiff::{Timestamp, civil::Date, tz::TimeZone};
use tracing::{error, info, warn};

use crate::quality::{QualityEvent, QualityReporter};
use crate::symbol::Symbol;

/// One received frame on its way to the writer thread.
pub struct WriteRecord {
    pub recv_time: Timestamp,
    pub symbol: Symbol,
    pub data: bytes::Bytes,
}

type ZstdEncoder = zstd::stream::write::Encoder<'static, File>;

const ZSTD_LEVEL: i32 = 1;
/// Uncompressed bytes batched before they are handed to zstd: one compressor
/// block, so zstd is not called once per websocket frame.
const ZSTD_BATCH_BYTES: usize = 128 * 1024;
/// A failed sidecar merge is not retried before this: an attempt walks and
/// fsyncs the daily file, so retrying on every record would stall the writer
/// for every symbol.
const MERGE_RETRY_BACKOFF: Duration = Duration::from_secs(5);
const ZSTD_MAGIC: u32 = 0xFD2F_B528;
const SKIPPABLE_MAGIC: u32 = 0x184D_2A50;
const SKIPPABLE_MAGIC_MASK: u32 = 0xFFFF_FFF0;

/// Bytes kept as-is in a filename: only path-hostile bytes are escaped, so
/// the exchange's identifier stays readable on disk. Hyperliquid names 315 of
/// its 316 spot pairs `@1` … `@315`; `+`/`=` occur in dated contracts. All are
/// legal filename characters on Linux, macOS and Windows.
fn is_safe_in_filename(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'@' | b'+' | b'=')
}

/// Percent-encode a symbol into a filename component (Hyperliquid's
/// `PURR/USDC` is not one).
///
/// The encoding is injective: two symbols sharing a file would interleave the
/// frames of two zstd encoders and render it undecodable. `%` is itself
/// unsafe, so it is always escaped and no two symbols collide.
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

/// `<prefix>_<YYYYMMDD><suffix>.zst`, where `prefix` is
/// `<dir>/<encoded symbol>`.
fn day_file(prefix: &Path, date: Date, suffix: fmt::Arguments<'_>) -> PathBuf {
    let mut name = prefix.as_os_str().to_owned();
    write!(name, "_{}{suffix}.zst", date.strftime("%Y%m%d"))
        .expect("formatting into an OsString cannot fail");
    PathBuf::from(name)
}

/// A UTC calendar day and the instant the next one starts.
#[derive(Clone, Copy)]
struct Day {
    date: Date,
    end: Timestamp,
}

impl Day {
    fn containing(timestamp: Timestamp) -> io::Result<Self> {
        let date = TimeZone::UTC.to_datetime(timestamp).date();
        let end = date
            .tomorrow()
            .and_then(|next| next.to_zoned(TimeZone::UTC))
            .map_err(io::Error::other)?
            .timestamp();
        Ok(Self { date, end })
    }
}

/// Take the exclusive lock without blocking; `false` if another writer holds
/// it. On Linux this is `flock(LOCK_EX | LOCK_NB)`.
fn try_exclusive_lock(file: &File) -> io::Result<bool> {
    match file.try_lock() {
        Ok(()) => Ok(true),
        Err(TryLockError::WouldBlock) => Ok(false),
        Err(TryLockError::Error(error)) => Err(error),
    }
}

fn lock_exclusively(file: &File, path: &Path) -> io::Result<()> {
    if try_exclusive_lock(file)? {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            format!("{} is locked by another writer", path.display()),
        ))
    }
}

/// Whether `path` still names the inode `file` refers to. A lock taken on a
/// file that was renamed away in the meantime guards nothing.
fn names_file(path: &Path, file: &File) -> io::Result<bool> {
    let held = file.metadata()?;
    match fs::metadata(path) {
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

fn rollback_append(daily: &mut File, start: u64) -> io::Result<()> {
    daily.set_len(start)?;
    daily.seek(SeekFrom::Start(start))?;
    daily.sync_all()
}

/// Open `path` for appending and lock it, unless another writer holds it or
/// it was renamed away before the lock was taken.
fn open_locked(path: &Path) -> io::Result<Option<File>> {
    let file = File::options()
        .create(true)
        .read(true)
        .append(true)
        .open(path)?;
    Ok((try_exclusive_lock(&file)? && names_file(path, &file)?).then_some(file))
}

/// Lock the day's file for appending; `None` while another writer holds it.
fn try_lock_daily(prefix: &Path, date: Date) -> io::Result<Option<File>> {
    let daily = day_file(prefix, date, format_args!(""));
    let Some(file) = open_locked(&daily)? else {
        return Ok(None);
    };
    if ends_on_frame_boundary(&file)? {
        return Ok(Some(file));
    }
    // The last writer died mid-frame and appending after that would make
    // every later frame undecodable: set the file aside, readable up to where
    // it stops, and start the day again in a fresh one.
    let salvage = day_file(
        prefix,
        date,
        format_args!("_unterminated-{}", Timestamp::now().as_nanosecond()),
    );
    fs::rename(&daily, &salvage)?;
    warn!(
        path = %daily.display(),
        salvage = %salvage.display(),
        "daily file ends mid-frame; moved it aside instead of appending to it"
    );
    let fresh = open_locked(&daily);
    // The old inode stays locked until the fresh one is.
    drop(file);
    fresh
}

/// Append a finished sidecar to the locked daily file, fsync, and delete the
/// sidecar. A failed append is rolled back, so the daily file never ends
/// mid-frame.
fn merge_sidecar(daily: &mut File, sidecar: &Path) -> io::Result<()> {
    let start = daily.metadata()?.len();
    let appended = File::open(sidecar)
        .and_then(|mut src| io::copy(&mut src, daily))
        .and_then(|_| daily.sync_all());
    if let Err(error) = appended {
        return Err(match rollback_append(daily, start) {
            Ok(()) => error,
            Err(rollback) => io::Error::new(
                error.kind(),
                format!("{error}; also failed to roll back partial append: {rollback}"),
            ),
        });
    }
    match fs::remove_file(sidecar) {
        Ok(()) => info!(
            sidecar = %sidecar.display(),
            "switch complete; merged and removed overlap sidecar"
        ),
        Err(error) => warn!(
            sidecar = %sidecar.display(),
            %error,
            "switch complete; merged overlap sidecar but failed to remove it"
        ),
    }
    Ok(())
}

/// Merge a finished sidecar into its day's file; `false` while another writer
/// still holds that file.
fn merge_into_daily(prefix: &Path, date: Date, sidecar: &Path) -> io::Result<bool> {
    let Some(mut daily) = try_lock_daily(prefix, date)? else {
        return Ok(false);
    };
    merge_sidecar(&mut daily, sidecar)?;
    Ok(true)
}

/// The day's file, or this run's sidecar while another writer holds it.
fn open_sink(prefix: &Path, date: Date, run_id: &str) -> io::Result<Sink> {
    if let Some(daily) = try_lock_daily(prefix, date)? {
        return Ok(Sink::Daily(ZstdEncoder::new(daily, ZSTD_LEVEL)?));
    }
    let path = day_file(prefix, date, format_args!("_{run_id}"));
    let file = File::options().create_new(true).write(true).open(&path)?;
    lock_exclusively(&file, &path)?;
    warn!(
        path = %path.display(),
        "daily file busy; writing a run-local sidecar until the incumbent releases it"
    );
    Ok(Sink::Sidecar {
        encoder: ZstdEncoder::new(file, ZSTD_LEVEL)?,
        path,
    })
}

fn reopen_sidecar(path: &Path) -> io::Result<ZstdEncoder> {
    let file = File::options().append(true).open(path)?;
    lock_exclusively(&file, path)?;
    ZstdEncoder::new(file, ZSTD_LEVEL)
}

fn no_open_file(prefix: &Path) -> io::Error {
    io::Error::other(format!("{} has no open file", prefix.display()))
}

/// Where a symbol's records go.
enum Sink {
    /// The day's file, locked by this process.
    Daily(ZstdEncoder),
    /// Another writer holds the day's file: this run's sidecar, appended to
    /// the daily file and deleted once that is free.
    Sidecar {
        encoder: ZstdEncoder,
        path: PathBuf,
    },
    /// A finished sidecar still waiting to be appended. Nothing can be
    /// written until the switch succeeds.
    SealedSidecar(PathBuf),
    Closed,
}

impl Sink {
    fn encoder(&mut self) -> Option<&mut ZstdEncoder> {
        match self {
            Sink::Daily(encoder) | Sink::Sidecar { encoder, .. } => Some(encoder),
            Sink::SealedSidecar(_) | Sink::Closed => None,
        }
    }

    fn sidecar(&self) -> Option<&Path> {
        match self {
            Sink::Sidecar { path, .. } | Sink::SealedSidecar(path) => Some(path),
            Sink::Daily(_) | Sink::Closed => None,
        }
    }
}

/// What the daily file is for once the sidecar has been appended to it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum AfterSwitch {
    /// More records of the day follow: write them to the daily file.
    Continue,
    /// Rotating or closing: release the daily file. An encoder opened on it
    /// now would finish as an empty extra frame.
    Release,
}

/// A sidecar sealed at UTC midnight, still owned by this run, appended once
/// its day's file is free.
struct PendingSidecar {
    date: Date,
    path: PathBuf,
}

/// One symbol's recording, rotated at UTC midnight.
struct RotatingFile {
    /// `<dir>/<encoded symbol>`, which every file name of the symbol extends.
    prefix: PathBuf,
    run_id: String,
    /// The day of the open file. The daily-file lock is probed for this day,
    /// never for a record's timestamp.
    day: Day,
    sink: Sink,
    pending_sidecars: Vec<PendingSidecar>,
    batch: BytesMut,
    /// A file could not be finalized or a sidecar not merged. Collection
    /// continues, but close must not report a clean exit.
    degraded: bool,
    /// Set after a sidecar merge failed; no switch or merge is attempted
    /// before then.
    merge_retry_at: Option<Instant>,
    quality: QualityReporter,
}

impl RotatingFile {
    fn new(
        timestamp: Timestamp,
        prefix: PathBuf,
        run_id: String,
        quality: QualityReporter,
    ) -> io::Result<Self> {
        let day = Day::containing(timestamp)?;
        let sink = open_sink(&prefix, day.date, &run_id)?;
        Ok(Self {
            prefix,
            run_id,
            day,
            sink,
            pending_sidecars: Vec::new(),
            batch: BytesMut::with_capacity(ZSTD_BATCH_BYTES),
            degraded: false,
            merge_retry_at: None,
            quality,
        })
    }

    fn write(&mut self, timestamp: Timestamp, data: Bytes) -> io::Result<()> {
        if timestamp >= self.day.end {
            self.rotate(timestamp)?;
        }
        if self
            .merge_retry_at
            .is_none_or(|retry_at| Instant::now() >= retry_at)
        {
            self.merge_retry_at = None;
            self.switch_to_daily(AfterSwitch::Continue)?;
            self.merge_pending_sidecars();
        }
        if self.sink.encoder().is_none() {
            return Err(no_open_file(&self.prefix));
        }

        let mut itoa = itoa::Buffer::new();
        self.batch
            .put_slice(itoa.format(timestamp.as_nanosecond()).as_bytes());
        self.batch.put_u8(b' ');
        self.batch.put(data);
        self.batch.put_u8(b'\n');
        if self.batch.len() >= ZSTD_BATCH_BYTES {
            self.flush_batch()?;
        }
        Ok(())
    }

    /// Finish the day's file, queue a sidecar that could not be switched yet,
    /// and open the next day's file.
    fn rotate(&mut self, timestamp: Timestamp) -> io::Result<()> {
        self.switch_to_daily(AfterSwitch::Release)?;
        let finalized = match self.finalize() {
            Ok(()) => true,
            Err(error) => {
                error!(
                    path = %self.prefix.display(),
                    %error,
                    "failed to finalize file on rotation; continuing with the new file"
                );
                self.mark_degraded(self.prefix.display().to_string(), &error);
                false
            }
        };
        match mem::replace(&mut self.sink, Sink::Closed) {
            Sink::Sidecar { path, .. } | Sink::SealedSidecar(path) if finalized => {
                warn!(
                    path = %path.display(),
                    "UTC date changed before switch; keeping the overlap sidecar until the daily file is free"
                );
                self.pending_sidecars.push(PendingSidecar {
                    date: self.day.date,
                    path,
                });
            }
            Sink::Sidecar { path, .. } | Sink::SealedSidecar(path) => warn!(
                path = %path.display(),
                "UTC date changed before switch; leaving incomplete overlap sidecar on disk"
            ),
            daily_or_closed => self.sink = daily_or_closed,
        }
        let day = Day::containing(timestamp)?;
        let sink = open_sink(&self.prefix, day.date, &self.run_id)?;
        self.batch.clear();
        self.sink = sink;
        self.day = day;
        info!(path = %self.prefix.display(), "date changed, file rotated");
        Ok(())
    }

    /// Once the incumbent has released the daily file, append this run's
    /// sidecar to it and delete the sidecar — the only time one is removed.
    ///
    /// The lock is probed for the open file's day: a late pre-midnight record
    /// must not switch today's sidecar into yesterday's file.
    fn switch_to_daily(&mut self, after: AfterSwitch) -> io::Result<()> {
        let Some(sidecar) = self.sink.sidecar() else {
            return Ok(());
        };
        let Some(mut daily) = try_lock_daily(&self.prefix, self.day.date)? else {
            return Ok(());
        };
        let sidecar = sidecar.to_path_buf();
        if let Err(error) = self.finalize() {
            error!(
                path = %sidecar.display(),
                %error,
                "failed to finalize handover sidecar before switching to the daily file"
            );
            self.mark_degraded(sidecar.display().to_string(), &error);
            // An unfinished frame must never be appended: everything after it
            // in the daily file would be undecodable.
            warn!(path = %sidecar.display(), "leaving unfinished overlap sidecar on disk");
            // Records the sidecar did not take go to the daily file.
            let after = if self.batch.is_empty() {
                after
            } else {
                AfterSwitch::Continue
            };
            return self.continue_in(daily, after);
        }
        if let Err(error) = merge_sidecar(&mut daily, &sidecar) {
            error!(
                path = %sidecar.display(),
                %error,
                "failed to append overlap sidecar after switch"
            );
            self.mark_degraded(sidecar.display().to_string(), &error);
            self.merge_retry_at = Some(Instant::now() + MERGE_RETRY_BACKOFF);
            // Unlock: the fallback probes the lock again through another fd.
            drop(daily);
            return self.resume_or_queue_sidecar(sidecar, after);
        }
        self.continue_in(daily, after)
    }

    /// After a failed append of a finished sidecar: reopen it, so the day's
    /// records keep going there and a later switch retries the append.
    /// Failing that, append it now if the daily file is free, or queue it
    /// when releasing.
    fn resume_or_queue_sidecar(&mut self, sidecar: PathBuf, after: AfterSwitch) -> io::Result<()> {
        let resume_error = match reopen_sidecar(&sidecar) {
            Ok(encoder) => {
                self.sink = Sink::Sidecar {
                    encoder,
                    path: sidecar,
                };
                return Ok(());
            }
            Err(error) => error,
        };
        error!(
            path = %sidecar.display(),
            %resume_error,
            "failed to resume overlap sidecar after a failed merge"
        );
        let merged = match try_lock_daily(&self.prefix, self.day.date)? {
            None => Err(resume_error),
            Some(mut daily) => match merge_sidecar(&mut daily, &sidecar) {
                Ok(()) => Ok(daily),
                Err(error) => {
                    self.mark_degraded(sidecar.display().to_string(), &error);
                    Err(error)
                }
            },
        };
        match (merged, after) {
            (Ok(daily), after) => self.continue_in(daily, after),
            // The sink stays a sealed sidecar for the next switch to retry.
            (Err(error), AfterSwitch::Continue) => Err(error),
            (Err(_), AfterSwitch::Release) => {
                self.pending_sidecars.push(PendingSidecar {
                    date: self.day.date,
                    path: sidecar,
                });
                self.sink = Sink::Closed;
                Ok(())
            }
        }
    }

    fn continue_in(&mut self, daily: File, after: AfterSwitch) -> io::Result<()> {
        self.sink = Sink::Closed;
        if after == AfterSwitch::Continue {
            self.sink = Sink::Daily(ZstdEncoder::new(daily, ZSTD_LEVEL)?);
        }
        Ok(())
    }

    fn merge_pending_sidecars(&mut self) {
        for sidecar in mem::take(&mut self.pending_sidecars) {
            match merge_into_daily(&self.prefix, sidecar.date, &sidecar.path) {
                Ok(true) => {}
                Ok(false) => self.pending_sidecars.push(sidecar),
                Err(error) => {
                    error!(
                        path = %sidecar.path.display(),
                        %error,
                        "failed to append overlap sidecar after switch"
                    );
                    self.mark_degraded(sidecar.path.display().to_string(), &error);
                    self.merge_retry_at = Some(Instant::now() + MERGE_RETRY_BACKOFF);
                    self.pending_sidecars.push(sidecar);
                }
            }
        }
    }

    fn holds_sidecars(&self) -> bool {
        self.sink.sidecar().is_some() || !self.pending_sidecars.is_empty()
    }

    fn flush_batch(&mut self) -> io::Result<()> {
        if self.batch.is_empty() {
            return Ok(());
        }
        let Some(encoder) = self.sink.encoder() else {
            return Err(no_open_file(&self.prefix));
        };
        encoder.write_all(&self.batch)?;
        self.batch.clear();
        Ok(())
    }

    /// Flush the batch, finish the open zstd frame and fsync it. A finished
    /// sidecar stays the sink until it is appended.
    fn finalize(&mut self) -> io::Result<()> {
        self.flush_batch()?;
        let encoder = match mem::replace(&mut self.sink, Sink::Closed) {
            Sink::Daily(encoder) => encoder,
            Sink::Sidecar { encoder, path } => {
                self.sink = Sink::SealedSidecar(path);
                encoder
            }
            idle => {
                self.sink = idle;
                return Ok(());
            }
        };
        let file = encoder.finish().map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "failed to finish zstd stream {}: {error}",
                    self.prefix.display()
                ),
            )
        })?;
        // A failed fsync may already have dropped the dirty pages and marked
        // them clean, so a retry could succeed with the data lost.
        file.sync_all().map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("failed to sync {}: {error}", self.prefix.display()),
            )
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
}

impl Drop for RotatingFile {
    fn drop(&mut self) {
        if let Err(error) = self.finalize() {
            warn!(path = %self.prefix.display(), %error, "failed to finalize file on drop");
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
            let prefix = Path::new(&self.path).join(name.as_ref());
            let mut rotating_file =
                RotatingFile::new(recv_time, prefix, self.run_id.clone(), self.quality.clone())?;
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
            if let Err(error) = file.switch_to_daily(AfterSwitch::Release) {
                error!(
                    symbol = %symbol,
                    %error,
                    "failed to merge overlap sidecar after switch"
                );
                if result.is_ok() {
                    result = Err(error.into());
                }
            }
            file.merge_pending_sidecars();
            if file.holds_sidecars() {
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
                .sink
                .sidecar()
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
                .sink
                .sidecar()
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
        let mut streamed = zstd::stream::write::Encoder::new(Vec::new(), 1).unwrap();
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
        writer.files.get_mut("btcusdt").unwrap().sink =
            Sink::Daily(ZstdEncoder::new(dev_null, ZSTD_LEVEL).unwrap());
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
