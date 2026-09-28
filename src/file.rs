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
/// Every cleanly closed file ends with a trailer: a zstd skippable frame of
/// `TRAILER_MAGIC`, its 16-byte size, `TRAILER_TAG` and the file length at
/// which the trailer starts, all little-endian. The next writer checks the
/// file's end with one `pread` instead of walking every block; decoders skip
/// it.
const TRAILER_MAGIC: u32 = 0x184D_2A5C;
const TRAILER_TAG: [u8; 8] = *b"COLLECTR";
const TRAILER_LEN: usize = 24;

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

/// The trailer of a file whose last frame ends at `offset`.
fn trailer(offset: u64) -> [u8; TRAILER_LEN] {
    let mut trailer = [0; TRAILER_LEN];
    trailer[..4].copy_from_slice(&TRAILER_MAGIC.to_le_bytes());
    trailer[4..8].copy_from_slice(&(TRAILER_LEN as u32 - 8).to_le_bytes());
    trailer[8..16].copy_from_slice(&TRAILER_TAG);
    trailer[16..].copy_from_slice(&offset.to_le_bytes());
    trailer
}

/// Whether `file` ends exactly where a complete zstd frame does.
///
/// A writer killed mid-frame leaves a tail no decoder can step past, so every
/// frame appended after it would be lost to readers. A file that ends with its
/// trailer takes one `pread`; one written without (by an older binary) is
/// walked.
fn ends_cleanly(file: &File) -> io::Result<bool> {
    let len = file.metadata()?.len();
    if len == 0 {
        return Ok(true);
    }
    if let Some(start) = len.checked_sub(TRAILER_LEN as u64) {
        let mut tail = [0; TRAILER_LEN];
        file.read_exact_at(&mut tail, start)?;
        if tail == trailer(start) {
            return Ok(true);
        }
    }
    ends_on_frame_boundary(file)
}

/// The walk behind [`ends_cleanly`]: only frame and block headers are read —
/// one small `pread` per block, no decompression.
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
    #[cfg(test)]
    fault::check(fault::Step::Lock)?;
    let daily = day_file(prefix, date, format_args!(""));
    let Some(file) = open_locked(&daily)? else {
        return Ok(None);
    };
    if ends_cleanly(&file)? {
        return Ok(Some(file));
    }
    // The last writer died mid-frame: set the file aside, readable up to
    // where it stops, and start the day again in a fresh one.
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

/// End a file whose last frame is finished with its trailer and fsync it. A
/// torn trailer is cut off again, so the file still ends on its frame.
fn seal(file: &mut File) -> io::Result<()> {
    let start = file.metadata()?.len();
    if let Err(error) = file.write_all(&trailer(start)) {
        file.set_len(start)?;
        return Err(error);
    }
    // A failed fsync may already have dropped the dirty pages and marked them
    // clean, so a retry could succeed with the data lost.
    file.sync_all()
}

/// What became of a finished sidecar.
enum Merge {
    Appended,
    /// It does not end on a frame boundary, so it stays on disk: appended, it
    /// would cut off everything written after it.
    Unterminated,
}

/// Append a finished sidecar and a fresh trailer to the locked daily file,
/// fsync, and delete the sidecar. A failed append is rolled back, so the daily
/// file never ends mid-frame.
fn merge_sidecar(daily: &mut File, sidecar: &Path) -> io::Result<Merge> {
    let mut source = File::open(sidecar)?;
    if !ends_cleanly(&source)? {
        return Ok(Merge::Unterminated);
    }
    let start = daily.metadata()?.len();
    let appended = (|| {
        #[cfg(test)]
        fault::check(fault::Step::Append)?;
        let copied = io::copy(&mut source, daily)?;
        daily.write_all(&trailer(start + copied))?;
        daily.sync_all()
    })();
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
    Ok(Merge::Appended)
}

/// Merge a finished sidecar into its day's file; `None` while another writer
/// still holds that file.
fn merge_into_daily(prefix: &Path, date: Date, sidecar: &Path) -> io::Result<Option<Merge>> {
    let Some(mut daily) = try_lock_daily(prefix, date)? else {
        return Ok(None);
    };
    merge_sidecar(&mut daily, sidecar).map(Some)
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
    #[cfg(test)]
    fault::check(fault::Step::Resume)?;
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
    /// never for a record's timestamp: a late pre-midnight record must not
    /// switch today's sidecar into yesterday's file.
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
            // An unfinished frame must never be appended.
            warn!(path = %sidecar.display(), "leaving unfinished overlap sidecar on disk");
            // Records the sidecar did not take go to the daily file.
            let after = if self.batch.is_empty() {
                after
            } else {
                AfterSwitch::Continue
            };
            return self.continue_in(daily, after);
        }
        match merge_sidecar(&mut daily, &sidecar) {
            Ok(Merge::Appended) => {}
            Ok(Merge::Unterminated) => self.sidecar_unterminated(&sidecar),
            Err(error) => {
                error!(
                    path = %sidecar.display(),
                    %error,
                    "failed to append overlap sidecar after switch"
                );
                self.mark_degraded(sidecar.display().to_string(), &error);
                self.merge_retry_at = Some(Instant::now() + MERGE_RETRY_BACKOFF);
                return self.resume_or_queue_sidecar(sidecar, daily, after);
            }
        }
        self.continue_in(daily, after)
    }

    /// After a failed append of a finished sidecar: reopen it, so the day's
    /// records keep going there and a later switch retries the append.
    /// Failing that, retry the append through the daily file still locked
    /// here — not through a fresh lock, which would verify it again — or
    /// queue the sidecar when releasing.
    fn resume_or_queue_sidecar(
        &mut self,
        sidecar: PathBuf,
        mut daily: File,
        after: AfterSwitch,
    ) -> io::Result<()> {
        match reopen_sidecar(&sidecar) {
            Ok(encoder) => {
                self.sink = Sink::Sidecar {
                    encoder,
                    path: sidecar,
                };
                return Ok(());
            }
            Err(error) => error!(
                path = %sidecar.display(),
                %error,
                "failed to resume overlap sidecar after a failed merge"
            ),
        }
        let merged = match merge_sidecar(&mut daily, &sidecar) {
            Ok(merge) => {
                if let Merge::Unterminated = merge {
                    self.sidecar_unterminated(&sidecar);
                }
                Ok(daily)
            }
            Err(error) => {
                self.mark_degraded(sidecar.display().to_string(), &error);
                Err(error)
            }
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
                Ok(Some(Merge::Appended)) => {}
                Ok(Some(Merge::Unterminated)) => self.sidecar_unterminated(&sidecar.path),
                Ok(None) => self.pending_sidecars.push(sidecar),
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

    fn sidecar_unterminated(&mut self, sidecar: &Path) {
        let error = "overlap sidecar does not end on a frame boundary; left on disk unmerged";
        error!(path = %sidecar.display(), "{error}");
        self.mark_degraded(sidecar.display().to_string(), error);
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

    /// Flush the batch, finish the open zstd frame, end the file with its
    /// trailer and fsync it. A finished sidecar stays the sink until it is
    /// appended.
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
        let mut file = encoder.finish().map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "failed to finish zstd stream {}: {error}",
                    self.prefix.display()
                ),
            )
        })?;
        seal(&mut file).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("failed to sync {}: {error}", self.prefix.display()),
            )
        })
    }

    fn mark_degraded(&mut self, target: String, error: impl fmt::Display) {
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
    dir: PathBuf,
    run_id: String,
    files: HashMap<Symbol, RotatingFile>,
    quality: QualityReporter,
}

impl Writer {
    pub fn new(dir: impl AsRef<Path>, run_id: &str, quality: QualityReporter) -> Self {
        Self {
            dir: dir.as_ref().to_path_buf(),
            run_id: run_id.to_owned(),
            files: HashMap::new(),
            quality,
        }
    }

    pub fn write(
        &mut self,
        recv_time: Timestamp,
        symbol: Symbol,
        data: Bytes,
    ) -> anyhow::Result<()> {
        if let Some(file) = self.files.get_mut(&symbol) {
            return Ok(file.write(recv_time, data)?);
        }
        // Distinct symbols never share a file: the encoding is injective.
        let prefix = self.dir.join(&*encode_symbol(&symbol));
        let mut file =
            RotatingFile::new(recv_time, prefix, self.run_id.clone(), self.quality.clone())?;
        file.write(recv_time, data)?;
        self.files.insert(symbol, file);
        Ok(())
    }

    /// Append and delete every sidecar whose daily file is free, then seal
    /// what is left: a sidecar whose daily file the incumbent still holds
    /// stays on disk. Every file is closed even after an error; the first
    /// one is returned.
    pub fn close(&mut self) -> anyhow::Result<()> {
        let mut first_error = None;
        for (symbol, file) in &mut self.files {
            if let Err(error) = file.switch_to_daily(AfterSwitch::Release) {
                error!(%symbol, %error, "failed to merge overlap sidecar after switch");
                first_error.get_or_insert(error.into());
            }
            file.merge_pending_sidecars();
            if file.holds_sidecars() {
                info!(%symbol, "closing before switch; leaving overlap sidecar on disk");
            }
            match file.finalize() {
                Ok(()) if file.degraded => {
                    error!(%symbol, "file closed, but an earlier rotation could not be finalized");
                    first_error.get_or_insert(anyhow::anyhow!(
                        "{symbol}: an earlier rotation could not be finalized"
                    ));
                }
                Ok(()) => info!(%symbol, "file closed cleanly"),
                Err(error) => {
                    error!(%symbol, %error, "failed to close file");
                    first_error.get_or_insert(error.into());
                }
            }
        }
        self.files.clear();
        first_error.map_or(Ok(()), Err)
    }
}

/// Failures of steps a real filesystem cannot be made to fail on demand.
#[cfg(test)]
mod fault {
    use std::{cell::Cell, io};

    #[derive(Clone, Copy, Debug)]
    pub enum Step {
        Lock,
        Append,
        Resume,
    }
    const STEPS: usize = 3;

    thread_local! {
        static ARMED: Cell<[u32; STEPS]> = const { Cell::new([0; STEPS]) };
        static CALLS: Cell<[u32; STEPS]> = const { Cell::new([0; STEPS]) };
    }

    /// Fail the next `times` times `step` is reached on this thread.
    pub fn arm(step: Step, times: u32) {
        ARMED.with(|armed| {
            let mut counts = armed.get();
            counts[step as usize] = times;
            armed.set(counts);
        });
    }

    /// How often `step` was reached on this thread.
    pub fn calls(step: Step) -> u32 {
        CALLS.with(|calls| calls.get()[step as usize])
    }

    pub fn check(step: Step) -> io::Result<()> {
        CALLS.with(|calls| {
            let mut counts = calls.get();
            counts[step as usize] += 1;
            calls.set(counts);
        });
        ARMED.with(|armed| {
            let mut counts = armed.get();
            let left = &mut counts[step as usize];
            if *left == 0 {
                return Ok(());
            }
            *left -= 1;
            armed.set(counts);
            Err(io::Error::other(format!("injected {step:?} failure")))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jiff::SignedDuration;

    /// 2027-01-15 08:00 UTC.
    const DAY_ONE_NS: i128 = 1_800_000_000_000_000_000;
    const DAY_ONE: &str = "btcusdt_20270115.zst";
    const DAY_TWO: &str = "btcusdt_20270116.zst";

    fn day_one() -> Timestamp {
        Timestamp::from_nanosecond(DAY_ONE_NS).unwrap()
    }

    fn day_two() -> Timestamp {
        day_one() + SignedDuration::from_hours(24)
    }

    fn test_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "collector-{tag}-test-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn writer(dir: &Path, run_id: &str) -> Writer {
        Writer::new(dir, run_id, QualityReporter::disabled())
    }

    fn put(writer: &mut Writer, recv_time: Timestamp, data: &'static [u8]) {
        writer
            .write(recv_time, Symbol::from("btcusdt"), Bytes::from_static(data))
            .unwrap();
    }

    fn names(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        names
    }

    fn has_sidecar(dir: &Path, run_id: &str) -> bool {
        let suffix = format!("_{run_id}.zst");
        names(dir).iter().any(|name| name.ends_with(&suffix))
    }

    fn decoded(path: impl AsRef<Path>) -> String {
        let bytes = zstd::decode_all(fs::read(path).unwrap().as_slice()).unwrap();
        String::from_utf8(bytes).unwrap()
    }

    fn zstd_frame_count(bytes: &[u8]) -> usize {
        bytes
            .windows(4)
            .filter(|window| *window == ZSTD_MAGIC.to_le_bytes())
            .count()
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
        encoded.sort();
        encoded.dedup();
        assert_eq!(encoded.len(), symbols.len(), "collision: {encoded:?}");
    }

    #[test]
    fn colliding_symbols_get_separate_files() {
        let dir = test_dir("collision");
        let mut writer = writer(&dir, "test-run");
        for symbol in ["purr/usdc", "purr_usdc"] {
            writer
                .write(day_one(), Symbol::from(symbol), Bytes::from_static(b"{}"))
                .unwrap();
        }
        writer.close().unwrap();

        let names = names(&dir);
        assert_eq!(
            names,
            ["purr%2Fusdc_20270115.zst", "purr_usdc_20270115.zst"]
        );
        for name in &names {
            assert_eq!(decoded(dir.join(name)), format!("{DAY_ONE_NS} {{}}\n"));
        }
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn writes_a_symbol_containing_a_path_separator() {
        let dir = test_dir("separator");
        let mut writer = writer(&dir, "test-run");
        writer
            .write(
                day_one(),
                Symbol::from("purr/usdc"),
                Bytes::from_static(b"{}"),
            )
            .unwrap();
        writer.close().unwrap();

        assert_eq!(names(&dir), ["purr%2Fusdc_20270115.zst"]);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn same_day_writes_share_one_file() {
        let dir = test_dir("daily");
        let mut writer = writer(&dir, "daily-run");
        let later_same_day = day_one() + SignedDuration::from_hours(3);
        put(&mut writer, day_one(), b"{}");
        put(&mut writer, later_same_day, b"{}");
        writer.close().unwrap();

        assert_eq!(names(&dir), [DAY_ONE]);
        assert_eq!(
            decoded(dir.join(DAY_ONE)),
            format!(
                "{DAY_ONE_NS} {{}}\n{} {{}}\n",
                later_same_day.as_nanosecond()
            )
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn date_boundary_opens_a_new_daily_file() {
        let dir = test_dir("day-boundary");
        let mut writer = writer(&dir, "boundary-run");
        put(&mut writer, day_one(), b"{}");
        put(&mut writer, day_two(), b"{}");
        writer.close().unwrap();

        assert_eq!(names(&dir), [DAY_ONE, DAY_TWO]);
        for name in [DAY_ONE, DAY_TWO] {
            assert_eq!(decoded(dir.join(name)).lines().count(), 1, "{name}");
        }
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn busy_daily_file_falls_back_to_run_sidecar() {
        let dir = test_dir("sidecar");
        let mut incumbent = writer(&dir, "incumbent");
        put(&mut incumbent, day_one(), br#"{"n":1}"#);
        let mut challenger = writer(&dir, "challenger");
        put(&mut challenger, day_one(), br#"{"n":2}"#);
        assert!(
            matches!(challenger.files["btcusdt"].sink, Sink::Sidecar { .. }),
            "challenger must use a sidecar while the daily file is locked"
        );

        incumbent.close().unwrap();
        put(&mut challenger, day_one(), br#"{"n":3}"#);
        assert!(
            matches!(challenger.files["btcusdt"].sink, Sink::Daily(_)),
            "after the incumbent exits, writes must reclaim the daily file"
        );
        challenger.close().unwrap();

        assert_eq!(
            names(&dir),
            [DAY_ONE],
            "the sidecar must be merged and removed"
        );
        let text = decoded(dir.join(DAY_ONE));
        for record in [r#"{"n":1}"#, r#"{"n":2}"#, r#"{"n":3}"#] {
            assert!(text.contains(record), "{record} missing: {text}");
        }
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn close_merges_sidecar_without_another_write() {
        let dir = test_dir("close-merge");
        let mut incumbent = writer(&dir, "incumbent");
        put(&mut incumbent, day_one(), br#"{"n":1}"#);
        let mut challenger = writer(&dir, "challenger");
        put(&mut challenger, day_one(), br#"{"n":2}"#);
        incumbent.close().unwrap();
        assert!(
            has_sidecar(&dir, "challenger"),
            "incumbent close must not steal a live overlap sidecar"
        );
        challenger.close().unwrap();

        assert_eq!(
            names(&dir),
            [DAY_ONE],
            "close must merge and remove the sidecar"
        );
        let merged = fs::read(dir.join(DAY_ONE)).unwrap();
        assert_eq!(
            zstd_frame_count(&merged),
            2,
            "close should not append an empty extra zstd frame"
        );
        let text = decoded(dir.join(DAY_ONE));
        assert!(text.contains(r#"{"n":1}"#), "{text}");
        assert!(
            text.contains(r#"{"n":2}"#),
            "sidecar records missing: {text}"
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn close_leaves_sidecar_when_daily_file_is_still_locked() {
        let dir = test_dir("close-leave-sidecar");
        let mut incumbent = writer(&dir, "incumbent");
        put(&mut incumbent, day_one(), br#"{"n":1}"#);
        let mut challenger = writer(&dir, "challenger");
        put(&mut challenger, day_one(), br#"{"n":2}"#);
        challenger.close().unwrap();

        let sidecar = dir.join("btcusdt_20270115_challenger.zst");
        assert!(
            decoded(&sidecar).contains(r#"{"n":2}"#),
            "challenger must leave a complete sidecar while the incumbent holds the daily file"
        );
        incumbent.close().unwrap();
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn midnight_reclaim_merges_sidecar_after_incumbent_exits() {
        let dir = test_dir("midnight-merge");
        let mut incumbent = writer(&dir, "incumbent");
        put(&mut incumbent, day_one(), br#"{"n":1}"#);
        let mut challenger = writer(&dir, "challenger");
        put(&mut challenger, day_one(), br#"{"n":2}"#);
        incumbent.close().unwrap();
        put(&mut challenger, day_two(), br#"{"n":3}"#);
        challenger.close().unwrap();

        assert_eq!(
            names(&dir),
            [DAY_ONE, DAY_TWO],
            "midnight rotation must merge yesterday's sidecar"
        );
        assert_eq!(
            zstd_frame_count(&fs::read(dir.join(DAY_ONE)).unwrap()),
            2,
            "yesterday should be incumbent frame + sidecar frame, not an extra empty frame"
        );
        let day_one = decoded(dir.join(DAY_ONE));
        assert!(day_one.contains(r#"{"n":1}"#), "{day_one}");
        assert!(
            day_one.contains(r#"{"n":2}"#),
            "sidecar records missing: {day_one}"
        );
        assert!(decoded(dir.join(DAY_TWO)).contains(r#"{"n":3}"#));
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn midnight_keeps_sidecar_while_incumbent_lock_is_held() {
        let dir = test_dir("midnight-lock");
        let mut incumbent = writer(&dir, "incumbent");
        put(&mut incumbent, day_one(), br#"{"n":1}"#);
        let mut challenger = writer(&dir, "challenger");
        put(&mut challenger, day_one(), br#"{"n":2}"#);
        put(&mut challenger, day_two(), br#"{"n":3}"#);
        assert_eq!(challenger.files["btcusdt"].pending_sidecars.len(), 1);
        assert!(
            has_sidecar(&dir, "challenger"),
            "the sidecar must remain until the incumbent releases yesterday's lock"
        );

        incumbent.close().unwrap();
        challenger.close().unwrap();

        assert_eq!(
            names(&dir),
            [DAY_ONE, DAY_TWO],
            "close must merge the leftover midnight sidecar"
        );
        let day_one = decoded(dir.join(DAY_ONE));
        assert!(day_one.contains(r#"{"n":1}"#), "{day_one}");
        assert!(
            day_one.contains(r#"{"n":2}"#),
            "sidecar records missing: {day_one}"
        );
        assert!(decoded(dir.join(DAY_TWO)).contains(r#"{"n":3}"#));
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn duplicate_run_id_cannot_share_a_sidecar() {
        let dir = test_dir("sidecar-exclusive");
        let mut incumbent = writer(&dir, "incumbent");
        put(&mut incumbent, day_one(), br#"{"n":1}"#);
        let mut first = writer(&dir, "same-run");
        put(&mut first, day_one(), br#"{"n":2}"#);
        let mut second = writer(&dir, "same-run");
        let error = second
            .write(
                day_one(),
                Symbol::from("btcusdt"),
                Bytes::from_static(br#"{"n":3}"#),
            )
            .unwrap_err();
        assert!(
            error
                .downcast_ref::<io::Error>()
                .is_some_and(|error| error.kind() == io::ErrorKind::AlreadyExists),
            "a second writer with the same run id must not share the sidecar: {error}"
        );
        first.close().unwrap();
        incumbent.close().unwrap();
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn frame_walk_accepts_whole_frames_and_rejects_a_truncated_tail() {
        let dir = test_dir("frame-walk");
        let path = dir.join("walk.zst");
        let check = |bytes: &[u8]| {
            fs::write(&path, bytes).unwrap();
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
        fs::remove_dir_all(&dir).unwrap();
    }

    /// Both collectors write this trailer; files from either must verify in
    /// the other.
    #[test]
    fn the_trailer_bytes_are_pinned() {
        assert_eq!(
            trailer(0x0102_0304_0506_0708),
            [
                0x5C, 0x2A, 0x4D, 0x18, // skippable-frame magic
                0x10, 0x00, 0x00, 0x00, // frame size: 16
                b'C', b'O', b'L', b'L', b'E', b'C', b'T', b'R', //
                0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, // offset
            ]
        );
    }

    #[test]
    fn a_trailer_spares_the_walk_and_a_misplaced_one_does_not() {
        let dir = test_dir("trailer");
        let path = dir.join("check.zst");
        let check = |bytes: &[u8]| {
            fs::write(&path, bytes).unwrap();
            ends_cleanly(&File::open(&path).unwrap()).unwrap()
        };
        let with_trailer = |body: &[u8], offset: u64| [body, &trailer(offset)].concat();
        let frames = zstd::encode_all(&b"records"[..], 1).unwrap();
        let garbage = b"no frame or block header here".as_slice();
        let len = |body: &[u8]| body.len() as u64;

        // The walk would reject this body: only the trailer was read.
        assert!(check(&with_trailer(garbage, len(garbage))));
        // A trailer that does not end the file it describes is not trusted.
        assert!(!check(&with_trailer(garbage, len(garbage) - 1)));
        assert!(check(&with_trailer(&frames, len(&frames) + 1)));
        assert!(check(&frames));
        assert!(!check(&frames[..frames.len() - 1]));
        fs::remove_dir_all(&dir).unwrap();
    }

    fn ends_with_its_trailer(bytes: &[u8]) -> bool {
        let start = bytes.len() - TRAILER_LEN;
        bytes[start..] == trailer(start as u64)
    }

    #[test]
    fn closed_and_merged_files_end_with_their_trailer() {
        let dir = test_dir("trailer-close");
        let mut incumbent = writer(&dir, "incumbent");
        put(&mut incumbent, day_one(), br#"{"n":1}"#);
        let mut challenger = writer(&dir, "challenger");
        put(&mut challenger, day_one(), br#"{"n":2}"#);
        challenger.close().unwrap();
        let sidecar = fs::read(dir.join("btcusdt_20270115_challenger.zst")).unwrap();
        assert!(ends_with_its_trailer(&sidecar));
        incumbent.close().unwrap();
        assert!(ends_with_its_trailer(&fs::read(dir.join(DAY_ONE)).unwrap()));
        fs::remove_dir_all(&dir).unwrap();

        let dir = test_dir("trailer-merge");
        let mut incumbent = writer(&dir, "incumbent");
        put(&mut incumbent, day_one(), br#"{"n":1}"#);
        let mut challenger = writer(&dir, "challenger");
        put(&mut challenger, day_one(), br#"{"n":2}"#);
        incumbent.close().unwrap();
        put(&mut challenger, day_one(), br#"{"n":3}"#);
        assert_eq!(names(&dir), [DAY_ONE], "the sidecar must be merged");
        assert!(ends_with_its_trailer(&fs::read(dir.join(DAY_ONE)).unwrap()));
        challenger.close().unwrap();
        assert!(ends_with_its_trailer(&fs::read(dir.join(DAY_ONE)).unwrap()));
        assert_eq!(
            decoded(dir.join(DAY_ONE)),
            format!(
                "{DAY_ONE_NS} {{\"n\":1}}\n{DAY_ONE_NS} {{\"n\":2}}\n{DAY_ONE_NS} {{\"n\":3}}\n"
            ),
            "decoders must skip every trailer and yield exactly the records"
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn an_unterminated_sidecar_is_reported_and_left_unmerged() {
        let dir = test_dir("sidecar-unterminated");
        let mut incumbent = writer(&dir, "incumbent");
        put(&mut incumbent, day_one(), br#"{"n":1}"#);
        let (quality, events) = QualityReporter::test_channel();
        let mut challenger = Writer::new(&dir, "challenger", quality);
        put(&mut challenger, day_one(), br#"{"n":2}"#);
        put(&mut challenger, day_two(), br#"{"n":3}"#);
        let sidecar = dir.join("btcusdt_20270115_challenger.zst");
        let bytes = fs::read(&sidecar).unwrap();
        fs::write(&sidecar, &bytes[..bytes.len() - TRAILER_LEN - 1]).unwrap();
        incumbent.close().unwrap();

        put(&mut challenger, day_two(), br#"{"n":4}"#);
        assert!(challenger.files["btcusdt"].pending_sidecars.is_empty());
        assert!(matches!(
            events.try_recv(),
            Ok(QualityEvent::StorageDegraded { target, .. }) if target == sidecar.display().to_string()
        ));
        assert!(
            sidecar.exists(),
            "an unterminated sidecar must stay on disk"
        );
        assert!(!decoded(dir.join(DAY_ONE)).contains(r#"{"n":2}"#));
        assert!(
            challenger.close().is_err(),
            "close must not report a clean exit"
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_failed_append_is_retried_through_the_lock_already_held() {
        let dir = test_dir("merge-relock");
        let mut incumbent = writer(&dir, "incumbent");
        put(&mut incumbent, day_one(), br#"{"n":1}"#);
        let mut challenger = writer(&dir, "challenger");
        put(&mut challenger, day_one(), br#"{"n":2}"#);
        incumbent.close().unwrap();
        fault::arm(fault::Step::Append, 1);
        fault::arm(fault::Step::Resume, 1);
        let locks = fault::calls(fault::Step::Lock);

        put(&mut challenger, day_one(), br#"{"n":3}"#);
        assert_eq!(
            fault::calls(fault::Step::Lock) - locks,
            1,
            "the daily file must not be locked and verified again"
        );
        assert!(matches!(challenger.files["btcusdt"].sink, Sink::Daily(_)));
        challenger.close().unwrap_err();
        assert_eq!(names(&dir), [DAY_ONE]);
        let text = decoded(dir.join(DAY_ONE));
        for record in [r#"{"n":1}"#, r#"{"n":2}"#, r#"{"n":3}"#] {
            assert!(text.contains(record), "{record} missing: {text}");
        }
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn unterminated_daily_file_is_set_aside_not_appended_to() {
        let dir = test_dir("unterminated");
        let daily = dir.join(DAY_ONE);
        let crashed = zstd::encode_all(&b"before-crash"[..], 1).unwrap();
        let crashed = &crashed[..crashed.len() - 2];
        fs::write(&daily, crashed).unwrap();

        let mut writer = writer(&dir, "restart");
        put(&mut writer, day_one(), b"after-restart");
        writer.close().unwrap();

        assert_eq!(decoded(&daily), format!("{DAY_ONE_NS} after-restart\n"));
        let names = names(&dir);
        let salvage = names
            .iter()
            .find(|name| name.starts_with("btcusdt_20270115_unterminated-"))
            .unwrap_or_else(|| panic!("damaged file must be kept aside: {names:?}"));
        assert_eq!(fs::read(dir.join(salvage)).unwrap(), crashed);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn late_record_does_not_switch_into_yesterdays_file() {
        let dir = test_dir("late-record");
        let mut incumbent = writer(&dir, "incumbent");
        put(&mut incumbent, day_one(), b"incumbent-day-one");
        put(&mut incumbent, day_two(), b"incumbent-day-two");
        let mut challenger = writer(&dir, "challenger");
        put(&mut challenger, day_two(), b"sidecar-day-two");
        incumbent.close().unwrap();
        // Stamped before midnight, delivered after the incumbent let go.
        put(&mut challenger, day_one(), b"late");
        challenger.close().unwrap();

        let day_one = decoded(dir.join(DAY_ONE));
        assert!(!day_one.contains("sidecar-day-two"), "{day_one}");
        assert!(!day_one.contains("late"), "{day_one}");
        let day_two = decoded(dir.join(DAY_TWO));
        for payload in ["incumbent-day-two", "sidecar-day-two", "late"] {
            assert!(day_two.contains(payload), "day two missing {payload}");
        }
        fs::remove_dir_all(&dir).unwrap();
    }

    /// fsync(2) on /dev/null fails with EINVAL, standing in for a disk error.
    #[cfg(target_os = "linux")]
    #[test]
    fn failed_fsync_is_reported_and_fails_close() {
        let dir = test_dir("fsync");
        let (quality, events) = QualityReporter::test_channel();
        let mut writer = Writer::new(&dir, "fsync", quality);
        put(&mut writer, day_one(), b"a");
        let dev_null = File::options().write(true).open("/dev/null").unwrap();
        writer.files.get_mut("btcusdt").unwrap().sink =
            Sink::Daily(ZstdEncoder::new(dev_null, ZSTD_LEVEL).unwrap());
        put(&mut writer, day_two(), b"b");

        assert!(
            matches!(
                events.try_recv(),
                Ok(QualityEvent::StorageDegraded { error, .. }) if error.contains("failed to sync")
            ),
            "a failed fsync must be reported"
        );
        assert!(
            writer.close().is_err(),
            "close must not report a clean exit"
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn rollback_append_restores_prior_length() {
        let dir = test_dir("rollback");
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
        assert_eq!(fs::read(&path).unwrap(), b"keep");
        fs::remove_dir_all(&dir).unwrap();
    }
}
