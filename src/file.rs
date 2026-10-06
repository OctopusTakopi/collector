use std::{
    borrow::Cow,
    collections::HashMap,
    fmt::{self, Write as _},
    fs::{self, File, TryLockError},
    io::{self, Read, Seek, SeekFrom, Write},
    mem,
    os::unix::fs::{FileExt, MetadataExt},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
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
/// A failed merge or reclaim is not retried before this, doubled per
/// consecutive failure up to `MERGE_RETRY_MAX`: an attempt may walk and fsync
/// the daily file, so retrying on every record would stall the writer for
/// every symbol.
const MERGE_RETRY_BACKOFF: Duration = Duration::from_secs(5);
const MERGE_RETRY_MAX: Duration = Duration::from_secs(600);
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
    #[cfg(test)]
    fault::check(fault::Step::Verify)?;
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
    #[cfg(test)]
    fault::check(fault::Step::Rollback)?;
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
/// An error leaves the day's file unusable for now, never the writer.
fn try_lock_daily(prefix: &Path, date: Date) -> io::Result<Option<File>> {
    #[cfg(test)]
    fault::check(fault::Step::Lock)?;
    let daily = day_file(prefix, date, format_args!(""));
    let Some(file) = open_locked(&daily)? else {
        return Ok(None);
    };
    match ends_cleanly(&file) {
        Ok(true) => return Ok(Some(file)),
        Ok(false) => {}
        Err(error) => warn!(
            path = %daily.display(),
            %error,
            "cannot verify the daily file; treating it as unterminated"
        ),
    }
    // The last writer died mid-frame, or the end cannot be checked: set the
    // file aside, readable up to where it stops, and start the day again in a
    // fresh one.
    let salvage = day_file(
        prefix,
        date,
        format_args!("_unterminated-{}", Timestamp::now().as_nanosecond()),
    );
    #[cfg(test)]
    fault::check(fault::Step::SetAside)?;
    fs::rename(&daily, &salvage)?;
    warn!(
        path = %daily.display(),
        salvage = %salvage.display(),
        "daily file does not end on a frame boundary; moved it aside instead of appending to it"
    );
    let fresh = open_locked(&daily);
    // The old inode stays locked until the fresh one is.
    drop(file);
    fresh
}

/// Why a sink could not be closed cleanly.
#[derive(Debug, thiserror::Error)]
enum FinalizeError {
    /// The batch or the end of the frame was not written: the file may end
    /// mid-frame and must never be appended.
    #[error(transparent)]
    Unfinished(io::Error),
    /// The frame is finished but was not synced: the file is whole, so a
    /// sidecar is still appended.
    #[error(transparent)]
    Unsynced(io::Error),
}

/// End `file`, whose last frame is finished, with its trailer and fsync it. A
/// torn trailer is cut off again, so the file still ends on its frame.
fn seal(file: &mut File, name: &Path) -> Result<(), FinalizeError> {
    let failed = |what: &str, error: io::Error| {
        io::Error::new(
            error.kind(),
            format!("failed to {what} {}: {error}", name.display()),
        )
    };
    let start = file
        .metadata()
        .map_err(|error| FinalizeError::Unsynced(failed("stat", error)))?
        .len();
    if let Err(error) = file.write_all(&trailer(start)) {
        let error = failed("write the trailer of", error);
        return Err(match file.set_len(start) {
            Ok(()) => FinalizeError::Unsynced(error),
            Err(_) => FinalizeError::Unfinished(error),
        });
    }
    #[cfg(test)]
    fault::check(fault::Step::Sync)
        .map_err(|error| FinalizeError::Unsynced(failed("sync", error)))?;
    // A failed fsync may already have dropped the dirty pages and marked them
    // clean, so a retry could succeed with the data lost.
    file.sync_all()
        .map_err(|error| FinalizeError::Unsynced(failed("sync", error)))
}

/// How much of `batch`, which holds whole records, an unfinished `sidecar`
/// already holds: through the batch's first copy of the last complete record
/// the sidecar decodes to. `None` if that record is not in the batch.
fn persisted_prefix(sidecar: &Path, batch: &[u8]) -> io::Result<Option<usize>> {
    let Some(last) = last_record(sidecar)? else {
        return Ok(None);
    };
    let mut end = 0;
    Ok(batch
        .split_inclusive(|&byte| byte == b'\n')
        .find_map(|record| {
            end += record.len();
            (record == last).then_some(end)
        }))
}

/// The last complete record in a file that may end mid-frame, read up to the
/// truncation.
fn last_record(path: &Path) -> io::Result<Option<Vec<u8>>> {
    let mut decoder = zstd::stream::read::Decoder::new(File::open(path)?)?;
    let mut chunk = vec![0; ZSTD_BATCH_BYTES];
    let (mut last, mut partial) = (Vec::new(), Vec::new());
    loop {
        let read = match decoder.read(&mut chunk) {
            Ok(0) => break,
            Ok(read) => read,
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error),
        };
        for piece in chunk[..read].split_inclusive(|&byte| byte == b'\n') {
            partial.extend_from_slice(piece);
            if piece.ends_with(b"\n") {
                mem::swap(&mut last, &mut partial);
                partial.clear();
            }
        }
    }
    Ok((!last.is_empty()).then_some(last))
}

/// What became of a finished sidecar.
enum Merge {
    Appended,
    /// It does not end on a frame boundary, so it stays on disk: appended, it
    /// would cut off everything written after it.
    Unterminated,
}

/// Why a finished sidecar was not appended.
#[derive(Debug, thiserror::Error)]
enum Unmerged {
    /// Reading the sidecar or appending it failed, and the append was rolled
    /// back: the daily file is as it was.
    #[error(transparent)]
    Failed(io::Error),
    /// The append failed and so did its rollback: the daily file may end
    /// mid-frame. It must not be written again before a fresh lock verifies
    /// it, and sets it aside if it is torn.
    #[error(transparent)]
    Torn(io::Error),
}

/// Append a finished sidecar and a fresh trailer to the locked daily file,
/// fsync, and delete the sidecar. A failed append is rolled back, so the daily
/// file never ends mid-frame unless that fails too.
fn merge_sidecar(daily: &mut File, sidecar: &Path) -> Result<Merge, Unmerged> {
    let mut source = File::open(sidecar).map_err(Unmerged::Failed)?;
    if !ends_cleanly(&source).map_err(Unmerged::Failed)? {
        return Ok(Merge::Unterminated);
    }
    let start = daily.metadata().map_err(Unmerged::Failed)?.len();
    let appended = (|| {
        #[cfg(test)]
        fault::check(fault::Step::Append)?;
        let copied = io::copy(&mut source, daily)?;
        daily.write_all(&trailer(start + copied))?;
        daily.sync_all()
    })();
    if let Err(error) = appended {
        return Err(match rollback_append(daily, start) {
            Ok(()) => Unmerged::Failed(error),
            Err(rollback) => Unmerged::Torn(io::Error::new(
                error.kind(),
                format!("{error}; also failed to roll back partial append: {rollback}"),
            )),
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
/// still holds that file. The lock is released either way, so the next one
/// verifies the file again.
fn merge_into_daily(prefix: &Path, date: Date, sidecar: &Path) -> Result<Option<Merge>, Unmerged> {
    let Some(mut daily) = try_lock_daily(prefix, date).map_err(Unmerged::Failed)? else {
        return Ok(None);
    };
    merge_sidecar(&mut daily, sidecar).map(Some)
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
    /// This run's sidecar with no encoder left: finished and waiting to be
    /// appended, or dropped mid-frame after a failed write, which the switch
    /// leaves on disk since it does not end cleanly. Nothing can be written
    /// until the switch.
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

/// What a merge or reclaim attempt came to. Ordered so that combining
/// attempts keeps the outcome the backoff must see.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Progress {
    /// Nothing to do, or another writer still holds the daily file.
    Idle,
    Done,
    Failed,
}

/// When merges and reclaims may be attempted again.
#[derive(Default)]
struct Backoff {
    failures: u32,
    until: Option<Instant>,
}

impl Backoff {
    fn ready(&self) -> bool {
        self.until.is_none_or(|until| Instant::now() >= until)
    }

    fn record(&mut self, progress: Progress) {
        match progress {
            Progress::Idle => {}
            Progress::Done => *self = Self::default(),
            Progress::Failed => {
                self.until = Some(Instant::now() + retry_delay(self.failures));
                self.failures = self.failures.saturating_add(1);
            }
        }
    }
}

/// The wait after a failure that follows `failures` consecutive ones.
fn retry_delay(failures: u32) -> Duration {
    MERGE_RETRY_BACKOFF
        .saturating_mul(2_u32.saturating_pow(failures))
        .min(MERGE_RETRY_MAX)
}

/// Storage faults that collection survives.
struct Faults {
    /// The first one: collection continues, but the exit is not clean.
    first: Option<String>,
    quality: QualityReporter,
}

impl Faults {
    /// Logs and reports a fault in `target`.
    fn report(&mut self, target: &Path, error: impl fmt::Display, what: &str) {
        error!(path = %target.display(), %error, "{what}");
        self.first
            .get_or_insert_with(|| format!("{}: {error}", target.display()));
        self.quality.report(QualityEvent::StorageDegraded {
            at_ns: QualityEvent::now_ns(),
            target: target.display().to_string(),
            error: error.to_string(),
        });
    }
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
    /// No switch or merge is attempted before a failed one's backoff ends.
    retry: Backoff,
    faults: Faults,
}

impl RotatingFile {
    fn new(
        timestamp: Timestamp,
        prefix: PathBuf,
        run_id: String,
        quality: QualityReporter,
    ) -> io::Result<Self> {
        let day = Day::containing(timestamp)?;
        let mut file = Self {
            prefix,
            run_id,
            day,
            sink: Sink::Closed,
            pending_sidecars: Vec::new(),
            batch: BytesMut::with_capacity(ZSTD_BATCH_BYTES),
            retry: Backoff::default(),
            faults: Faults {
                first: None,
                quality,
            },
        };
        file.sink = file.open_sink(day.date)?;
        Ok(file)
    }

    /// The day's file, or this run's sidecar while another writer holds it or
    /// it cannot be used. Fails only if neither can be opened.
    fn open_sink(&mut self, date: Date) -> io::Result<Sink> {
        let path = day_file(&self.prefix, date, format_args!("_{}", self.run_id));
        match try_lock_daily(&self.prefix, date) {
            Ok(Some(daily)) => return Ok(Sink::Daily(ZstdEncoder::new(daily, ZSTD_LEVEL)?)),
            Ok(None) => warn!(
                path = %path.display(),
                "daily file busy; writing a run-local sidecar until the incumbent releases it"
            ),
            Err(error) => {
                self.daily_unusable(date, &error);
                self.retry.record(Progress::Failed);
            }
        }
        let file = File::options().create_new(true).write(true).open(&path)?;
        lock_exclusively(&file, &path)?;
        Ok(Sink::Sidecar {
            encoder: ZstdEncoder::new(file, ZSTD_LEVEL)?,
            path,
        })
    }

    /// The day's file could not be locked, verified or set aside.
    fn daily_unusable(&mut self, date: Date, error: &io::Error) {
        let daily = day_file(&self.prefix, date, format_args!(""));
        self.faults.report(
            &daily,
            error,
            "cannot use the daily file; records go to a run-local sidecar meanwhile",
        );
    }

    /// The file the open sink writes: the day's file, or this run's sidecar.
    fn sink_path(&self) -> PathBuf {
        match self.sink.sidecar() {
            Some(sidecar) => sidecar.to_path_buf(),
            None => day_file(&self.prefix, self.day.date, format_args!("")),
        }
    }

    fn write(&mut self, timestamp: Timestamp, data: Bytes) -> io::Result<()> {
        if timestamp >= self.day.end {
            self.rotate(timestamp)?;
        }
        if self.retry.ready() {
            let switched = self.switch_to_daily(AfterSwitch::Continue)?;
            let merged = self.merge_pending_sidecars();
            self.retry.record(switched.max(merged));
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
        let switched = self.switch_to_daily(AfterSwitch::Release)?;
        self.retry.record(switched);
        let path = self.sink_path();
        let finalized = self.finalize();
        if let Err(error) = &finalized {
            self.faults.report(
                &path,
                error,
                "failed to finalize file on rotation; continuing with the new file",
            );
        }
        // A finished frame is whole even if its sync failed.
        let whole = !matches!(finalized, Err(FinalizeError::Unfinished(_)));
        match mem::replace(&mut self.sink, Sink::Closed) {
            Sink::Sidecar { path, .. } | Sink::SealedSidecar(path) if whole => {
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
        let sink = self.open_sink(day.date)?;
        self.batch.clear();
        self.sink = sink;
        self.day = day;
        info!(path = %self.prefix.display(), "date changed, file rotated");
        Ok(())
    }

    /// Once the incumbent has released the daily file, append this run's
    /// sidecar to it and delete the sidecar — the only time one is removed.
    /// Failures are reported for the backoff; only opening an encoder on the
    /// daily file fails this.
    fn switch_to_daily(&mut self, after: AfterSwitch) -> io::Result<Progress> {
        let Some(sidecar) = self.sink.sidecar() else {
            return Ok(Progress::Idle);
        };
        let mut daily = match try_lock_daily(&self.prefix, self.day.date) {
            Ok(Some(daily)) => daily,
            Ok(None) => return Ok(Progress::Idle),
            Err(error) => {
                self.daily_unusable(self.day.date, &error);
                return Ok(Progress::Failed);
            }
        };
        let sidecar = sidecar.to_path_buf();
        match self.finalize() {
            Ok(()) => {}
            Err(FinalizeError::Unsynced(error)) => self.faults.report(
                &sidecar,
                error,
                "handover sidecar is finished but not synced; appending it anyway",
            ),
            Err(FinalizeError::Unfinished(error)) => {
                // An unfinished frame must never be appended.
                self.faults.report(
                    &sidecar,
                    error,
                    "failed to finalize handover sidecar; leaving it on disk unfinished",
                );
                // Records the sidecar did not take go to the daily file.
                self.skip_persisted(&sidecar);
                let after = if self.batch.is_empty() {
                    after
                } else {
                    AfterSwitch::Continue
                };
                self.continue_in(daily, after)?;
                return Ok(Progress::Done);
            }
        }
        match merge_sidecar(&mut daily, &sidecar) {
            Ok(Merge::Appended) => {}
            Ok(Merge::Unterminated) => self.sidecar_unterminated(&sidecar),
            Err(unmerged) => {
                self.faults.report(
                    &sidecar,
                    &unmerged,
                    "failed to append overlap sidecar after switch",
                );
                let daily = match unmerged {
                    Unmerged::Failed(_) => Some(daily),
                    // Released: the fresh lock would otherwise be refused as
                    // this process's own.
                    Unmerged::Torn(_) => {
                        drop(daily);
                        None
                    }
                };
                return self.resume_or_queue_sidecar(sidecar, daily, after);
            }
        }
        self.continue_in(daily, after)?;
        Ok(Progress::Done)
    }

    /// After a failed append of a finished sidecar: reopen it, so the day's
    /// records keep going there and a later switch retries the append.
    /// Failing that, retry the append through `daily`, the daily file still
    /// locked here, not through a fresh lock, which would verify it again;
    /// should that fail too, queue the sidecar until this run releases the
    /// daily file. A daily file whose append could not be rolled back is
    /// never written again through the same lock: the sidecar is queued and
    /// the day's file taken afresh, which verifies it.
    ///
    /// `Done` only if the retry appended the sidecar or found it
    /// unterminated; resumed or queued, it is `Failed`, for the backoff.
    fn resume_or_queue_sidecar(
        &mut self,
        sidecar: PathBuf,
        daily: Option<File>,
        after: AfterSwitch,
    ) -> io::Result<Progress> {
        match reopen_sidecar(&sidecar) {
            Ok(encoder) => {
                self.sink = Sink::Sidecar {
                    encoder,
                    path: sidecar,
                };
                return Ok(Progress::Failed);
            }
            Err(error) => self.faults.report(
                &sidecar,
                error,
                "failed to resume overlap sidecar after a failed merge",
            ),
        }
        let daily = match daily {
            Some(mut daily) => match merge_sidecar(&mut daily, &sidecar) {
                Ok(merge) => {
                    if let Merge::Unterminated = merge {
                        self.sidecar_unterminated(&sidecar);
                    }
                    self.continue_in(daily, after)?;
                    return Ok(Progress::Done);
                }
                Err(unmerged) => {
                    self.faults.report(
                        &sidecar,
                        &unmerged,
                        "failed to append overlap sidecar again",
                    );
                    match unmerged {
                        Unmerged::Failed(_) => Some(daily),
                        Unmerged::Torn(_) => {
                            drop(daily);
                            None
                        }
                    }
                }
            },
            None => None,
        };
        self.pending_sidecars.push(PendingSidecar {
            date: self.day.date,
            path: sidecar,
        });
        match daily {
            Some(daily) => self.continue_in(daily, after)?,
            None => self.take_daily_afresh(after)?,
        }
        Ok(Progress::Failed)
    }

    /// After a torn append: the day's file through a fresh lock, which
    /// verifies it and sets it aside if it is torn. Without one nothing is
    /// written until a switch.
    fn take_daily_afresh(&mut self, after: AfterSwitch) -> io::Result<()> {
        self.sink = Sink::Closed;
        match try_lock_daily(&self.prefix, self.day.date) {
            Ok(Some(daily)) => self.continue_in(daily, after),
            Ok(None) => Ok(()),
            Err(error) => {
                self.daily_unusable(self.day.date, &error);
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

    fn merge_pending_sidecars(&mut self) -> Progress {
        let mut progress = Progress::Idle;
        for sidecar in mem::take(&mut self.pending_sidecars) {
            // This run holds that daily file: the sidecar goes in once the
            // file is released, at rotation or close.
            if sidecar.date == self.day.date && matches!(self.sink, Sink::Daily(_)) {
                self.pending_sidecars.push(sidecar);
                continue;
            }
            let step = match merge_into_daily(&self.prefix, sidecar.date, &sidecar.path) {
                Ok(Some(Merge::Appended)) => Progress::Done,
                Ok(Some(Merge::Unterminated)) => {
                    self.sidecar_unterminated(&sidecar.path);
                    Progress::Done
                }
                Ok(None) => {
                    self.pending_sidecars.push(sidecar);
                    Progress::Idle
                }
                Err(unmerged) => {
                    self.faults.report(
                        &sidecar.path,
                        unmerged,
                        "failed to append queued overlap sidecar",
                    );
                    self.pending_sidecars.push(sidecar);
                    Progress::Failed
                }
            };
            progress = progress.max(step);
        }
        progress
    }

    fn sidecar_unterminated(&mut self, sidecar: &Path) {
        self.faults.report(
            sidecar,
            "it does not end on a frame boundary",
            "overlap sidecar left on disk unmerged",
        );
    }

    fn holds_sidecars(&self) -> bool {
        self.sink.sidecar().is_some() || !self.pending_sidecars.is_empty()
    }

    fn flush_batch(&mut self) -> io::Result<()> {
        self.write_batch()?;
        self.batch.clear();
        Ok(())
    }

    /// Hand the batch to the encoder and flush it into the file, so that only
    /// the batch in hand can be partly written when a later step fails.
    fn write_batch(&mut self) -> io::Result<()> {
        if self.batch.is_empty() {
            return Ok(());
        }
        let Some(encoder) = self.sink.encoder() else {
            return Err(no_open_file(&self.prefix));
        };
        let written = encoder.write_all(&self.batch).and_then(|()| {
            #[cfg(test)]
            fault::check(fault::Step::Flush)?;
            encoder.flush()
        });
        if let Err(error) = &written {
            self.abandon_encoder(error);
        }
        written
    }

    /// Drop the encoder after a failed write or flush, unfinished.
    ///
    /// zstd takes input before it reports an earlier write's failure, so the
    /// encoder may already hold part of the batch: fed the batch again, it
    /// would record that part twice, ahead of a valid trailer. Unfinished, the
    /// file ends mid-frame, and the next writer of its day sets it aside,
    /// readable up to where it stops. A sidecar keeps the batch: the switch
    /// writes the records it lacks to the daily file.
    fn abandon_encoder(&mut self, error: &io::Error) {
        match mem::replace(&mut self.sink, Sink::Closed) {
            Sink::Sidecar { path, .. } => {
                self.faults.report(
                    &path,
                    error,
                    "failed to write the overlap sidecar; leaving it on disk unfinished",
                );
                self.sink = Sink::SealedSidecar(path);
            }
            _ => {
                let lost = self.batch.len();
                self.batch.clear();
                let daily = day_file(&self.prefix, self.day.date, format_args!(""));
                self.faults.report(
                    &daily,
                    error,
                    &format!(
                        "failed to write the daily file; leaving it unfinished, {lost} batched bytes lost"
                    ),
                );
            }
        }
    }

    /// Drop from the batch the records an unfinished sidecar already holds.
    /// When that cannot be told, the whole batch is rewritten: a duplicate
    /// over a gap.
    fn skip_persisted(&mut self, sidecar: &Path) {
        if self.batch.is_empty() {
            return;
        }
        let unknown = match persisted_prefix(sidecar, &self.batch) {
            Ok(Some(persisted)) => {
                self.batch.advance(persisted);
                return;
            }
            Ok(None) => "its last record is not in the batch".to_owned(),
            Err(error) => format!("it cannot be decoded: {error}"),
        };
        self.faults.report(
            sidecar,
            unknown,
            &format!(
                "rewriting all {} batched bytes after the unfinished overlap sidecar; \
                 some may be recorded twice",
                self.batch.len()
            ),
        );
    }

    /// Flush the batch, finish the open zstd frame, end the file with its
    /// trailer and fsync it. A finished sidecar stays the sink until it is
    /// appended. The batch is kept until its frame is finished, for
    /// [`Self::skip_persisted`].
    fn finalize(&mut self) -> Result<(), FinalizeError> {
        self.write_batch().map_err(FinalizeError::Unfinished)?;
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
        #[cfg(test)]
        fault::check(fault::Step::Finish).map_err(FinalizeError::Unfinished)?;
        let mut file = encoder.finish().map_err(|error| {
            FinalizeError::Unfinished(io::Error::new(
                error.kind(),
                format!(
                    "failed to finish zstd stream {}: {error}",
                    self.prefix.display()
                ),
            ))
        })?;
        self.batch.clear();
        seal(&mut file, &self.prefix)
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
                error!(%symbol, %error, "failed to switch to the daily file");
                first_error.get_or_insert(error.into());
            }
            let finalized = file.finalize();
            // Released now, the daily file takes the sidecars queued for it.
            file.merge_pending_sidecars();
            if file.holds_sidecars() {
                info!(%symbol, "closing before switch; leaving overlap sidecar on disk");
            }
            match (finalized, &file.faults.first) {
                (Ok(()), Some(fault)) => {
                    error!(%symbol, fault, "file closed, but storage degraded earlier");
                    first_error.get_or_insert(anyhow::anyhow!(
                        "{symbol}: storage degraded earlier: {fault}"
                    ));
                }
                (Ok(()), None) => info!(%symbol, "file closed cleanly"),
                (Err(error), _) => {
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
        Verify,
        SetAside,
        Append,
        Resume,
        Flush,
        Finish,
        Sync,
        Rollback,
    }
    const STEPS: usize = 9;

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

    /// What a file decodes to up to where it stops, whole or torn.
    fn readable(path: &Path) -> String {
        let mut decoder = zstd::stream::read::Decoder::new(File::open(path).unwrap()).unwrap();
        let (mut bytes, mut chunk) = (Vec::new(), vec![0; 64 * 1024]);
        while let Ok(read @ 1..) = decoder.read(&mut chunk) {
            bytes.extend_from_slice(&chunk[..read]);
        }
        String::from_utf8(bytes).unwrap()
    }

    /// `count` distinct records numbered from `first`, about 1 KiB each, so a
    /// little over a hundred fill a batch.
    fn numbered(first: usize, count: usize) -> impl Iterator<Item = Bytes> {
        (first..first + count)
            .map(|n| Bytes::from(format!(r#"{{"n":{n},"pad":"{}"}}"#, "x".repeat(1_000))))
    }

    /// The numbers of the whole numbered records in decoded text.
    fn numbers(text: &str) -> Vec<usize> {
        text.split_inclusive('\n')
            .filter(|line| line.ends_with('\n'))
            .filter_map(|line| {
                let start = line.find(r#""n":"#)? + 4;
                let end = start + line[start..].find(',')?;
                line[start..end].parse().ok()
            })
            .collect()
    }

    /// Writes numbered records until one fails; the number of the one that
    /// failed, which was batched before its flush did.
    fn write_until_failure(writer: &mut Writer, first: usize, count: usize) -> usize {
        numbered(first, count)
            .position(|record| {
                writer
                    .write(day_one(), Symbol::from("btcusdt"), record)
                    .is_err()
            })
            .map(|offset| first + offset)
            .expect("a flush must fail")
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

    /// An append whose rollback failed too may have left the daily file
    /// ending mid-frame: nothing is written to it through that lock again.
    /// The sidecar is resumed, and the next switch takes the daily file
    /// through a fresh lock, which verifies it.
    #[test]
    fn a_daily_file_whose_append_could_not_be_rolled_back_is_not_written_again() {
        let dir = test_dir("merge-torn");
        let mut incumbent = writer(&dir, "incumbent");
        put(&mut incumbent, day_one(), br#"{"n":1}"#);
        let mut challenger = writer(&dir, "challenger");
        put(&mut challenger, day_one(), br#"{"n":2}"#);
        incumbent.close().unwrap();
        fault::arm(fault::Step::Append, 1);
        fault::arm(fault::Step::Rollback, 1);

        put(&mut challenger, day_one(), br#"{"n":3}"#);
        assert!(
            matches!(challenger.files["btcusdt"].sink, Sink::Sidecar { .. }),
            "resumed, not written to the daily file"
        );
        let locks = fault::calls(fault::Step::Lock);
        challenger.files.get_mut("btcusdt").unwrap().retry = Backoff::default();
        put(&mut challenger, day_one(), br#"{"n":4}"#);
        assert_eq!(fault::calls(fault::Step::Lock) - locks, 1, "taken afresh");
        assert!(matches!(challenger.files["btcusdt"].sink, Sink::Daily(_)));
        challenger.close().unwrap_err();
        assert_eq!(names(&dir), [DAY_ONE]);
        let text = decoded(dir.join(DAY_ONE));
        for record in [r#"{"n":1}"#, r#"{"n":2}"#, r#"{"n":3}"#, r#"{"n":4}"#] {
            assert!(text.contains(record), "{record} missing: {text}");
        }
        fs::remove_dir_all(&dir).unwrap();
    }

    /// Torn and not resumable: the sidecar is queued and the day goes on in
    /// the daily file taken afresh, which takes the sidecar at close.
    #[test]
    fn a_torn_append_whose_sidecar_cannot_resume_takes_the_daily_file_afresh() {
        let dir = test_dir("merge-torn-queued");
        let mut incumbent = writer(&dir, "incumbent");
        put(&mut incumbent, day_one(), br#"{"n":1}"#);
        let mut challenger = writer(&dir, "challenger");
        put(&mut challenger, day_one(), br#"{"n":2}"#);
        incumbent.close().unwrap();
        fault::arm(fault::Step::Append, 1);
        fault::arm(fault::Step::Rollback, 1);
        fault::arm(fault::Step::Resume, 1);
        let locks = fault::calls(fault::Step::Lock);

        put(&mut challenger, day_one(), br#"{"n":3}"#);
        assert_eq!(
            fault::calls(fault::Step::Lock) - locks,
            2,
            "locked to switch, then afresh after the torn append"
        );
        let file = &challenger.files["btcusdt"];
        assert!(matches!(file.sink, Sink::Daily(_)));
        assert_eq!(file.pending_sidecars.len(), 1);
        challenger.close().unwrap_err();
        assert_eq!(names(&dir), [DAY_ONE]);
        let text = decoded(dir.join(DAY_ONE));
        for record in [r#"{"n":1}"#, r#"{"n":2}"#, r#"{"n":3}"#] {
            assert!(text.contains(record), "{record} missing: {text}");
        }
        fs::remove_dir_all(&dir).unwrap();
    }

    /// A resumed or queued sidecar backs the switch off; one that the retry
    /// through the held lock appended does not.
    #[test]
    fn only_a_sidecar_left_behind_backs_the_switch_off() {
        for (resume_fails, expect) in [(false, Progress::Failed), (true, Progress::Done)] {
            let dir = test_dir("merge-progress");
            let mut incumbent = writer(&dir, "incumbent");
            put(&mut incumbent, day_one(), br#"{"n":1}"#);
            let mut challenger = writer(&dir, "challenger");
            put(&mut challenger, day_one(), br#"{"n":2}"#);
            incumbent.close().unwrap();
            fault::arm(fault::Step::Append, 1);
            if resume_fails {
                fault::arm(fault::Step::Resume, 1);
            }

            let file = challenger.files.get_mut("btcusdt").unwrap();
            assert_eq!(file.switch_to_daily(AfterSwitch::Continue).unwrap(), expect);
            challenger.close().unwrap_err();
            fs::remove_dir_all(&dir).unwrap();
        }
    }

    #[test]
    fn a_queued_sidecar_goes_in_once_the_daily_file_is_released() {
        let dir = test_dir("merge-queued");
        let mut incumbent = writer(&dir, "incumbent");
        put(&mut incumbent, day_one(), br#"{"n":1}"#);
        let mut challenger = writer(&dir, "challenger");
        put(&mut challenger, day_one(), br#"{"n":2}"#);
        incumbent.close().unwrap();
        fault::arm(fault::Step::Append, 2);
        fault::arm(fault::Step::Resume, 1);

        put(&mut challenger, day_one(), br#"{"n":3}"#);
        let file = &challenger.files["btcusdt"];
        assert!(matches!(file.sink, Sink::Daily(_)));
        assert_eq!(file.pending_sidecars.len(), 1);
        put(&mut challenger, day_one(), br#"{"n":4}"#);
        challenger.close().unwrap_err();
        assert_eq!(names(&dir), [DAY_ONE]);
        let text = decoded(dir.join(DAY_ONE));
        for record in [r#"{"n":1}"#, r#"{"n":2}"#, r#"{"n":3}"#, r#"{"n":4}"#] {
            assert!(text.contains(record), "{record} missing: {text}");
        }
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn an_unverifiable_daily_file_is_set_aside() {
        let dir = test_dir("unverifiable");
        let daily = dir.join(DAY_ONE);
        let earlier = zstd::encode_all(&b"earlier"[..], 1).unwrap();
        fs::write(&daily, &earlier).unwrap();
        fault::arm(fault::Step::Verify, 1);

        let mut writer = writer(&dir, "restart");
        put(&mut writer, day_one(), b"later");
        writer.close().unwrap();

        assert_eq!(decoded(&daily), format!("{DAY_ONE_NS} later\n"));
        let names = names(&dir);
        let salvage = names
            .iter()
            .find(|name| name.starts_with("btcusdt_20270115_unterminated-"))
            .unwrap_or_else(|| panic!("the unverified file must be kept aside: {names:?}"));
        assert_eq!(fs::read(dir.join(salvage)).unwrap(), earlier);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_daily_file_that_cannot_be_set_aside_falls_back_to_a_sidecar() {
        let dir = test_dir("unmovable");
        let daily = dir.join(DAY_ONE);
        let crashed = zstd::encode_all(&b"before-crash"[..], 1).unwrap();
        fs::write(&daily, &crashed[..crashed.len() - 2]).unwrap();
        fault::arm(fault::Step::SetAside, 1);
        let (quality, events) = QualityReporter::test_channel();
        let mut writer = Writer::new(&dir, "restart", quality);

        put(&mut writer, day_one(), b"during");
        assert!(matches!(
            events.try_recv(),
            Ok(QualityEvent::StorageDegraded { target, .. }) if target == daily.display().to_string()
        ));
        assert!(matches!(writer.files["btcusdt"].sink, Sink::Sidecar { .. }));
        put(&mut writer, day_one(), b"backing-off");
        assert!(has_sidecar(&dir, "restart"), "no retry before the backoff");

        writer.files.get_mut("btcusdt").unwrap().retry = Backoff::default();
        put(&mut writer, day_one(), b"after");
        writer.close().unwrap_err();
        assert!(!has_sidecar(&dir, "restart"));
        let text = decoded(&daily);
        for record in ["during", "backing-off", "after"] {
            assert!(text.contains(record), "{record} missing: {text}");
        }
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn failed_merges_back_off_exponentially_until_a_success() {
        let delays: Vec<u64> = (0..9).map(|n| retry_delay(n).as_secs()).collect();
        assert_eq!(delays, [5, 10, 20, 40, 80, 160, 320, 600, 600]);
        assert_eq!(retry_delay(u32::MAX), MERGE_RETRY_MAX);

        let mut backoff = Backoff::default();
        assert!(backoff.ready());
        backoff.record(Progress::Failed);
        backoff.record(Progress::Failed);
        assert!(!backoff.ready());
        assert_eq!(backoff.failures, 2);
        // A daily file still held elsewhere is neither failure nor success.
        backoff.record(Progress::Idle);
        assert_eq!(backoff.failures, 2);
        backoff.record(Progress::Done);
        assert!(backoff.ready());
        assert_eq!(backoff.failures, 0);
    }

    #[test]
    fn a_finished_sidecar_whose_sync_failed_is_still_appended() {
        let dir = test_dir("unsynced-switch");
        let mut incumbent = writer(&dir, "incumbent");
        put(&mut incumbent, day_one(), br#"{"n":1}"#);
        let (quality, events) = QualityReporter::test_channel();
        let mut challenger = Writer::new(&dir, "challenger", quality);
        put(&mut challenger, day_one(), br#"{"n":2}"#);
        incumbent.close().unwrap();
        fault::arm(fault::Step::Sync, 1);

        put(&mut challenger, day_one(), br#"{"n":3}"#);
        assert!(matches!(
            events.try_recv(),
            Ok(QualityEvent::StorageDegraded { error, .. }) if error.contains("failed to sync")
        ));
        assert_eq!(names(&dir), [DAY_ONE], "the whole sidecar must be appended");
        challenger.close().unwrap_err();
        let text = decoded(dir.join(DAY_ONE));
        for record in [r#"{"n":1}"#, r#"{"n":2}"#, r#"{"n":3}"#] {
            assert!(text.contains(record), "{record} missing: {text}");
        }
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn an_unsynced_sidecar_sealed_at_midnight_is_queued() {
        let dir = test_dir("unsynced-midnight");
        let mut incumbent = writer(&dir, "incumbent");
        put(&mut incumbent, day_one(), br#"{"n":1}"#);
        let mut challenger = writer(&dir, "challenger");
        put(&mut challenger, day_one(), br#"{"n":2}"#);
        fault::arm(fault::Step::Sync, 1);
        put(&mut challenger, day_two(), br#"{"n":3}"#);
        assert_eq!(challenger.files["btcusdt"].pending_sidecars.len(), 1);

        incumbent.close().unwrap();
        challenger.close().unwrap_err();
        assert_eq!(names(&dir), [DAY_ONE, DAY_TWO]);
        assert!(decoded(dir.join(DAY_ONE)).contains(r#"{"n":2}"#));
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn an_unfinished_sidecar_is_left_on_disk() {
        let dir = test_dir("unfinished-switch");
        let mut incumbent = writer(&dir, "incumbent");
        put(&mut incumbent, day_one(), br#"{"n":1}"#);
        let mut challenger = writer(&dir, "challenger");
        put(&mut challenger, day_one(), br#"{"n":2}"#);
        incumbent.close().unwrap();
        fault::arm(fault::Step::Finish, 1);

        put(&mut challenger, day_one(), br#"{"n":3}"#);
        assert!(matches!(challenger.files["btcusdt"].sink, Sink::Daily(_)));
        challenger.close().unwrap_err();
        assert!(has_sidecar(&dir, "challenger"), "it must never be appended");
        let text = decoded(dir.join(DAY_ONE));
        assert!(text.contains(r#"{"n":3}"#), "{text}");
        assert!(
            !text.contains(r#"{"n":2}"#),
            "the record the sidecar already holds must not be rewritten: {text}"
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    /// zstd takes input before it reports a failed write, so an encoder whose
    /// flush failed may hold the batch already: fed it again at close, it
    /// would record the batch twice behind a valid trailer.
    #[test]
    fn a_failed_flush_drops_the_frame_instead_of_writing_the_batch_twice() {
        let dir = test_dir("failed-flush");
        let mut first = writer(&dir, "first");
        for record in numbered(0, 130) {
            first
                .write(day_one(), Symbol::from("btcusdt"), record)
                .unwrap();
        }
        fault::arm(fault::Step::Flush, 1);
        write_until_failure(&mut first, 130, 200);
        assert!(
            first.close().is_err(),
            "a dropped frame is not a clean close"
        );

        let daily = dir.join(DAY_ONE);
        assert!(!ends_cleanly(&File::open(&daily).unwrap()).unwrap());
        let kept = numbers(&readable(&daily));
        assert_eq!(kept, (0..kept.len()).collect::<Vec<_>>(), "no record twice");
        assert!(!kept.is_empty(), "the first flushed batch stays readable");

        // The next writer of the day sets the torn file aside.
        let mut next = writer(&dir, "next");
        put(&mut next, day_one(), b"after");
        next.close().unwrap();
        assert_eq!(decoded(&daily), format!("{DAY_ONE_NS} after\n"));
        assert!(
            names(&dir)
                .iter()
                .any(|name| name.starts_with("btcusdt_20270115_unterminated-"))
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    /// The same for a sidecar: it stays on disk unfinished, and the switch
    /// writes the records it lacks to the daily file, once.
    #[test]
    fn a_sidecar_whose_flush_failed_is_left_and_its_batch_switched() {
        let dir = test_dir("failed-sidecar-flush");
        let mut incumbent = writer(&dir, "incumbent");
        put(&mut incumbent, day_one(), br#"{"incumbent":1}"#);
        let mut challenger = writer(&dir, "challenger");
        for record in numbered(0, 130) {
            challenger
                .write(day_one(), Symbol::from("btcusdt"), record)
                .unwrap();
        }
        fault::arm(fault::Step::Flush, 1);
        let last = write_until_failure(&mut challenger, 130, 200);
        incumbent.close().unwrap();
        assert!(challenger.close().is_err());

        let sidecar = dir.join("btcusdt_20270115_challenger.zst");
        assert!(
            !ends_cleanly(&File::open(&sidecar).unwrap()).unwrap(),
            "the torn sidecar must never be appended"
        );
        let mut recorded = numbers(&readable(&sidecar));
        recorded.extend(numbers(&decoded(dir.join(DAY_ONE))));
        recorded.sort_unstable();
        assert_eq!(recorded, (0..=last).collect::<Vec<_>>(), "each record once");
        fs::remove_dir_all(&dir).unwrap();
    }

    /// A sidecar that ends mid-frame after `persisted` of `batch`: earlier
    /// records in whole flushed blocks, then a torn block.
    fn torn_sidecar(path: &Path, earlier: &[u8], batch: &[u8], persisted: usize) {
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 1).unwrap();
        encoder.write_all(earlier).unwrap();
        encoder.flush().unwrap();
        encoder.write_all(&batch[..persisted]).unwrap();
        encoder.flush().unwrap();
        let flushed = encoder.get_ref().len();
        encoder.write_all(&batch[persisted..]).unwrap();
        encoder.flush().unwrap();
        let mut bytes = encoder.finish().unwrap();
        // Part of the unflushed block's header only.
        bytes.truncate(flushed + 2);
        fs::write(path, bytes).unwrap();
    }

    #[test]
    fn only_records_an_unfinished_sidecar_lacks_are_rewritten() {
        let dir = test_dir("persisted");
        let path = dir.join("torn.zst");
        let earlier = b"1 {\"n\":1}\n2 {\"n\":2}\n".as_slice();
        let batch = b"3 {\"n\":3}\n4 {\"n\":4}\n5 {\"n\":5}\n".as_slice();
        const RECORD: usize = 10;
        let mut file = RotatingFile::new(
            day_one(),
            dir.join("btcusdt"),
            "run".to_owned(),
            QualityReporter::disabled(),
        )
        .unwrap();
        let mut rewrite = |path: &Path| {
            file.batch = BytesMut::from(batch);
            file.skip_persisted(path);
            file.batch.to_vec()
        };

        for (persisted, kept) in [
            (2 * RECORD, 2 * RECORD),
            // A block boundary inside a record: that record is rewritten.
            (RECORD + 5, RECORD),
            (0, 0),
        ] {
            torn_sidecar(&path, earlier, batch, persisted);
            assert_eq!(
                rewrite(&path),
                &batch[kept..],
                "{persisted} bytes persisted"
            );
        }
        assert_eq!(persisted_prefix(&path, batch).unwrap(), None);

        fs::write(&path, b"not zstd at all").unwrap();
        assert!(persisted_prefix(&path, batch).is_err());
        assert_eq!(rewrite(&path), batch, "an undecodable sidecar rewrites all");
        drop(file);
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

        let daily = dir.join(DAY_ONE).display().to_string();
        assert!(
            matches!(
                events.try_recv(),
                Ok(QualityEvent::StorageDegraded { target, error, .. })
                    if target == daily && error.contains("failed to sync")
            ),
            "a failed fsync must be reported against the file it failed on"
        );
        let error = writer.close().unwrap_err().to_string();
        assert!(
            error.contains("storage degraded earlier") && error.contains(&daily),
            "close must not report a clean exit, and says why: {error}"
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
