use std::{collections::HashMap, fs::File, io, io::Write};

use bytes::BufMut;
use jiff::Timestamp;
use tracing::{error, info, warn};
use zstd::stream::write::Encoder as ZstdEncoder;

pub struct RotatingFile {
    next_rotation: i64,
    path: String,
    file: Option<ZstdEncoder<'static, File>>,
    buf: bytes::BytesMut,
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
        })
    }

    pub fn finalize(&mut self) -> io::Result<()> {
        let Some(encoder) = self.file.take() else {
            return Ok(());
        };

        match encoder.finish() {
            Ok(file) => file.sync_all(),
            Err(error) => Err(error),
        }
    }

    pub fn write(&mut self, timestamp: Timestamp, data: bytes::Bytes) -> Result<(), io::Error> {
        let ts_nanos = timestamp.as_nanosecond();
        if ts_nanos >= self.next_rotation as i128 {
            if let Err(error) = self.finalize() {
                error!(path = %self.path, %error, "failed to finalize file on rotation");
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

        self.file.as_mut().unwrap().write_all(&self.buf)
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
    files: HashMap<String, RotatingFile>,
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
        symbol: &str,
        data: bytes::Bytes,
    ) -> Result<(), anyhow::Error> {
        if let Some(rotating_file) = self.files.get_mut(symbol) {
            rotating_file.write(recv_time, data)?;
        } else {
            let symbol_lower = symbol.to_lowercase();
            let path = format!("{}/{}", self.path, symbol_lower);
            let mut rotating_file = RotatingFile::new(recv_time, path)?;
            rotating_file.write(recv_time, data)?;
            self.files.insert(symbol.to_string(), rotating_file);
        }
        Ok(())
    }

    pub fn close(&mut self) {
        for (symbol, file) in &mut self.files {
            match file.finalize() {
                Ok(()) => info!(symbol = %symbol, "file closed cleanly"),
                Err(error) => error!(symbol = %symbol, %error, "failed to close file"),
            }
        }
        self.files.clear();
    }
}
