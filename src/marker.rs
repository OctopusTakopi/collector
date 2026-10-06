//! The file naming a directory's exchange. Every collector writes to a
//! directory of its own: the first to start there names its exchange in the
//! marker, and one for another exchange is refused, so two venues, or the
//! JSON and the SBE collectors, never append to one symbol's file. The tools
//! read a directory's venue from the marker instead of guessing it from the
//! path.

use std::{
    fs::File,
    io::{self, Read, Write},
    path::Path,
};

use clap::ValueEnum;

use crate::collection::Exchange;

/// One line, the exchange's canonical command-line name. sbe-collector
/// writes the same file with its own names.
pub const MARKER: &str = "_exchange";

/// What a directory's marker says.
#[derive(Debug, PartialEq, Eq)]
pub enum Marked {
    /// No marker, or an empty one: a claim still being written, or one a
    /// crash cut short.
    Unmarked,
    Exchange(Exchange),
    /// An exchange this collector does not record, such as sbe-collector's
    /// `binancesbespot`.
    Foreign(String),
}

impl Marked {
    pub fn read(dir: &Path) -> io::Result<Self> {
        let mut name = String::new();
        match File::open(dir.join(MARKER)) {
            Ok(mut file) => file.read_to_string(&mut name)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Self::Unmarked),
            Err(error) => return Err(error),
        };
        let name = name.trim();
        Ok(if name.is_empty() {
            Self::Unmarked
        } else {
            Exchange::from_str(name, false)
                .map_or_else(|_| Self::Foreign(name.to_owned()), Self::Exchange)
        })
    }
}

impl Exchange {
    /// Marks `dir` as this exchange's, or confirms it already is. Refuses a
    /// directory marked for anything else.
    pub fn claim(self, dir: &Path) -> io::Result<()> {
        let path = dir.join(MARKER);
        let mut file = match File::options().write(true).create_new(true).open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                match Marked::read(dir)? {
                    Marked::Exchange(marked) if marked == self => return Ok(()),
                    Marked::Exchange(marked) => return Err(refused(dir, marked.as_str(), self)),
                    Marked::Foreign(name) => return Err(refused(dir, &name, self)),
                    // Empty: finish the claim. A concurrent one, a handover's
                    // other process, writes the same line.
                    Marked::Unmarked => File::options().write(true).truncate(true).open(&path)?,
                }
            }
            Err(error) => return Err(error),
        };
        writeln!(file, "{}", self.as_str())?;
        file.sync_all()?;
        // Persist the directory entry too.
        File::open(dir)?.sync_all()
    }
}

fn refused(dir: &Path, marked: &str, exchange: Exchange) -> io::Error {
    io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!(
            "{} holds {marked} recordings, not {}",
            dir.display(),
            exchange.as_str()
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "collector-marker-{tag}-{}-{}",
            std::process::id(),
            jiff::Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn a_claim_marks_the_directory_and_is_confirmed_again() {
        let dir = test_dir("claim");
        assert_eq!(Marked::read(&dir).unwrap(), Marked::Unmarked);

        Exchange::BinanceFuturesUm.claim(&dir).unwrap();
        Exchange::BinanceFuturesUm.claim(&dir).unwrap();

        assert_eq!(
            std::fs::read_to_string(dir.join(MARKER)).unwrap(),
            "binancefuturesum\n",
            "the canonical name, whatever alias was typed"
        );
        assert_eq!(
            Marked::read(&dir).unwrap(),
            Marked::Exchange(Exchange::BinanceFuturesUm)
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_directory_marked_for_another_exchange_is_refused() {
        let dir = test_dir("other");
        Exchange::KrakenSpot.claim(&dir).unwrap();

        let error = Exchange::KrakenFutures.claim(&dir).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "{} holds krakenspot recordings, not krakenfutures",
                dir.display()
            )
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// sbe-collector writes the same marker: its directories are foreign
    /// here, and refused.
    #[test]
    fn an_sbe_collector_directory_is_foreign_and_refused() {
        let dir = test_dir("foreign");
        std::fs::write(dir.join(MARKER), "binancesbespot\n").unwrap();

        assert_eq!(
            Marked::read(&dir).unwrap(),
            Marked::Foreign("binancesbespot".to_owned())
        );
        let error = Exchange::BinanceSpot.claim(&dir).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("holds binancesbespot recordings"),
            "{error}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A claim a crash cut short is finished by the next one.
    #[test]
    fn an_empty_marker_is_completed() {
        let dir = test_dir("empty");
        std::fs::write(dir.join(MARKER), "").unwrap();
        assert_eq!(Marked::read(&dir).unwrap(), Marked::Unmarked);

        Exchange::Bybit.claim(&dir).unwrap();

        assert_eq!(
            Marked::read(&dir).unwrap(),
            Marked::Exchange(Exchange::Bybit)
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
}
