//! The file naming a directory's exchange. Every collector writes to a
//! directory of its own: the first to hold its handover role names its
//! exchange in the marker, and one for another exchange is refused, so two
//! venues, or the JSON and the SBE collectors, never append to one symbol's
//! file. The tools read a directory's venue from the marker instead of
//! guessing it from the path.

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
    /// No marker, or an empty one: an older binary's claim still being
    /// written, or one a crash cut short.
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
    /// Refuses `dir` if its marker names anything else. Only reads: an
    /// unmarked directory is marked by [`Self::claim`], once this process is
    /// its incumbent.
    pub fn check_marker(self, dir: &Path) -> io::Result<()> {
        self.marked_for(dir)?;
        Ok(())
    }

    /// Marks `dir` as this exchange's, or confirms it already is. Refuses a
    /// directory marked for anything else.
    pub fn claim(self, dir: &Path) -> io::Result<()> {
        if self.marked_for(dir)? {
            return Ok(());
        }
        let staged = dir.join(format!(
            ".{MARKER}.{}.{}",
            std::process::id(),
            self.as_str()
        ));
        let linked = self.link_staged(dir, &staged);
        // Gone already where it replaced an empty marker; a leftover only
        // litters.
        let _ = std::fs::remove_file(&staged);
        linked?;
        // Persist the directory entry too.
        File::open(dir)?.sync_all()
    }

    /// Whether `dir` is marked for this exchange already; an error if it is
    /// marked for anything else.
    fn marked_for(self, dir: &Path) -> io::Result<bool> {
        match Marked::read(dir)? {
            Marked::Unmarked => Ok(false),
            Marked::Exchange(marked) if marked == self => Ok(true),
            Marked::Exchange(marked) => Err(refused(dir, marked.as_str(), self)),
            Marked::Foreign(name) => Err(refused(dir, &name, self)),
        }
    }

    /// Writes the name whole to `staged`, then links it into place as the
    /// marker: a reader never sees it half written, and of claims racing,
    /// only one links.
    fn link_staged(self, dir: &Path, staged: &Path) -> io::Result<()> {
        let mut file = File::create(staged)?;
        writeln!(file, "{}", self.as_str())?;
        file.sync_all()?;
        let path = dir.join(MARKER);
        match std::fs::hard_link(staged, &path) {
            // Claimed meanwhile, or empty: a claim an older binary's crash
            // cut short, replaced whole.
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                if self.marked_for(dir)? {
                    Ok(())
                } else {
                    std::fs::rename(staged, &path)
                }
            }
            linked => linked,
        }
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
        let names: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(names, [MARKER], "nothing staged is left behind");
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

    /// The check up front only reads: it refuses a directory marked for
    /// another exchange, and leaves an unmarked one, or one not made yet,
    /// unmarked.
    #[test]
    fn the_check_refuses_another_exchange_and_writes_nothing() {
        let dir = test_dir("check");
        Exchange::Coinbase.check_marker(&dir).unwrap();
        Exchange::Coinbase
            .check_marker(&dir.join("not-yet"))
            .unwrap();
        assert_eq!(Marked::read(&dir).unwrap(), Marked::Unmarked);
        assert!(!dir.join("not-yet").exists());

        Exchange::KrakenSpot.claim(&dir).unwrap();
        Exchange::KrakenSpot.check_marker(&dir).unwrap();
        let error = Exchange::Coinbase.check_marker(&dir).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "{} holds krakenspot recordings, not coinbase",
                dir.display()
            )
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Two exchanges claiming one directory at once: exactly one wins, and
    /// the marker it leaves is whole.
    #[test]
    fn of_two_exchanges_claiming_at_once_only_one_wins() {
        for round in 0..10 {
            let dir = test_dir(&format!("race-{round}"));
            let barrier = std::sync::Barrier::new(2);
            let (dir_ref, barrier) = (&dir, &barrier);
            let won = std::thread::scope(|scope| {
                [Exchange::KrakenSpot, Exchange::Coinbase]
                    .map(|exchange| {
                        scope.spawn(move || {
                            barrier.wait();
                            exchange.claim(dir_ref).is_ok()
                        })
                    })
                    .map(|claim| claim.join().unwrap())
            });
            let marker = std::fs::read_to_string(dir.join(MARKER)).unwrap();
            match won {
                [true, false] => assert_eq!(marker, "krakenspot\n"),
                [false, true] => assert_eq!(marker, "coinbase\n"),
                both => panic!("round {round}: {both:?}, marker {marker:?}"),
            }
            std::fs::remove_dir_all(dir).unwrap();
        }
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
