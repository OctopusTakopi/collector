//! A Binance combined stream on the slot supervisor: the stream list is in
//! the URL, so a session subscribes nothing, and is replaced before Binance
//! cuts it at 24 hours or when Binance announces a server shutdown.

use std::time::Duration;

use hyper::header::HeaderMap;
use tracing::warn;

use super::Endpoint;
use crate::{
    routing,
    session::{Handshake, Protocol, Reason, Requests, Session, Socket, Verdict},
};

/// When a session starts arranging its replacement. Binance: "a single
/// connection is only valid for 24 hours; expect to be disconnected at the 24
/// hour mark", on spot and both futures markets. The margin covers the idle
/// timeout bounding how late the check runs.
const MAX_SESSION_AGE: Duration = Duration::from_secs(23 * 60 * 60);

/// How far apart redundant connections place their age-driven handovers, so
/// connections opened staggered do not all hand over together a day later.
const SESSION_AGE_STAGGER: Duration = Duration::from_secs(15 * 60);

/// `{"e":"serverShutdown","E":...}`, pushed before Binance closes a
/// connection. The lead time is unspecified: the documented ten minutes was
/// withdrawn on 2026-06-09 in favour of "reconnect as soon as possible".
const SERVER_SHUTDOWN: &str = "serverShutdown";

/// Longest frame tested for the announcement. Market data is text here, so
/// the test is on the hot path; the announcement is well under a hundred
/// bytes, so a length check rejects nearly every market frame.
const SHUTDOWN_PROBE_LIMIT: usize = 256;

fn max_session_age(connection: usize) -> Duration {
    MAX_SESSION_AGE.saturating_sub(SESSION_AGE_STAGGER * connection as u32)
}

/// True if `payload` is the shutdown announcement, bare or in a combined-stream
/// envelope. Parsed after the cheap tests, so market data that merely quotes
/// the word cannot trigger a handover.
fn is_server_shutdown(payload: &[u8]) -> bool {
    if payload.len() > SHUTDOWN_PROBE_LIMIT
        || !routing::contains(payload, SERVER_SHUTDOWN.as_bytes())
    {
        return false;
    }
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(payload) else {
        return false;
    };
    let event = value.get("data").unwrap_or(&value);
    event.get("e").and_then(serde_json::Value::as_str) == Some(SERVER_SHUTDOWN)
}

/// One endpoint's combined stream for every symbol.
pub(super) struct Stream {
    pub(super) endpoint: &'static Endpoint,
    pub(super) url: String,
}

impl Protocol for Stream {
    type Session = Announcements;

    fn label(&self) -> &'static str {
        self.endpoint.label
    }

    fn handshake(&self) -> anyhow::Result<Handshake<'_>> {
        Ok(Handshake {
            url: &self.url,
            headers: HeaderMap::new(),
        })
    }

    fn idle_timeout(&self) -> Duration {
        self.endpoint.idle_timeout
    }

    fn max_age(&self, connection: usize) -> Option<Duration> {
        Some(max_session_age(connection))
    }

    fn open(&self, socket: Socket) -> (Announcements, Requests) {
        let announcements = Announcements {
            label: self.endpoint.label,
            socket,
        };
        (announcements, Requests::default())
    }
}

/// The only protocol frame a Binance stream session expects: its shutdown
/// notice. Everything else is market data, the replies to nothing included.
pub(super) struct Announcements {
    label: &'static str,
    socket: Socket,
}

impl Session for Announcements {
    fn is_control(&self, text: &[u8]) -> bool {
        is_server_shutdown(text)
    }

    fn on_control(&mut self, text: &[u8]) -> Verdict {
        warn!(
            endpoint = self.label,
            connection = self.socket.connection,
            session = self.socket.session,
            text = %String::from_utf8_lossy(text),
            "the venue announced a server shutdown"
        );
        Verdict::Relieve(Reason::ServerShutdown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ANNOUNCEMENT: &str = r#"{"e":"serverShutdown","E":1770123456789}"#;

    fn announcements() -> Announcements {
        Announcements {
            label: "test",
            socket: Socket {
                connection: 0,
                session: 0,
            },
        }
    }

    #[test]
    fn redundant_connections_hand_over_at_different_times() {
        assert_eq!(max_session_age(0), MAX_SESSION_AGE);
        assert_eq!(max_session_age(1), MAX_SESSION_AGE - SESSION_AGE_STAGGER);
        // Even the last of the eight the CLI allows stays well inside 24 h.
        assert!(max_session_age(7) > Duration::from_secs(20 * 60 * 60));
    }

    #[test]
    fn the_announcement_is_recognised() {
        assert!(is_server_shutdown(ANNOUNCEMENT.as_bytes()));
        assert!(is_server_shutdown(
            br#"{"stream":"btcusdt@depth","data":{"e":"serverShutdown","E":1}}"#
        ));
    }

    /// Only the event type counts: a false positive opens a connection per
    /// frame.
    #[test]
    fn only_the_event_type_counts() {
        assert!(!is_server_shutdown(
            br#"{"code":-1130,"msg":"serverShutdown is not a valid stream"}"#
        ));
        assert!(!is_server_shutdown(
            br#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1}}"#
        ));
        assert!(!is_server_shutdown(b"serverShutdown"));
        assert!(!is_server_shutdown(b"not json at all"));

        let padded = format!(
            r#"{{"e":"serverShutdown","E":1,"pad":"{}"}}"#,
            "x".repeat(SHUTDOWN_PROBE_LIMIT)
        );
        assert!(!is_server_shutdown(padded.as_bytes()));
    }

    /// The announcement is protocol, never recorded, and asks for a
    /// replacement; market data and replies are recorded as they come.
    #[test]
    fn the_announcement_asks_for_a_replacement_and_nothing_else_does() {
        let mut session = announcements();
        assert!(session.is_control(ANNOUNCEMENT.as_bytes()));
        assert!(matches!(
            session.on_control(ANNOUNCEMENT.as_bytes()),
            Verdict::Relieve(Reason::ServerShutdown)
        ));
        for data in [
            br#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1,"s":"BTCUSDT"}}"#.as_slice(),
            br#"{"result":null,"id":1}"#,
        ] {
            assert!(!session.is_control(data));
        }
        assert!(session.settled() && session.complete());
    }
}
