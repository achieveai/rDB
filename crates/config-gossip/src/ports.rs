//! Where a listener that asked for "any port" (port `0`) actually lands.
//!
//! Unset, nothing changes: a TCP listener binds port `0` and the OS picks, and gossip draws
//! random candidates from [`DYNAMIC_PORTS`]. With [`PORT_RANGE_ENV`] set to `LO-HI`, every
//! port-`0` bind draws random candidates from that range instead, and retries any refused bind
//! up to [`EPHEMERAL_BIND_ATTEMPTS`] times. A fixed (non-zero) port is never moved.
//!
//! Why it exists: observed 2026-10-01, two IIS worker processes on the build host held about
//! 15,700 of the 16,384 ports in Windows' dynamic pool in state `Bound`. A port-`0` TCP bind
//! then failed with `os error 10055` and gossip with `10013`, while ports below the pool bound
//! fine. Tests still ask for "any port" (test plan §6 rule 4); this module decides where.
//!
//! It lives in `config-gossip` because gossip already owned the candidate loop, and both
//! `config-server` and `config-testkit` already depend on this crate. `config-core` is ruled
//! out: it holds no network and no ambient inputs (ADR-0004).

use std::fmt;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::ops::RangeInclusive;

use tokio::net::TcpListener;
use tracing::debug;

/// The environment variable naming the range port-`0` binds draw from, as `LO-HI`.
pub const PORT_RANGE_ENV: &str = "RETCD_TEST_PORT_RANGE";

/// The logger the bind events go to. Not `config_gossip::*`: ADR-0013 requires `node_id` on
/// every `config_gossip*` line, and this helper also runs where no node exists yet (the
/// testkit reserving ports before a cluster starts). Its events carry the caller's span, so a
/// bind made inside a node span (gossip, the daemon) still has that node's fields.
const LOG_TARGET: &str = "retcd_ports";

/// How many candidate ports a port-`0` bind tries before giving up.
///
/// Candidates are random, not left to the OS. Windows hands out ephemeral ports from a rotating
/// pointer, so OS picks made moments apart are neighbours. On 2026-09-30 three daemons failed
/// on ports 55483-55485 after 8 OS picks, all inside one 100-port block of Windows' UDP
/// exclusions (`os error 10013`). Random draws are independent: with a few hundred refused
/// ports in the range, 32 refusals in a row does not happen by chance.
pub const EPHEMERAL_BIND_ATTEMPTS: u32 = 32;

/// Where gossip draws ephemeral candidates when [`PORT_RANGE_ENV`] is unset: the IANA dynamic
/// range (RFC 6335), which is also Windows' default ephemeral range.
pub const DYNAMIC_PORTS: RangeInclusive<u16> = 49152..=65535;

/// Parse `LO-HI` into a port range. `LO` must be at least 1 and not above `HI`.
///
/// # Errors
///
/// A message naming [`PORT_RANGE_ENV`] and the rejected text.
pub fn parse_port_range(text: &str) -> Result<RangeInclusive<u16>, String> {
    let bad = |why: &str| format!("{PORT_RANGE_ENV}={text:?}: {why}; expected LO-HI");
    let (lo, hi) = text.trim().split_once('-').ok_or_else(|| bad("no `-`"))?;
    let lo: u16 = lo.trim().parse().map_err(|_| bad("LO is not a port"))?;
    let hi: u16 = hi.trim().parse().map_err(|_| bad("HI is not a port"))?;
    if lo == 0 {
        return Err(bad("LO must be at least 1"));
    }
    if lo > hi {
        return Err(bad("LO is above HI"));
    }
    Ok(lo..=hi)
}

/// The range [`PORT_RANGE_ENV`] names, or `None` when it is unset or blank.
///
/// # Errors
///
/// The variable is set to something [`parse_port_range`] rejects, or is not Unicode.
pub fn port_range_from_env() -> Result<Option<RangeInclusive<u16>>, String> {
    match std::env::var(PORT_RANGE_ENV) {
        Ok(value) => range_from_value(Some(&value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(e) => Err(format!("{PORT_RANGE_ENV}: {e}")),
    }
}

/// [`port_range_from_env`] on a value already read, so the rule is testable without touching
/// the process environment.
fn range_from_value(value: Option<&str>) -> Result<Option<RangeInclusive<u16>>, String> {
    match value.map(str::trim) {
        None | Some("") => Ok(None),
        Some(text) => parse_port_range(text).map(Some),
    }
}

/// One uniformly drawn port from `range`.
///
/// `RandomState` is std's per-process random seed, advanced on every call, so this needs no
/// dependency for what is not a security decision.
pub fn random_port_in(range: &RangeInclusive<u16>) -> u16 {
    use std::hash::{BuildHasher, Hasher};
    let draw = std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish();
    let width = u64::from(range.end() - range.start()) + 1;
    let offset = u16::try_from(draw % width).expect("an offset below the range width fits u16");
    range.start() + offset
}

/// Bind a TCP listener on `addr`.
///
/// A non-zero port, or port `0` with [`PORT_RANGE_ENV`] unset, is one plain
/// `TcpListener::bind(addr)`. Port `0` with the range set tries up to
/// [`EPHEMERAL_BIND_ATTEMPTS`] random ports from it, retrying any refused bind. `site` names
/// the listener in the debug events `{site}_ephemeral_bind` and `{site}_ephemeral_bind_retry`.
///
/// # Errors
///
/// The bind failed; for a range, the last refusal, naming the range and the attempt count. A
/// malformed [`PORT_RANGE_ENV`] is `InvalidInput`, and only for port `0`.
pub async fn bind_tcp(site: &str, addr: SocketAddr) -> io::Result<TcpListener> {
    if addr.port() != 0 {
        return TcpListener::bind(addr).await;
    }
    let range =
        port_range_from_env().map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let draw_from = range.clone();
    bind_tcp_in(site, addr, range.as_ref(), || {
        draw_from.as_ref().map_or(0, random_port_in)
    })
    .await
}

/// [`bind_tcp`] with the range and the candidate source injected.
async fn bind_tcp_in(
    site: &str,
    addr: SocketAddr,
    range: Option<&RangeInclusive<u16>>,
    next_port: impl FnMut() -> u16,
) -> io::Result<TcpListener> {
    let Some(range) = range.filter(|_| addr.port() == 0) else {
        return TcpListener::bind(addr).await;
    };
    bind_candidates(
        site,
        Some(range),
        next_port,
        |port| TcpListener::bind(SocketAddr::new(addr.ip(), port)),
        |_| true,
    )
    .await
    .map_err(|e| {
        let kind = match &e {
            CandidateError::Fatal(last) | CandidateError::Exhausted { last, .. } => last.kind(),
        };
        io::Error::new(kind, e.to_string())
    })
}

/// Why [`bind_candidates`] gave up.
#[derive(Debug)]
pub(crate) enum CandidateError<E> {
    /// A failure another port cannot fix, returned as soon as it happened.
    Fatal(E),
    /// Every candidate was refused; `last` is the final refusal.
    Exhausted {
        last: E,
        attempts: u32,
        /// The [`PORT_RANGE_ENV`] range the candidates came from, if one was set.
        env_range: Option<RangeInclusive<u16>>,
    },
}

/// The text appended to the last refusal when every candidate was refused. Unset, it is
/// exactly what gossip has always reported, which ADR-0003 quotes.
pub(crate) fn exhausted_note(attempts: u32, env_range: Option<&RangeInclusive<u16>>) -> String {
    match env_range {
        None => format!(" (last of {attempts} ephemeral candidate ports)"),
        Some(r) => format!(
            " (last of {attempts} ephemeral candidate ports in {}-{} from {PORT_RANGE_ENV})",
            r.start(),
            r.end()
        ),
    }
}

impl<E: fmt::Display> fmt::Display for CandidateError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fatal(e) => write!(f, "{e}"),
            Self::Exhausted {
                last,
                attempts,
                env_range,
            } => write!(f, "{last}{}", exhausted_note(*attempts, env_range.as_ref())),
        }
    }
}

/// The one candidate loop: draw a port, try `bind`, and on a `retryable` refusal draw again,
/// up to [`EPHEMERAL_BIND_ATTEMPTS`] times. Every bind logs a debug event with `site`,
/// `attempt` and `port`: `{site}_ephemeral_bind` on success, `{site}_ephemeral_bind_retry`
/// (with `error`) on a refusal, under the [`LOG_TARGET`] logger.
pub(crate) async fn bind_candidates<T, E, Fut>(
    site: &str,
    env_range: Option<&RangeInclusive<u16>>,
    mut next_port: impl FnMut() -> u16,
    mut bind: impl FnMut(u16) -> Fut,
    retryable: impl Fn(&E) -> bool,
) -> Result<T, CandidateError<E>>
where
    Fut: Future<Output = Result<T, E>>,
    E: fmt::Display,
{
    let mut attempt = 1;
    loop {
        let port = next_port();
        match bind(port).await {
            Ok(bound) => {
                debug!(target: LOG_TARGET, site, attempt, port, "{site}_ephemeral_bind");
                return Ok(bound);
            }
            Err(e) if !retryable(&e) => return Err(CandidateError::Fatal(e)),
            Err(e) if attempt < EPHEMERAL_BIND_ATTEMPTS => {
                debug!(
                    target: LOG_TARGET,
                    site,
                    attempt,
                    port,
                    error = %e,
                    "{site}_ephemeral_bind_retry"
                );
                attempt += 1;
            }
            Err(last) => {
                return Err(CandidateError::Exhausted {
                    last,
                    attempts: attempt,
                    env_range: env_range.cloned(),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;

    /// The range the gate defaults to. Any free span below the dynamic pool would do.
    fn test_range() -> RangeInclusive<u16> {
        20000..=26999
    }

    fn loopback(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    /// A listener bound inside the range, held so its port refuses the next bind.
    async fn blocker() -> TcpListener {
        let range = test_range();
        bind_tcp_in("blocker", loopback(0), Some(&range), || {
            random_port_in(&range)
        })
        .await
        .expect("a blocker binds somewhere in the range")
    }

    #[test]
    fn a_range_parses_with_or_without_spaces() {
        assert_eq!(parse_port_range("20000-26999"), Ok(20000..=26999));
        assert_eq!(parse_port_range(" 20000 - 26999 "), Ok(20000..=26999));
        assert_eq!(parse_port_range("1-1"), Ok(1..=1));
    }

    #[test]
    fn a_malformed_range_is_rejected_with_the_variable_named() {
        for bad in [
            "20000",
            "a-b",
            "20000-",
            "-26999",
            "0-10",
            "26999-20000",
            "1-70000",
        ] {
            let e = parse_port_range(bad).expect_err(bad);
            assert!(e.contains(PORT_RANGE_ENV), "{bad}: {e}");
            assert!(e.contains(bad), "{bad}: the rejected text is quoted: {e}");
        }
    }

    #[test]
    fn unset_or_blank_means_no_range() {
        assert_eq!(range_from_value(None), Ok(None));
        assert_eq!(range_from_value(Some("")), Ok(None));
        assert_eq!(range_from_value(Some("  ")), Ok(None));
        assert_eq!(
            range_from_value(Some("20000-26999")),
            Ok(Some(20000..=26999))
        );
        assert!(range_from_value(Some("junk")).is_err());
    }

    #[test]
    fn draws_stay_inside_the_range_and_spread_across_it() {
        let range = test_range();
        let draws: Vec<u16> = (0..256).map(|_| random_port_in(&range)).collect();
        assert!(draws.iter().all(|p| range.contains(p)), "{draws:?}");
        let spread = draws.iter().max().unwrap() - draws.iter().min().unwrap();
        assert!(
            spread > 1000,
            "candidates clustered within {spread}: {draws:?}"
        );
        assert_eq!(
            random_port_in(&(7..=7)),
            7,
            "a one-port range draws that port"
        );
    }

    /// Unset is today's behaviour: one plain bind of port `0`, never a candidate draw. The
    /// bind itself may fail on a host whose dynamic pool is exhausted; that is the OS's answer,
    /// and the point is that this path never asked the candidate source.
    #[config_log::retcd_test]
    async fn without_a_range_port_zero_is_left_to_the_os() {
        let mut drawn = 0;
        let result = bind_tcp_in("t", loopback(0), None, || {
            drawn += 1;
            0
        })
        .await;
        assert_eq!(drawn, 0, "no range, no candidates");
        if let Ok(listener) = result {
            assert_ne!(listener.local_addr().unwrap().port(), 0);
        }
    }

    #[config_log::retcd_test]
    async fn with_a_range_the_listener_lands_inside_it() {
        let range = test_range();
        let listener = bind_tcp_in("t", loopback(0), Some(&range), || random_port_in(&range))
            .await
            .expect("bind inside the range");
        let port = listener.local_addr().unwrap().port();
        assert!(range.contains(&port), "{port} outside {range:?}");
    }

    #[config_log::retcd_test]
    async fn an_occupied_candidate_is_skipped() {
        let held = blocker().await;
        let taken = held.local_addr().unwrap().port();
        let range = test_range();
        let mut drawn = 0;
        let listener = bind_tcp_in("t", loopback(0), Some(&range), || {
            drawn += 1;
            if drawn <= 3 {
                taken
            } else {
                random_port_in(&range)
            }
        })
        .await
        .expect("bind past the held port");
        let port = listener.local_addr().unwrap().port();
        assert_ne!(port, taken);
        assert!(range.contains(&port), "{port} outside {range:?}");
        assert!(
            drawn > 3,
            "the held port was tried three times, then more: {drawn}"
        );
    }

    #[config_log::retcd_test]
    async fn every_candidate_refused_names_the_range_and_the_count() {
        let held = blocker().await;
        let taken = held.local_addr().unwrap().port();
        let range = test_range();
        let mut drawn = 0;
        let e = bind_tcp_in("t", loopback(0), Some(&range), || {
            drawn += 1;
            taken
        })
        .await
        .expect_err("every candidate is the held port");
        let text = e.to_string();
        assert_eq!(drawn, EPHEMERAL_BIND_ATTEMPTS as usize);
        assert!(
            text.ends_with(&format!(
                "(last of {EPHEMERAL_BIND_ATTEMPTS} ephemeral candidate ports in 20000-26999 \
                 from {PORT_RANGE_ENV})"
            )),
            "{text}"
        );
    }

    #[config_log::retcd_test]
    async fn a_fixed_port_is_never_moved() {
        let held = blocker().await;
        let fixed = held.local_addr().unwrap();
        let range = test_range();
        let mut drawn = 0;
        let refused = bind_tcp_in("t", fixed, Some(&range), || {
            drawn += 1;
            random_port_in(&range)
        })
        .await;
        assert!(refused.is_err(), "a held fixed port is refused, not moved");
        assert_eq!(drawn, 0, "a fixed port never draws a candidate");

        drop(held);
        let rebound = bind_tcp("t", fixed)
            .await
            .expect("the freed fixed port binds");
        assert_eq!(rebound.local_addr().unwrap(), fixed);
    }
}
