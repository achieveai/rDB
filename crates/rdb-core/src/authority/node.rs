//! The `nodes/{id}` record (#80 S1b, ADR-rdb-0023): where a declared data node listens and which
//! machine it runs on.
//!
//! `admin::declare` writes one per declared node, create-only. Its declared fields, the peer
//! address and the machine, are never rewritten; since layout 2 the node itself may rewrite its
//! own app address, and nothing else (#83 B2, below).
//! A node process reads its own and every `--peers` entry's at start, and refuses to start when
//! one disagrees with what it was given. The machine is an operator's label, checked at
//! declaration for its form ([`machine_refusal`]) and for a clash (rdb-api's
//! `admin::machine_clash`) only: nothing can prove which machine a process runs on.
//!
//! # Layout 1
//!
//! `[1]`, the node as a little-endian u32, then the address and the machine, each as one length
//! byte and that many bytes. The address is UTF-8 `host:port`, 1 to [`MAX_ADDR_LEN`] bytes. The
//! machine is 1 to [`MAX_MACHINE_LEN`] bytes of `[a-z0-9._:-]` ([`machine_refusal`]). Nothing
//! may follow. A decoder that meets anything else answers `None`. It is outside `contracts/` on
//! purpose, as [`crate::authority::partition`]'s codec is: a layout of one record family, not a
//! seam between teams.
//!
//! # Layout 2 (#83 slice B2)
//!
//! Layout 1's bytes after a version byte of `[2]`, then the app address as a little-endian u16
//! length and that many bytes of UTF-8, 1 or more. The app address is where the node's app
//! takes forwarded requests, opaque to rDB. The node writes it into its own record at open, and
//! changes nothing else in it (owner ruling, 2026-11-19; amends ADR-rdb-0023's "nothing updates
//! one in place"). The decoder takes any length; only the writer holds it to
//! [`MAX_APP_ADDR_LEN`] ([`app_addr_refusal`]), so the cap can rise without a new layout.
//! [`NodeRecord::decode`] reads both layouts as the declared fields alone, so a record with an
//! app address equals the one the declaration wrote.

use core::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use bytes::Bytes;

use crate::contracts::ids::NodeId;

/// The version byte of layout 1.
const LAYOUT_VERSION: u8 = 1;

/// The version byte of layout 2: layout 1's fields and an app address (#83 B2).
const LAYOUT_APP: u8 = 2;

/// The longest app address a node writes, in bytes (#83 B2, lead decision 2026-11-19): a
/// 253-byte DNS name, a port and a scheme or path fit.
pub const MAX_APP_ADDR_LEN: usize = 512;

/// Why `app` is not an app address a node may write, or `None`: 1 to [`MAX_APP_ADDR_LEN`]
/// bytes. Its content is the app's own: rDB never dials it.
#[must_use]
pub fn app_addr_refusal(app: &str) -> Option<String> {
    if app.is_empty() {
        return Some("the app address is empty".to_owned());
    }
    (app.len() > MAX_APP_ADDR_LEN).then(|| {
        format!(
            "the app address is {} bytes; the most is {MAX_APP_ADDR_LEN}",
            app.len()
        )
    })
}

/// The longest address, in bytes: one length byte.
pub const MAX_ADDR_LEN: usize = 255;

/// The longest machine label, in bytes.
pub const MAX_MACHINE_LEN: usize = 63;

/// One `nodes/{id}` record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRecord {
    /// The node, the same as the key's.
    pub node: NodeId,
    /// Where it listens, `host:port`, as its `--listen` and every `--peers` name it.
    pub addr: String,
    /// The machine it runs on: a label, lowercase ([`machine_refusal`]).
    pub machine: String,
}

/// Why `label` is not a machine label a record holds, or `None`: 1 to [`MAX_MACHINE_LEN`] bytes
/// of lowercase ASCII letters, digits, `.`, `_`, `:` and `-`.
#[must_use]
pub fn machine_refusal(label: &str) -> Option<String> {
    if label.is_empty() || label.len() > MAX_MACHINE_LEN {
        return Some(format!(
            "machine {label:?}: a machine label is 1 to {MAX_MACHINE_LEN} bytes"
        ));
    }
    if let Some(bad) = label.chars().find(|c| !machine_char(*c)) {
        return Some(format!(
            "machine {label:?}: {bad:?} is not one of a-z, 0-9, '.', '_', ':' and '-'"
        ));
    }
    None
}

const fn machine_char(c: char) -> bool {
    matches!(c, 'a'..='z' | '0'..='9' | '.' | '_' | ':' | '-')
}

/// The host of a declared address, in the form two nodes are compared in (#80 S1b ruling 1):
/// one machine is one host however its address is spelled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Host {
    /// An IP address, canonical: an IPv4-mapped IPv6 address is the IPv4 address, so `::1` and
    /// `0:0:0:0:0:0:0:1` are one host, and `[::ffff:127.0.0.1]` is `127.0.0.1`.
    Ip(IpAddr),
    /// A host name, lowercase, less one trailing dot. Two names are one host only when they
    /// are equal so: a name and an IP of the same machine are not caught.
    Name(String),
}

impl std::fmt::Display for Host {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ip(ip) => write!(f, "{ip}"),
            Self::Name(name) => f.write_str(name),
        }
    }
}

/// The host of `addr`, a `host:port` a node is declared at, or why it is not one a peer can
/// dial: no `:port`, a port that is not 1 to 65535 (0 is refused, since no peer can dial it and
/// a node's declared address is never rewritten), an empty host, a bracketed host that is not
/// an IPv6 address, an IPv6 address without brackets, or an unspecified, broadcast or
/// multicast IP address (`0.0.0.0`, `[::]`, `255.255.255.255`, `224.0.0.0/4`, `ff00::/8`, and
/// their IPv4-mapped forms), which a peer on another machine cannot dial. The unspecified address is a
/// listener's wildcard, not a place to dial: `--listen 0.0.0.0` stays legal
/// (`admin::node_start_refusal`), but a record holds an address peers dial.
///
/// A host that is not an IP address must be a DNS host name, checked against a whitelist
/// ([`name_refusal`]) rather than by listing bad hosts, since a declared address is never
/// rewritten: only
/// ASCII letters, digits, `-` and `.`; after one trailing dot is removed, 1 to 253 bytes of
/// labels of 1 to 63 bytes each, none empty and none starting or ending with `-`; and the last
/// label starts with a letter. That last rule refuses every numeric form a resolver may read
/// as an IPv4 address (`127.1`, `0x7f.1`, `0xffffffff`), so such a host is never compared as a
/// name beside the IP it names. Four-octet IPv4 addresses are parsed as IPs, above.
///
/// # Errors
///
/// The reason, as text that names `addr`.
pub fn parse_addr(addr: &str) -> Result<Host, String> {
    let Some((host, port)) = addr.rsplit_once(':') else {
        return Err(format!("address {addr:?} is not host:port"));
    };
    match port.parse::<u16>() {
        Ok(0) => {
            return Err(format!(
                "address {addr:?} has port 0, which no peer can dial; a node's declared address \
                 is never rewritten, so give the port it will listen on"
            ))
        }
        Ok(_) => {}
        Err(_) => return Err(format!("address {addr:?} has no port of 1 to 65535")),
    }
    if host.is_empty() {
        return Err(format!("address {addr:?} has no host"));
    }
    if let Some(inner) = host.strip_prefix('[') {
        let ip = inner
            .strip_suffix(']')
            .and_then(|inner| inner.parse::<Ipv6Addr>().ok())
            .ok_or_else(|| format!("address {addr:?}: a bracketed host must be an IPv6 address"))?;
        return dialable(addr, ip.to_canonical());
    }
    if host.contains(':') {
        return Err(format!(
            "address {addr:?}: an IPv6 host is written in brackets, as [::1]:7100"
        ));
    }
    if let Ok(ip) = host.parse::<Ipv4Addr>() {
        return dialable(addr, IpAddr::V4(ip));
    }
    let name = host.strip_suffix('.').unwrap_or(host);
    if name.is_empty() {
        return Err(format!("address {addr:?} has no host"));
    }
    if let Some(why) = name_refusal(name) {
        return Err(format!("address {addr:?}: {why}"));
    }
    Ok(Host::Name(name.to_ascii_lowercase()))
}

/// The longest host name, in bytes, without its trailing dot (RFC 1035).
const MAX_NAME_LEN: usize = 253;

/// The longest label of a host name, in bytes (RFC 1035).
const MAX_LABEL_LEN: usize = 63;

/// Why `name`, a host with one trailing dot removed, is not a host name [`parse_addr`] takes,
/// or `None`. A whitelist: ASCII letters, digits, `-` and `.` only; 1 to [`MAX_NAME_LEN`]
/// bytes; labels split at `.`, each 1 to [`MAX_LABEL_LEN`] bytes and neither starting nor
/// ending with `-`; and the last label starts with a letter, so no numeric spelling of an IPv4
/// address passes as a name.
fn name_refusal(name: &str) -> Option<&'static str> {
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.')
    {
        return Some("a host name holds only ASCII letters, digits, '-' and '.'");
    }
    if name.len() > MAX_NAME_LEN {
        return Some("a host name is at most 253 bytes, less one trailing dot");
    }
    let bad_label = |label: &str| {
        label.is_empty()
            || label.len() > MAX_LABEL_LEN
            || label.starts_with('-')
            || label.ends_with('-')
    };
    if name.split('.').any(bad_label) {
        return Some(
            "each label of a host name is 1 to 63 bytes, with no empty label and no leading or \
             trailing '-'",
        );
    }
    let last = name.rsplit('.').next().unwrap_or(name);
    if !last.starts_with(|c: char| c.is_ascii_alphabetic()) {
        return Some(
            "the last label of a host name starts with a letter; a numeric host is an IPv4 \
             address of four decimal octets, as 127.0.0.1",
        );
    }
    None
}

/// `ip` as [`parse_addr`]'s host, or why no peer can dial it over TCP: an unspecified,
/// broadcast or multicast address.
fn dialable(addr: &str, ip: IpAddr) -> Result<Host, String> {
    let undialable = match ip {
        IpAddr::V4(v4) => v4.is_unspecified() || v4.is_broadcast() || v4.is_multicast(),
        IpAddr::V6(v6) => v6.is_unspecified() || v6.is_multicast(),
    };
    if undialable {
        return Err(format!(
            "address {addr:?} is an unspecified, broadcast or multicast address, which no peer \
             can dial over TCP; a node's declared address is never rewritten, so give the address \
             the node listens on"
        ));
    }
    Ok(Host::Ip(ip))
}

/// [`parse_addr`]'s host, or `None` when it refuses `addr`.
#[must_use]
pub fn host_of(addr: &str) -> Option<Host> {
    parse_addr(addr).ok()
}

impl NodeRecord {
    /// Layout 1. Every field must already be one [`Self::decode`] reads back: the caller
    /// checks the address and the machine first (`admin::declare` does).
    ///
    /// # Panics
    ///
    /// When the address or the machine is longer than its length byte holds.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let addr = u8::try_from(self.addr.len()).expect("an address of at most 255 bytes");
        let machine = u8::try_from(self.machine.len()).expect("a machine of at most 255 bytes");
        let mut out = Vec::with_capacity(1 + 4 + 2 + self.addr.len() + self.machine.len());
        out.push(LAYOUT_VERSION);
        out.extend_from_slice(&self.node.0.to_le_bytes());
        out.push(addr);
        out.extend_from_slice(self.addr.as_bytes());
        out.push(machine);
        out.extend_from_slice(self.machine.as_bytes());
        Bytes::from(out)
    }

    /// Layout 2: this record and `app`, the node's app address (#83 B2). The caller checks
    /// `app` with [`app_addr_refusal`] first.
    ///
    /// # Panics
    ///
    /// As [`Self::encode`], and when `app` is longer than a u16 holds.
    #[must_use]
    pub fn encode_with_app(&self, app: &str) -> Bytes {
        let len = u16::try_from(app.len()).expect("an app address of at most 65535 bytes");
        let mut out = self.encode().to_vec();
        out[0] = LAYOUT_APP;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(app.as_bytes());
        Bytes::from(out)
    }

    /// The declared fields `body` holds, in layout 1 or 2: `None` for another version, a short
    /// or long body, an empty or non-UTF-8 address or app address, or a machine
    /// [`machine_refusal`] refuses. An app address is not one of them: see
    /// [`Self::decode_with_app`].
    #[must_use]
    pub fn decode(body: &[u8]) -> Option<Self> {
        Self::decode_with_app(body).map(|(record, _)| record)
    }

    /// [`Self::decode`] and the app address: `None` in layout 1, which holds none.
    #[must_use]
    pub fn decode_with_app(body: &[u8]) -> Option<(Self, Option<String>)> {
        let (&version, rest) = body.split_first()?;
        if version != LAYOUT_VERSION && version != LAYOUT_APP {
            return None;
        }
        let (node, rest) = rest.split_at_checked(4)?;
        let node = NodeId(u32::from_le_bytes(node.try_into().ok()?));
        let (addr, rest) = field(rest)?;
        let (machine, rest) = field(rest)?;
        let (app, rest) = if version == LAYOUT_APP {
            let (len, rest) = rest.split_at_checked(2)?;
            let len = usize::from(u16::from_le_bytes(len.try_into().ok()?));
            let (app, rest) = rest.split_at_checked(len)?;
            let app = std::str::from_utf8(app)
                .ok()
                .filter(|app| !app.is_empty())?;
            (Some(app.to_owned()), rest)
        } else {
            (None, rest)
        };
        if !rest.is_empty() || addr.is_empty() || machine_refusal(machine).is_some() {
            return None;
        }
        let record = Self {
            node,
            addr: addr.to_owned(),
            machine: machine.to_owned(),
        };
        Some((record, app))
    }
}

/// One length byte and that many bytes of UTF-8, and what follows.
fn field(body: &[u8]) -> Option<(&str, &[u8])> {
    let (&len, rest) = body.split_first()?;
    let (text, rest) = rest.split_at_checked(usize::from(len))?;
    Some((std::str::from_utf8(text).ok()?, rest))
}

#[cfg(test)]
mod tests {
    use super::{
        app_addr_refusal, host_of, machine_refusal, parse_addr, Host, NodeRecord, MAX_APP_ADDR_LEN,
    };
    use crate::contracts::ids::NodeId;

    /// b2_1 (#83 B2): layout 2's bytes are pinned as a literal, as layout 1's are. It decodes
    /// to the declared fields alone, equal to layout 1's record, and its app address through
    /// `decode_with_app`; layout 1 holds none. An empty or non-UTF-8 app address, a length past
    /// the body, and a trailing byte decode as nothing. An app address past the writer's cap
    /// still decodes: the cap is the writer's. The writer's check refuses empty and 513 bytes,
    /// and takes 512. Cut: `decode_with_app` reads `[2]` as layout 1 (no app field), red at the
    /// pinned literal. Unit.
    #[test]
    fn b2_1_node_record_layout_2_bytes_are_pinned_and_layout_1_still_reads() {
        let record = NodeRecord {
            node: NodeId(3),
            addr: "127.0.0.3:7100".to_owned(),
            machine: "rack1-b".to_owned(),
        };
        let mut pinned = vec![2, 3, 0, 0, 0, 14];
        pinned.extend_from_slice(b"127.0.0.3:7100");
        pinned.push(7);
        pinned.extend_from_slice(b"rack1-b");
        pinned.extend_from_slice(&[9, 0]);
        pinned.extend_from_slice(b"gw-3:8443");
        assert_eq!(
            record.encode_with_app("gw-3:8443").as_ref(),
            pinned.as_slice()
        );
        assert_eq!(NodeRecord::decode(&pinned), Some(record.clone()));
        assert_eq!(
            NodeRecord::decode_with_app(&pinned),
            Some((record.clone(), Some("gw-3:8443".to_owned())))
        );
        assert_eq!(
            NodeRecord::decode_with_app(&record.encode()),
            Some((record.clone(), None)),
            "layout 1 holds no app address"
        );

        let mut empty_app = pinned[..pinned.len() - 11].to_vec();
        empty_app.extend_from_slice(&[0, 0]);
        let mut bad_utf8 = pinned[..pinned.len() - 11].to_vec();
        bad_utf8.extend_from_slice(&[1, 0, 0xFF]);
        let mut trailing = pinned.clone();
        trailing.push(0);
        for (what, body) in [
            ("an empty app address", empty_app),
            ("a non-UTF-8 app address", bad_utf8),
            (
                "an app address cut short",
                pinned[..pinned.len() - 1].to_vec(),
            ),
            ("a trailing byte", trailing),
        ] {
            assert_eq!(NodeRecord::decode_with_app(&body), None, "{what}");
        }

        let long = "a".repeat(MAX_APP_ADDR_LEN + 1);
        assert_eq!(
            NodeRecord::decode_with_app(&record.encode_with_app(&long)),
            Some((record, Some(long.clone()))),
            "the cap is the writer's"
        );
        assert!(app_addr_refusal("").is_some());
        assert!(app_addr_refusal(&long).is_some());
        assert_eq!(app_addr_refusal(&long[1..]), None);
    }

    /// b2_2 (#83 B2, PR #159 review): the public cap is 512 **bytes**, pinned by literals so a
    /// change of `MAX_APP_ADDR_LEN` alone goes red here: 1 and 512 bytes are taken, 0 and 513
    /// refused. A multibyte UTF-8 address of 511 bytes, 256 characters, is taken; one more
    /// two-byte character makes 513 bytes in 257 characters, refused: a check that counted
    /// characters would take it. Cuts: `MAX_APP_ADDR_LEN` 513, red at "513 bytes"; the check
    /// on `app.chars().count()`, red at "513 bytes in 257 characters". Unit.
    #[test]
    fn b2_2_the_app_address_cap_is_512_bytes() {
        assert_eq!(MAX_APP_ADDR_LEN, 512);
        assert_eq!(app_addr_refusal("a"), None, "1 byte");
        assert_eq!(app_addr_refusal(&"a".repeat(512)), None, "512 bytes");
        assert!(app_addr_refusal("").is_some(), "0 bytes");
        assert!(app_addr_refusal(&"a".repeat(513)).is_some(), "513 bytes");

        let under = format!("a{}", "\u{e9}".repeat(255));
        assert_eq!((under.len(), under.chars().count()), (511, 256));
        assert_eq!(
            app_addr_refusal(&under),
            None,
            "511 bytes in 256 characters"
        );
        let over = format!("{under}\u{e9}");
        assert_eq!((over.len(), over.chars().count()), (513, 257));
        assert!(
            app_addr_refusal(&over).is_some(),
            "513 bytes in 257 characters"
        );
    }

    /// #80 S1b row 4: layout 1's bytes are pinned as a literal, since every later build reads
    /// what a declaration wrote: changing `encode` and `decode` together cannot pass while old
    /// stores stop reading. Each malformed body decodes as nothing: a wrong version, a body cut
    /// short, a trailing byte, an empty body, an empty address or machine, and a machine byte
    /// outside the label set. Unit.
    /// Cut: `decode` skips its `rest.is_empty()` check; the trailing byte decodes, red.
    #[test]
    fn s1b_node_record_layout_1_bytes_are_pinned() {
        let record = NodeRecord {
            node: NodeId(3),
            addr: "127.0.0.3:7100".to_owned(),
            machine: "rack1-b".to_owned(),
        };
        let mut pinned = vec![1, 3, 0, 0, 0, 14];
        pinned.extend_from_slice(b"127.0.0.3:7100");
        pinned.push(7);
        pinned.extend_from_slice(b"rack1-b");
        assert_eq!(record.encode().as_ref(), pinned.as_slice());
        assert_eq!(NodeRecord::decode(&pinned), Some(record));

        let mut wrong_version = pinned.clone();
        wrong_version[0] = 0xEE;
        let mut trailing = pinned.clone();
        trailing.push(0);
        let mut upper = pinned.clone();
        let last = upper.len() - 1;
        upper[last] = b'B';
        let mut empty_addr = vec![1, 3, 0, 0, 0, 0, 7];
        empty_addr.extend_from_slice(b"rack1-b");
        let mut empty_machine = vec![1, 3, 0, 0, 0, 14];
        empty_machine.extend_from_slice(b"127.0.0.3:7100");
        empty_machine.push(0);
        for (what, body) in [
            ("a wrong version", wrong_version),
            ("a body cut short", pinned[..pinned.len() - 1].to_vec()),
            ("a trailing byte", trailing),
            ("an empty body", Vec::new()),
            ("an empty address", empty_addr),
            ("an empty machine", empty_machine),
            ("an upper-case machine byte", upper),
        ] {
            assert_eq!(NodeRecord::decode(&body), None, "{what}");
        }
    }

    /// #80 S1b: the host of an address, which `admin::declare` compares, and the machine
    /// label set. Two spellings of one IP are one host; a name is lowercased and loses one
    /// trailing dot. Port 0, a bracketed host that is not IPv6 and an unbracketed IPv6 host are
    /// refused, each with its reason (PR #144 review). So are unspecified, broadcast and
    /// multicast addresses, IPv4, IPv6 and IPv4-mapped (PR #144 review r2). A name passes only
    /// the whitelist (PR #144 review r3, `name_refusal`): numeric and hex spellings (`127.1`,
    /// `1a.2`, `0xffffffff`, `0x7f.1`), empty labels (`..`), a leading or trailing `-`, a name
    /// over 253 bytes, and any byte outside letters, digits, `-` and `.` (space, tab, CR, LF,
    /// NUL, `_`, non-ASCII) are refused. Neighbours that stay hosts: `223.255.255.255`,
    /// `240.0.0.1`, `[fe80::1]`, `localhost`, `1a.example`, and a 253-byte name. A machine
    /// label is `[a-z0-9._:-]`, 1 to 63 bytes, so whitespace and control bytes are refused
    /// there too. Unit.
    /// Cut: `parse_addr` accepts port 0; red at `127.0.0.2:0`. Cut: `dialable` refuses nothing;
    /// red at `255.255.255.255:7100`. Cut: `dialable` skips `is_unspecified`; red at
    /// `0.0.0.0:7100`. Cut: `name_refusal` answers `None`; red at `127.1:7101`.
    #[test]
    fn s1b_host_of_and_machine_labels() {
        let ip = |text: &str| Some(Host::Ip(text.parse().expect("an IP")));
        assert_eq!(host_of("127.0.0.2:7100"), ip("127.0.0.2"));
        assert_eq!(host_of("[::1]:7100"), ip("::1"));
        assert_eq!(host_of("[0:0:0:0:0:0:0:1]:7100"), ip("::1"));
        assert_eq!(host_of("[::ffff:127.0.0.1]:7100"), ip("127.0.0.1"));
        assert_eq!(
            host_of("DB-A.example.:1"),
            Some(Host::Name("db-a.example".to_owned()))
        );
        assert_eq!(host_of("223.255.255.255:7100"), ip("223.255.255.255"));
        assert_eq!(host_of("240.0.0.1:7100"), ip("240.0.0.1"));
        assert_eq!(host_of("[fe80::1]:7100"), ip("fe80::1"));
        for (name, host) in [
            ("localhost:7100", "localhost"),
            ("db-a.example:7100", "db-a.example"),
            ("1a.example:7100", "1a.example"),
            ("a:7100", "a"),
        ] {
            assert_eq!(host_of(name), Some(Host::Name(host.to_owned())), "{name}");
        }
        let longest = format!(
            "{}.{}.{}.{}",
            "a".repeat(63),
            "b".repeat(63),
            "c".repeat(63),
            "d".repeat(61)
        );
        assert_eq!(longest.len(), 253);
        assert!(
            host_of(&format!("{longest}.:1")).is_some(),
            "253 bytes and a trailing dot"
        );
        for (bad, why) in [
            ("127.0.0.2", "not host:port"),
            ("127.0.0.2:x", "no port"),
            (":7100", "no host"),
            ("127.0.0.2:70000", "no port"),
            ("127.0.0.2:0", "port 0"),
            ("[::gg]:7100", "must be an IPv6 address"),
            ("[127.0.0.1]:7100", "must be an IPv6 address"),
            ("[::1:7100", "must be an IPv6 address"),
            ("::1:7100", "in brackets"),
            (".:7100", "no host"),
            ("127.1:7101", "starts with a letter"),
            ("127.0.0.1.:7100", "starts with a letter"),
            ("1:7100", "starts with a letter"),
            ("1a.2:7100", "starts with a letter"),
            ("0xffffffff:7100", "starts with a letter"),
            ("0xe0000001:7100", "starts with a letter"),
            ("0x7f.1:7100", "starts with a letter"),
            ("0x7f000001:7100", "starts with a letter"),
            ("..:7100", "no empty label"),
            ("...:7100", "no empty label"),
            ("db-a..example:7100", "no empty label"),
            ("db-a.example..:7100", "no empty label"),
            ("-db.example:7100", "trailing '-'"),
            ("db-.example:7100", "trailing '-'"),
            ("db a:7100", "only ASCII letters"),
            ("db\ta:7100", "only ASCII letters"),
            ("db\ra:7100", "only ASCII letters"),
            ("db\na:7100", "only ASCII letters"),
            ("db\0a:7100", "only ASCII letters"),
            ("db_a:7100", "only ASCII letters"),
            ("dé.example:7100", "only ASCII letters"),
            ("255.255.255.255:7100", "broadcast or multicast"),
            ("224.0.0.1:7100", "broadcast or multicast"),
            ("239.255.255.255:7100", "broadcast or multicast"),
            ("[ff02::1]:7100", "broadcast or multicast"),
            ("[::ffff:224.0.0.1]:7100", "broadcast or multicast"),
            ("[::ffff:255.255.255.255]:7100", "broadcast or multicast"),
            ("0.0.0.0:7100", "unspecified"),
            ("[::]:7100", "unspecified"),
            ("[::ffff:0.0.0.0]:7100", "unspecified"),
        ] {
            let refused = parse_addr(bad).expect_err(bad);
            assert!(
                refused.contains(why) && refused.contains(&format!("{bad:?}")),
                "{bad}: {refused}"
            );
        }
        let too_long = format!(
            "{}.{}.{}.{}",
            "a".repeat(63),
            "b".repeat(63),
            "c".repeat(63),
            "d".repeat(62)
        );
        let refused = parse_addr(&format!("{too_long}:1")).expect_err("254 bytes");
        assert!(refused.contains("at most 253 bytes"), "{refused}");
        let label = format!("{}.example:1", "a".repeat(64));
        let refused = parse_addr(&label).expect_err("a 64-byte label");
        assert!(refused.contains("1 to 63 bytes"), "{refused}");
        assert_eq!(machine_refusal("host-a.rack_1:x"), None);
        for bad in [
            "",
            "HostA",
            "a b",
            "a\tb",
            "a\rb",
            "a\nb",
            "a\0b",
            "a\u{7f}b",
            "dé",
            &"m".repeat(64),
        ] {
            assert!(machine_refusal(bad).is_some(), "{bad:?}");
        }
    }
}
