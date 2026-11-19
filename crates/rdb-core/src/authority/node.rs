//! The `nodes/{id}` record (#80 S1b, ADR-rdb-0023): where a declared data node listens and which
//! machine it runs on.
//!
//! `admin::declare` writes one per declared node, create-only, and nothing updates it in place.
//! A node process reads its own and every `--peers` entry's at start, and refuses to start when
//! one disagrees with what it was given. The machine is an operator's label, checked only for
//! a clash at declaration: nothing can prove which machine a process runs on.
//!
//! # Layout 1
//!
//! `[1]`, the node as a little-endian u32, then the address and the machine, each as one length
//! byte and that many bytes. The address is UTF-8 `host:port`, 1 to [`MAX_ADDR_LEN`] bytes. The
//! machine is 1 to [`MAX_MACHINE_LEN`] bytes of `[a-z0-9._:-]` ([`machine_refusal`]). Nothing
//! may follow. A decoder that meets anything else answers `None`. It is outside `contracts/` on
//! purpose, as [`crate::authority::partition`]'s codec is: a layout of one record family, not a
//! seam between teams.

use bytes::Bytes;

use crate::contracts::ids::NodeId;

/// The version byte of layout 1.
const LAYOUT_VERSION: u8 = 1;

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

/// The host part of a `host:port` address, brackets of an IPv6 host removed, or `None` when
/// `addr` has no port that parses as a u16 or an empty host.
#[must_use]
pub fn host_of(addr: &str) -> Option<&str> {
    let (host, port) = addr.rsplit_once(':')?;
    port.parse::<u16>().ok()?;
    let host = host
        .strip_prefix('[')
        .and_then(|inner| inner.strip_suffix(']'))
        .unwrap_or(host);
    (!host.is_empty()).then_some(host)
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

    /// The record `body` holds, exactly: `None` for another version, a short or long body, an
    /// empty or non-UTF-8 address, or a machine [`machine_refusal`] refuses.
    #[must_use]
    pub fn decode(body: &[u8]) -> Option<Self> {
        let [LAYOUT_VERSION, rest @ ..] = body else {
            return None;
        };
        let (node, rest) = rest.split_at_checked(4)?;
        let node = NodeId(u32::from_le_bytes(node.try_into().ok()?));
        let (addr, rest) = field(rest)?;
        let (machine, rest) = field(rest)?;
        if !rest.is_empty() || addr.is_empty() || machine_refusal(machine).is_some() {
            return None;
        }
        Some(Self {
            node,
            addr: addr.to_owned(),
            machine: machine.to_owned(),
        })
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
    use super::{host_of, machine_refusal, NodeRecord};
    use crate::contracts::ids::NodeId;

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
        wrong_version[0] = 2;
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

    /// #80 S1b: the host part of an address, which `admin::declare` compares, and the machine
    /// label set. Unit.
    #[test]
    fn s1b_host_of_and_machine_labels() {
        assert_eq!(host_of("127.0.0.2:7100"), Some("127.0.0.2"));
        assert_eq!(host_of("[::1]:7100"), Some("::1"));
        assert_eq!(host_of("db-a.example:1"), Some("db-a.example"));
        for bad in ["127.0.0.2", "127.0.0.2:x", ":7100", "127.0.0.2:70000"] {
            assert_eq!(host_of(bad), None, "{bad}");
        }
        assert_eq!(machine_refusal("host-a.rack_1:x"), None);
        for bad in ["", "HostA", "a b", &"m".repeat(64)] {
            assert!(machine_refusal(bad).is_some(), "{bad:?}");
        }
    }
}
