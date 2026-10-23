//! `config_core::key_hex`: the one rendering of a key in a log line (ADR-0013).
//!
//! config-core, config-engine and config-storage each had a copy. They now share this one, so
//! the redaction rule — lowercase hex of at most the first 32 bytes, never the key's text —
//! has one place to be right.

/// Lowercase hex, two characters per byte.
/// Cut: uppercase digits — the first assertion fails.
#[config_log::retcd_test]
fn key_hex_is_lowercase_hex() {
    assert_eq!(config_core::key_hex(b"\x00\xff\x0a/a"), "00ff0a2f61");
    assert_eq!(config_core::key_hex(b""), "");
}

/// At most the first 32 bytes are shown, so a log line never carries a whole large key.
/// Cut: no cap — a 64-byte key renders as 128 characters.
#[config_log::retcd_test]
fn key_hex_shows_at_most_32_bytes() {
    let key = [0xabu8; 64];
    assert_eq!(config_core::key_hex(&key), "ab".repeat(32));
    assert_eq!(config_core::key_hex(&key[..32]), "ab".repeat(32));
}
