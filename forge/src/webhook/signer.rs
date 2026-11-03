//! Webhook request signing: HMAC-SHA256 over the timestamp and the body.
//!
//! ```text
//! X-Forge-Timestamp: 1730000000
//! X-Forge-Signature: v1=<64 lowercase hex>
//!
//! signature = HMAC-SHA256(secret, "<timestamp>.<body>")
//! ```
//!
//! `<timestamp>` is the decimal unix-seconds value exactly as sent in
//! `X-Forge-Timestamp`, and `<body>` is the raw request body. The secret is
//! the alert's secret as bytes, used exactly as stored: no trimming, no
//! decoding. Binding the timestamp into the MAC lets a receiver reject a
//! captured request replayed later ([`verify`]'s `tolerance`).
//!
//! Pure functions, no I/O and no clock: the caller supplies the time, so a
//! dispatcher signs with a fresh timestamp on every attempt and a test can
//! verify at any instant it likes.

use std::time::Duration;

use hmac::{Hmac, Mac};
use sha2::Sha256;
use thiserror::Error;

/// The only signature scheme so far; a future one gets `v2=`.
pub const SIGNATURE_PREFIX: &str = "v1=";
/// Hex digits in a `v1` signature: one SHA-256 output.
pub const SIGNATURE_HEX_LEN: usize = 64;
/// A sensible default `tolerance` for [`verify`].
pub const DEFAULT_TOLERANCE: Duration = Duration::from_secs(300);

type HmacSha256 = Hmac<Sha256>;

/// Why [`verify`] rejected a request.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum VerifyError {
    /// The header is not `v1=` followed by 64 hex digits.
    #[error("signature is not v1=<{SIGNATURE_HEX_LEN} hex digits>")]
    Malformed,

    /// The timestamp is further from `now` than the tolerance allows.
    #[error("timestamp is {skew_secs} s away from now, beyond the {tolerance_secs} s tolerance")]
    OutsideTolerance { skew_secs: u64, tolerance_secs: u64 },

    /// Well-formed and timely, but not made with this secret over this
    /// timestamp and body.
    #[error("signature does not match")]
    Mismatch,
}

/// `"v1=" + hex(HMAC-SHA256(secret, "<timestamp_secs>.<body>"))`.
pub fn sign(secret: &[u8], timestamp_secs: i64, body: &[u8]) -> String {
    let tag = mac(secret, timestamp_secs, body).finalize().into_bytes();
    format!("{SIGNATURE_PREFIX}{}", hex::encode(tag))
}

/// Check `signature` (an `X-Forge-Signature` value) for `timestamp_secs` and
/// `body`, as a receiver would.
///
/// The timestamp is checked before the MAC, and the MAC comparison is
/// constant-time. Hex digits may be either case.
pub fn verify(
    secret: &[u8],
    timestamp_secs: i64,
    body: &[u8],
    signature: &str,
    now_secs: i64,
    tolerance: Duration,
) -> Result<(), VerifyError> {
    let tag = signature
        .strip_prefix(SIGNATURE_PREFIX)
        .filter(|hex| hex.len() == SIGNATURE_HEX_LEN)
        .and_then(|hex| hex::decode(hex).ok())
        .ok_or(VerifyError::Malformed)?;

    let skew_secs = now_secs.abs_diff(timestamp_secs);
    if skew_secs > tolerance.as_secs() {
        return Err(VerifyError::OutsideTolerance {
            skew_secs,
            tolerance_secs: tolerance.as_secs(),
        });
    }

    mac(secret, timestamp_secs, body)
        .verify_slice(&tag)
        .map_err(|_| VerifyError::Mismatch)
}

fn mac(secret: &[u8], timestamp_secs: i64, body: &[u8]) -> HmacSha256 {
    // HMAC accepts a key of any length.
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC takes keys of any length");
    mac.update(timestamp_secs.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    mac
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &[u8] = b"0123456789abcdef";
    const BODY: &[u8] = br#"{"rows":[{"slot":1}]}"#;
    const TS: i64 = 1_730_000_000;
    const TOL: Duration = DEFAULT_TOLERANCE;

    #[test]
    fn signature_shape() {
        let sig = sign(SECRET, TS, BODY);
        let hex = sig.strip_prefix("v1=").unwrap();
        assert_eq!(hex.len(), SIGNATURE_HEX_LEN);
        assert!(hex.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)), "{sig}");
    }

    #[test]
    fn known_answer() {
        // Computed outside Rust, by both of:
        //   printf '%s' '1730000000.{"rows":[{"slot":1}]}' | openssl dgst -sha256 -hmac 0123456789abcdef
        //   python3 -c 'import hmac,hashlib; print(hmac.new(b"0123456789abcdef",
        //       b"1730000000.{\"rows\":[{\"slot\":1}]}", hashlib.sha256).hexdigest())'
        assert_eq!(
            sign(SECRET, TS, BODY),
            "v1=5011c4c719ee10ccc1077dbaa83466a0a8f2d6ef5c8f5ae7700f850599efdd05"
        );
    }

    #[test]
    fn round_trip() {
        let sig = sign(SECRET, TS, BODY);
        assert_eq!(verify(SECRET, TS, BODY, &sig, TS, TOL), Ok(()));
        assert_eq!(verify(SECRET, TS, BODY, &sig.to_uppercase().replace("V1=", "v1="), TS, TOL), Ok(()));
    }

    #[test]
    fn any_change_is_a_mismatch() {
        let sig = sign(SECRET, TS, BODY);
        assert_eq!(verify(b"0123456789abcdeF", TS, BODY, &sig, TS, TOL), Err(VerifyError::Mismatch));
        assert_eq!(verify(SECRET, TS + 1, BODY, &sig, TS, TOL), Err(VerifyError::Mismatch));
        assert_eq!(verify(SECRET, TS, br#"{"rows":[{"slot":2}]}"#, &sig, TS, TOL), Err(VerifyError::Mismatch));
        // Trailing whitespace is part of the body.
        assert_eq!(verify(SECRET, TS, b"{\"rows\":[{\"slot\":1}]}\n", &sig, TS, TOL), Err(VerifyError::Mismatch));
    }

    #[test]
    fn the_secret_is_used_as_stored() {
        // No trimming: these are three different keys.
        let a = sign(b"secret-secret-16", TS, BODY);
        let b = sign(b" secret-secret-16", TS, BODY);
        let c = sign(b"secret-secret-16 ", TS, BODY);
        assert_ne!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn timestamp_and_body_cannot_trade_bytes() {
        // "17.3" + "body" vs "1.73" + "body": the '.' separator and the
        // decimal timestamp keep the signed strings distinct.
        assert_ne!(sign(SECRET, 17, b"3x"), sign(SECRET, 173, b"x"));
    }

    #[test]
    fn tolerance_is_inclusive_in_both_directions() {
        let sig = sign(SECRET, TS, BODY);
        assert_eq!(verify(SECRET, TS, BODY, &sig, TS + 300, TOL), Ok(()));
        assert_eq!(verify(SECRET, TS, BODY, &sig, TS - 300, TOL), Ok(()));
        assert_eq!(
            verify(SECRET, TS, BODY, &sig, TS + 301, TOL),
            Err(VerifyError::OutsideTolerance {
                skew_secs: 301,
                tolerance_secs: 300
            })
        );
        assert!(matches!(
            verify(SECRET, TS, BODY, &sig, TS - 301, TOL),
            Err(VerifyError::OutsideTolerance { skew_secs: 301, .. })
        ));
        // Extreme values cannot overflow the skew.
        assert!(matches!(
            verify(SECRET, i64::MIN, BODY, &sign(SECRET, i64::MIN, BODY), i64::MAX, TOL),
            Err(VerifyError::OutsideTolerance { skew_secs: u64::MAX, .. })
        ));
    }

    #[test]
    fn a_stale_forgery_reports_the_format_first_then_time() {
        assert_eq!(verify(SECRET, TS, BODY, "v1=zz", TS + 10_000, TOL), Err(VerifyError::Malformed));
        let forged = format!("v1={}", "0".repeat(64));
        assert!(matches!(
            verify(SECRET, TS, BODY, &forged, TS + 10_000, TOL),
            Err(VerifyError::OutsideTolerance { .. })
        ));
        assert_eq!(verify(SECRET, TS, BODY, &forged, TS, TOL), Err(VerifyError::Mismatch));
    }

    #[test]
    fn malformed_signatures() {
        let good = sign(SECRET, TS, BODY);
        let hex = &good[3..];
        for bad in [
            String::new(),
            hex.to_owned(),
            format!("v2={hex}"),
            format!("V1={hex}"),
            format!("v1={}", &hex[..63]),
            format!("v1={hex}0"),
            format!("v1= {hex}"),
            format!("v1={}g", &hex[..63]),
            format!("{good},{good}"),
        ] {
            assert_eq!(verify(SECRET, TS, BODY, &bad, TS, TOL), Err(VerifyError::Malformed), "{bad:?}");
        }
    }

    #[test]
    fn negative_and_empty_inputs_sign() {
        let sig = sign(b"", -1, b"");
        assert_eq!(verify(b"", -1, b"", &sig, 0, TOL), Ok(()));
    }
}
