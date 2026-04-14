//! Shadow of `services/change-source/pg/lsn.ts` — pure LSN ↔ string helpers.
//!
//! These functions are the smallest possible end-to-end exercise of the
//! shadow harness: pure transforms, no state, no IO.
//! They double as a build-pipeline smoke test.
//!
//! The port is intentionally self-contained — no dependency on other
//! zero-cache-rs crates. The sync-worker port will follow the same rule:
//! each ported file's Rust source lives inside the crate that shadows it,
//! not pulled in from elsewhere.

use napi::bindgen_prelude::BigInt;

/// `toBigInt(lsn: LSN): bigint` — parse `"H/L"` hex into a `u64`.
fn to_big_int(lsn: &str) -> Result<u64, String> {
    let (hi, lo) = lsn
        .split_once('/')
        .ok_or_else(|| format!("Malformed LSN: \"{}\"", lsn))?;
    let high = u64::from_str_radix(hi, 16).map_err(|_| format!("Malformed LSN: \"{}\"", lsn))?;
    let low = u64::from_str_radix(lo, 16).map_err(|_| format!("Malformed LSN: \"{}\"", lsn))?;
    Ok((high << 32).wrapping_add(low))
}

/// `fromBigInt(val: bigint): LSN` — format `u64` as `"H/L"` uppercase hex.
fn from_big_int(val: u64) -> String {
    format!("{:X}/{:X}", val >> 32, val & 0xffff_ffff)
}

/// Shadow of TS `toBigInt(lsn: LSN): bigint`.
#[napi(js_name = "lsn_to_big_int")]
pub fn lsn_to_big_int(lsn: String) -> napi::Result<BigInt> {
    let v = to_big_int(&lsn).map_err(napi::Error::from_reason)?;
    Ok(BigInt::from(v))
}

/// Shadow of TS `fromBigInt(val: bigint): LSN`.
#[napi(js_name = "lsn_from_big_int")]
pub fn lsn_from_big_int(val: BigInt) -> napi::Result<String> {
    let (signed, words, _lossless) = (val.sign_bit, val.words, false);
    if signed || words.is_empty() {
        return Err(napi::Error::from_reason(
            "lsn_from_big_int: negative or empty BigInt".to_string(),
        ));
    }
    if words.len() > 1 && words[1..].iter().any(|&w| w != 0) {
        return Err(napi::Error::from_reason(
            "lsn_from_big_int: BigInt exceeds 64 bits".to_string(),
        ));
    }
    Ok(from_big_int(words[0]))
}

#[cfg(test)]
mod tests {
    use super::{from_big_int, to_big_int};

    #[test]
    fn round_trip() {
        let n = to_big_int("2B/4F8370").unwrap();
        assert_eq!(from_big_int(n), "2B/4F8370");
    }

    #[test]
    fn malformed_lsn_errors() {
        assert!(to_big_int("garbage").is_err());
    }
}
