// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use chrono::{DateTime, Duration, Utc};
use rand::{RngCore, rngs::OsRng};
use thiserror::Error;

pub const ID_VERSION: u8 = 1;
pub const CUSTOM_EPOCH: &str = "2025-01-01T00:00:00Z";

const CROCKFORD_ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";
const ID_LEN: usize = 26;
const PACKED_LEN: usize = 16;
const BASE37_SENTINEL: u16 = 36;
const BASE37_RADIX: u16 = 37;
const BASE37_MAX: u16 = BASE37_RADIX * BASE37_RADIX * BASE37_RADIX;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedId {
    pub version: u8,
    pub site: String,
    pub env: String,
    pub timestamp: DateTime<Utc>,
    pub broker_slot: u16,
    pub random: [u8; 5],
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DecodeError {
    #[error("request ID must be exactly 26 characters, got {actual}")]
    WrongLength { actual: usize },
    #[error("request ID contains disallowed Crockford base32 character {ch:?}")]
    DisallowedCrockfordCharacter { ch: char },
    #[error("request ID base32 value does not fit in 16 bytes")]
    Base32ValueOutOfRange,
    #[error("unknown request ID version byte {version}")]
    UnknownVersion { version: u8 },
    #[error("packed tag value {value} is outside the base37 range")]
    TagPackedValueOutOfRange { value: u16 },
    #[error("packed tag decodes to an empty tag")]
    EmptyTag,
    #[error("packed tag contains non-sentinel characters after a sentinel")]
    EmbeddedSentinel,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum EncodeError {
    #[error("tag must not be empty")]
    EmptyTag,
    #[error("tag must be at most 3 characters, got {actual}")]
    TagTooLong { actual: usize },
    #[error("tag contains invalid character {ch:?}")]
    InvalidTagCharacter { ch: char },
    #[error("timestamp is before the custom epoch")]
    TimestampBeforeCustomEpoch,
    #[error("timestamp exceeds the u32 seconds-since-epoch ceiling")]
    TimestampTooLarge,
}

pub fn encode(
    site: &str,
    env: &str,
    broker_slot: u16,
    timestamp: DateTime<Utc>,
) -> Result<String, EncodeError> {
    let mut random = [0; 5];
    let mut rng = OsRng;
    rng.fill_bytes(&mut random);
    encode_with_fixed_random(site, env, broker_slot, timestamp, random)
}

pub(crate) fn encode_with_fixed_random(
    site: &str,
    env: &str,
    broker_slot: u16,
    timestamp: DateTime<Utc>,
    random: [u8; 5],
) -> Result<String, EncodeError> {
    let site_packed = pack_tag(site)?;
    let env_packed = pack_tag(env)?;
    let timestamp_sec = timestamp_seconds(timestamp)?;

    let mut packed = [0; PACKED_LEN];
    packed[0] = ID_VERSION;
    packed[1..3].copy_from_slice(&site_packed.to_be_bytes());
    packed[3..5].copy_from_slice(&env_packed.to_be_bytes());
    packed[5..9].copy_from_slice(&timestamp_sec.to_be_bytes());
    packed[9..11].copy_from_slice(&broker_slot.to_be_bytes());
    packed[11..16].copy_from_slice(&random);

    Ok(encode_crockford_base32(&packed))
}

pub fn decode(id: &str) -> Result<DecodedId, DecodeError> {
    let packed = decode_crockford_base32(id)?;
    let version = packed[0];
    if version != ID_VERSION {
        return Err(DecodeError::UnknownVersion { version });
    }

    let site_packed = u16::from_be_bytes([packed[1], packed[2]]);
    let env_packed = u16::from_be_bytes([packed[3], packed[4]]);
    let timestamp_sec = u32::from_be_bytes([packed[5], packed[6], packed[7], packed[8]]);
    let broker_slot = u16::from_be_bytes([packed[9], packed[10]]);
    let mut random = [0; 5];
    random.copy_from_slice(&packed[11..16]);

    Ok(DecodedId {
        version,
        site: unpack_tag(site_packed)?,
        env: unpack_tag(env_packed)?,
        timestamp: custom_epoch() + Duration::seconds(timestamp_sec as i64),
        broker_slot,
        random,
    })
}

pub fn pack_tag(tag: &str) -> Result<u16, EncodeError> {
    let len = tag.chars().count();
    if len == 0 {
        return Err(EncodeError::EmptyTag);
    }
    if len > 3 {
        return Err(EncodeError::TagTooLong { actual: len });
    }

    let mut digits = [BASE37_SENTINEL; 3];
    for (index, ch) in tag.chars().enumerate() {
        digits[index] = tag_char_value(ch)?;
    }

    Ok(digits[0] * BASE37_RADIX * BASE37_RADIX + digits[1] * BASE37_RADIX + digits[2])
}

pub fn unpack_tag(value: u16) -> Result<String, DecodeError> {
    if value >= BASE37_MAX {
        return Err(DecodeError::TagPackedValueOutOfRange { value });
    }

    let digits = [
        value / (BASE37_RADIX * BASE37_RADIX),
        (value / BASE37_RADIX) % BASE37_RADIX,
        value % BASE37_RADIX,
    ];

    let sentinel_index = digits.iter().position(|digit| *digit == BASE37_SENTINEL);
    match sentinel_index {
        Some(0) => Err(DecodeError::EmptyTag),
        Some(index) => {
            if digits[index + 1..]
                .iter()
                .any(|digit| *digit != BASE37_SENTINEL)
            {
                return Err(DecodeError::EmbeddedSentinel);
            }
            Ok(digits[..index]
                .iter()
                .map(|digit| value_char(*digit))
                .collect())
        }
        None => Ok(digits.iter().map(|digit| value_char(*digit)).collect()),
    }
}

fn timestamp_seconds(timestamp: DateTime<Utc>) -> Result<u32, EncodeError> {
    let epoch = custom_epoch();
    if timestamp < epoch {
        return Err(EncodeError::TimestampBeforeCustomEpoch);
    }
    let seconds = timestamp.signed_duration_since(epoch).num_seconds();
    u32::try_from(seconds).map_err(|_| EncodeError::TimestampTooLarge)
}

fn custom_epoch() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(CUSTOM_EPOCH)
        .expect("custom epoch is a valid RFC 3339 timestamp")
        .with_timezone(&Utc)
}

fn tag_char_value(ch: char) -> Result<u16, EncodeError> {
    match ch {
        '0'..='9' => Ok(ch as u16 - '0' as u16),
        'a'..='z' => Ok(ch as u16 - 'a' as u16 + 10),
        _ => Err(EncodeError::InvalidTagCharacter { ch }),
    }
}

fn value_char(value: u16) -> char {
    match value {
        0..=9 => char::from(b'0' + value as u8),
        10..=35 => char::from(b'a' + (value as u8 - 10)),
        _ => unreachable!("base37 sentinel is handled before character conversion"),
    }
}

fn encode_crockford_base32(packed: &[u8; PACKED_LEN]) -> String {
    let mut value = u128::from_be_bytes(*packed);
    let mut encoded = [b'0'; ID_LEN];
    for ch in encoded.iter_mut().rev() {
        *ch = CROCKFORD_ALPHABET[(value & 0x1f) as usize];
        value >>= 5;
    }
    String::from_utf8(encoded.to_vec()).expect("Crockford alphabet is valid UTF-8")
}

fn decode_crockford_base32(id: &str) -> Result<[u8; PACKED_LEN], DecodeError> {
    let actual_len = id.chars().count();
    if actual_len != ID_LEN {
        return Err(DecodeError::WrongLength { actual: actual_len });
    }

    let mut value = 0u128;
    for ch in id.chars() {
        let digit = crockford_value(ch).ok_or(DecodeError::DisallowedCrockfordCharacter { ch })?;
        value = value
            .checked_mul(32)
            .and_then(|value| value.checked_add(digit as u128))
            .ok_or(DecodeError::Base32ValueOutOfRange)?;
    }

    Ok(value.to_be_bytes())
}

fn crockford_value(ch: char) -> Option<u8> {
    if !ch.is_ascii() {
        return None;
    }

    CROCKFORD_ALPHABET
        .iter()
        .position(|candidate| *candidate == ch as u8)
        .map(|index| index as u8)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone};

    fn epoch() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(CUSTOM_EPOCH)
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn base37_roundtrips_representative_tags() {
        for tag in ["0", "a", "z", "00", "az", "zz", "000", "abc", "zzz"] {
            let packed = pack_tag(tag).unwrap();
            assert_eq!(unpack_tag(packed).unwrap(), tag);
        }
    }

    #[test]
    fn base37_rejects_empty_tag() {
        assert_eq!(pack_tag(""), Err(EncodeError::EmptyTag));
    }

    #[test]
    fn base37_rejects_too_long_tag() {
        assert_eq!(pack_tag("abcd"), Err(EncodeError::TagTooLong { actual: 4 }));
    }

    #[test]
    fn base37_rejects_uppercase_tag() {
        assert_eq!(
            pack_tag("AB"),
            Err(EncodeError::InvalidTagCharacter { ch: 'A' })
        );
    }

    #[test]
    fn base37_rejects_non_alphanumeric_tag() {
        assert_eq!(
            pack_tag("a-b"),
            Err(EncodeError::InvalidTagCharacter { ch: '-' })
        );
    }

    #[test]
    fn base37_rejects_out_of_range_packed_value() {
        assert_eq!(
            unpack_tag(50_653),
            Err(DecodeError::TagPackedValueOutOfRange { value: 50_653 })
        );
    }

    #[test]
    fn base37_rejects_embedded_sentinel() {
        let packed = 10 * 37 * 37 + 36 * 37 + 12;
        assert_eq!(unpack_tag(packed), Err(DecodeError::EmbeddedSentinel));
    }

    #[test]
    fn roundtrip_known_vector() {
        let timestamp = Utc.with_ymd_and_hms(2025, 1, 2, 3, 4, 5).unwrap();
        let id =
            encode_with_fixed_random("bol", "dev", 42, timestamp, [0x01, 0x23, 0x45, 0x67, 0x89])
                .unwrap();
        assert_eq!(id, "017sg4fag005yaa01a04hmasw9");

        let decoded = decode(&id).unwrap();
        assert_eq!(
            decoded,
            DecodedId {
                version: 1,
                site: "bol".to_string(),
                env: "dev".to_string(),
                timestamp,
                broker_slot: 42,
                random: [0x01, 0x23, 0x45, 0x67, 0x89],
            }
        );
    }

    #[test]
    fn roundtrip_epoch_timestamp() {
        let id = encode_with_fixed_random("b", "d", 0, epoch(), [0, 1, 2, 3, 4]).unwrap();
        assert_eq!(decode(&id).unwrap().timestamp, epoch());
    }

    #[test]
    fn roundtrip_timestamp_u32_ceiling() {
        let timestamp = epoch() + Duration::seconds(u32::MAX as i64);
        let id = encode_with_fixed_random("b", "d", 0, timestamp, [4, 3, 2, 1, 0]).unwrap();
        assert_eq!(decode(&id).unwrap().timestamp, timestamp);
    }

    #[test]
    fn roundtrip_broker_slot_boundaries() {
        for broker_slot in [0, u16::MAX] {
            let id =
                encode_with_fixed_random("b", "d", broker_slot, epoch(), [9, 8, 7, 6, 5]).unwrap();
            assert_eq!(decode(&id).unwrap().broker_slot, broker_slot);
        }
    }

    #[test]
    fn rejects_wrong_length() {
        assert_eq!(
            decode("017sg4fag00000001a04hmasw"),
            Err(DecodeError::WrongLength { actual: 25 })
        );
        assert_eq!(
            decode("017sg4fag00000001a04hmasw90"),
            Err(DecodeError::WrongLength { actual: 27 })
        );
    }

    #[test]
    fn rejects_disallowed_crockford_characters() {
        for ch in ['i', 'l', 'o', 'u', 'A', '!'] {
            let mut id = "017sg4fag00000001a04hmasw9".to_string();
            id.replace_range(0..1, &ch.to_string());
            assert_eq!(
                decode(&id),
                Err(DecodeError::DisallowedCrockfordCharacter { ch })
            );
        }
    }

    #[test]
    fn rejects_unknown_version_with_offending_byte() {
        let mut id =
            encode_with_fixed_random("bol", "dev", 42, epoch(), [0x01, 0x23, 0x45, 0x67, 0x89])
                .unwrap();
        id.replace_range(0..2, "02");
        assert_eq!(decode(&id), Err(DecodeError::UnknownVersion { version: 2 }));
    }

    #[test]
    fn encode_rejects_timestamp_before_custom_epoch() {
        let timestamp = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();
        assert_eq!(
            encode_with_fixed_random("b", "d", 0, timestamp, [0, 0, 0, 0, 0]),
            Err(EncodeError::TimestampBeforeCustomEpoch)
        );
    }
}
