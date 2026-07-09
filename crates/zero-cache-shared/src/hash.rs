//! Port of `packages/shared/src/hash.ts`.
//!
//! `h32`/`h64`/`h128` build wider hashes by running xxHash32 with successive
//! seeds and concatenating the 32-bit words, exactly as the TypeScript source
//! does. Strings are hashed as their UTF-8 bytes (js-xxhash uses `TextEncoder`).

/// A 32-bit xxHash of the UTF-8 bytes of `s` with seed 0.
pub fn h32(s: &str) -> u32 {
    xx_hash32(s.as_bytes(), 0)
}

/// A 64-bit hash: two seeded xxHash32 words concatenated (seeds 0, 1).
pub fn h64(s: &str) -> u64 {
    hash_words(s, 2) as u64
}

/// A 128-bit hash: four seeded xxHash32 words concatenated (seeds 0..=3).
pub fn h128(s: &str) -> u128 {
    hash_words(s, 4)
}

/// Runs xxHash32 `words` times with seeds `0..words` and concatenates the
/// results, matching `hash(str, words)` in the TS source:
/// `hash = (hash << 32) + xxHash32(str, i)`.
fn hash_words(s: &str, words: u32) -> u128 {
    let bytes = s.as_bytes();
    let mut hash: u128 = 0;
    for i in 0..words {
        hash = (hash << 32) + xx_hash32(bytes, i) as u128;
    }
    hash
}

const PRIME32_1: u32 = 2_654_435_761;
const PRIME32_2: u32 = 2_246_822_519;
const PRIME32_3: u32 = 3_266_489_917;
const PRIME32_4: u32 = 668_265_263;
const PRIME32_5: u32 = 374_761_393;

#[inline]
fn read_u32_le(b: &[u8], i: usize) -> u32 {
    u32::from_le_bytes([b[i], b[i + 1], b[i + 2], b[i + 3]])
}

#[inline]
fn round(acc: u32, lane: u32) -> u32 {
    let acc = acc.wrapping_add(lane.wrapping_mul(PRIME32_2));
    acc.rotate_left(13).wrapping_mul(PRIME32_1)
}

/// Canonical xxHash32 over a byte slice. Matches `js-xxhash`'s `xxHash32`.
pub fn xx_hash32(buffer: &[u8], seed: u32) -> u32 {
    let len = buffer.len();
    let mut i = 0usize;
    let mut acc: u32;

    if len >= 16 {
        let mut v1 = seed.wrapping_add(PRIME32_1).wrapping_add(PRIME32_2);
        let mut v2 = seed.wrapping_add(PRIME32_2);
        let mut v3 = seed;
        let mut v4 = seed.wrapping_sub(PRIME32_1);

        let limit = len - 16;
        while i <= limit {
            v1 = round(v1, read_u32_le(buffer, i));
            v2 = round(v2, read_u32_le(buffer, i + 4));
            v3 = round(v3, read_u32_le(buffer, i + 8));
            v4 = round(v4, read_u32_le(buffer, i + 12));
            i += 16;
        }
        acc = v1
            .rotate_left(1)
            .wrapping_add(v2.rotate_left(7))
            .wrapping_add(v3.rotate_left(12))
            .wrapping_add(v4.rotate_left(18));
    } else {
        acc = seed.wrapping_add(PRIME32_5);
    }

    acc = acc.wrapping_add(len as u32);

    while i + 4 <= len {
        acc = acc.wrapping_add(read_u32_le(buffer, i).wrapping_mul(PRIME32_3));
        acc = acc.rotate_left(17).wrapping_mul(PRIME32_4);
        i += 4;
    }
    while i < len {
        acc = acc.wrapping_add((buffer[i] as u32).wrapping_mul(PRIME32_5));
        acc = acc.rotate_left(11).wrapping_mul(PRIME32_1);
        i += 1;
    }

    // Final avalanche.
    acc ^= acc >> 15;
    acc = acc.wrapping_mul(PRIME32_2);
    acc ^= acc >> 13;
    acc = acc.wrapping_mul(PRIME32_3);
    acc ^= acc >> 16;
    acc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xxhash32_known_vectors() {
        // Canonical xxHash32 reference vectors.
        assert_eq!(xx_hash32(b"", 0), 0x02CC_5D05);
        assert_eq!(xx_hash32(b"abc", 0), 0x32D1_53FF);
        // Seeded.
        assert_eq!(xx_hash32(b"", 1), 0x0B2C_B792);
    }
}
