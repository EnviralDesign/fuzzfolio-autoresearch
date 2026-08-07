//! Python-compatible deterministic selectors for finite immigrant axes.

use thiserror::Error;

#[derive(Debug, Error, Eq, PartialEq)]
pub enum SelectorError {
    #[error("pair immigrant selector axis has no values: {axis}")]
    EmptyAxis { axis: String },
    #[error("pair immigrant selector {field} exceeds its unsigned 32-bit byte length")]
    TokenTooLong { field: &'static str },
    #[error("pair immigrant selector exhausted its unsigned 64-bit rejection counter")]
    AttemptExhausted,
}

/// Mirrors `_selector_index` in `temporal_qd_pair_factory.py`.
///
/// The byte framing, UTF-8 conversion, hash, and rejection boundary are all
/// intentional compatibility behavior.  In particular, do not replace this
/// with a modulo of a digest: that would make the finite axis biased.
pub fn selector_index(
    seed: impl ToString,
    axis: impl ToString,
    size: usize,
) -> Result<usize, SelectorError> {
    let seed = seed.to_string();
    let axis = axis.to_string();
    if size == 0 {
        return Err(SelectorError::EmptyAxis { axis });
    }
    let seed_bytes = seed.as_bytes();
    let axis_bytes = axis.as_bytes();
    let seed_length = u32::try_from(seed_bytes.len())
        .map_err(|_| SelectorError::TokenTooLong { field: "seed" })?;
    let axis_length = u32::try_from(axis_bytes.len())
        .map_err(|_| SelectorError::TokenTooLong { field: "axis" })?;

    let mut attempt = 0_u64;
    loop {
        let mut material = Vec::with_capacity(4 + seed_bytes.len() + 4 + axis_bytes.len() + 8);
        material.extend_from_slice(&seed_length.to_be_bytes());
        material.extend_from_slice(seed_bytes);
        material.extend_from_slice(&axis_length.to_be_bytes());
        material.extend_from_slice(axis_bytes);
        material.extend_from_slice(&attempt.to_be_bytes());
        let digest = sha256_digest(&material);
        if let Some(index) = unbiased_digest_index(&digest, size) {
            return Ok(index);
        }
        attempt = attempt
            .checked_add(1)
            .ok_or(SelectorError::AttemptExhausted)?;
    }
}

/// Return the unbiased finite bucket represented by a 256-bit SHA-256 value,
/// or `None` when that digest lies in the rejected high tail.
pub(crate) fn unbiased_digest_index(digest: &[u8; 32], size: usize) -> Option<usize> {
    debug_assert!(size > 0);
    let remainder = two_to_power_256_modulo(size);
    if remainder != 0 && digest.as_slice() >= rejection_threshold(remainder).as_slice() {
        return None;
    }
    Some(digest_modulo(digest, size))
}

pub(crate) fn sha256_digest(input: &[u8]) -> [u8; 32] {
    const INITIAL: [u32; 8] = [
        0x6a09_e667,
        0xbb67_ae85,
        0x3c6e_f372,
        0xa54f_f53a,
        0x510e_527f,
        0x9b05_688c,
        0x1f83_d9ab,
        0x5be0_cd19,
    ];
    const ROUND: [u32; 64] = [
        0x428a_2f98,
        0x7137_4491,
        0xb5c0_fbcf,
        0xe9b5_dba5,
        0x3956_c25b,
        0x59f1_11f1,
        0x923f_82a4,
        0xab1c_5ed5,
        0xd807_aa98,
        0x1283_5b01,
        0x2431_85be,
        0x550c_7dc3,
        0x72be_5d74,
        0x80de_b1fe,
        0x9bdc_06a7,
        0xc19b_f174,
        0xe49b_69c1,
        0xefbe_4786,
        0x0fc1_9dc6,
        0x240c_a1cc,
        0x2de9_2c6f,
        0x4a74_84aa,
        0x5cb0_a9dc,
        0x76f9_88da,
        0x983e_5152,
        0xa831_c66d,
        0xb003_27c8,
        0xbf59_7fc7,
        0xc6e0_0bf3,
        0xd5a7_9147,
        0x06ca_6351,
        0x1429_2967,
        0x27b7_0a85,
        0x2e1b_2138,
        0x4d2c_6dfc,
        0x5338_0d13,
        0x650a_7354,
        0x766a_0abb,
        0x81c2_c92e,
        0x9272_2c85,
        0xa2bf_e8a1,
        0xa81a_664b,
        0xc24b_8b70,
        0xc76c_51a3,
        0xd192_e819,
        0xd699_0624,
        0xf40e_3585,
        0x106a_a070,
        0x19a4_c116,
        0x1e37_6c08,
        0x2748_774c,
        0x34b0_bcb5,
        0x391c_0cb3,
        0x4ed8_aa4a,
        0x5b9c_ca4f,
        0x682e_6ff3,
        0x748f_82ee,
        0x78a5_636f,
        0x84c8_7814,
        0x8cc7_0208,
        0x90be_fffa,
        0xa450_6ceb,
        0xbef9_a3f7,
        0xc671_78f2,
    ];

    let bit_length = (input.len() as u64).wrapping_mul(8);
    let mut padded = Vec::with_capacity(input.len() + 72);
    padded.extend_from_slice(input);
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_length.to_be_bytes());

    let mut state = INITIAL;
    for chunk in padded.chunks_exact(64) {
        let mut words = [0_u32; 64];
        for (index, bytes) in chunk.chunks_exact(4).take(16).enumerate() {
            words[index] = u32::from_be_bytes(bytes.try_into().expect("four-byte chunk"));
        }
        for index in 16..64 {
            let sigma0 = words[index - 15].rotate_right(7)
                ^ words[index - 15].rotate_right(18)
                ^ (words[index - 15] >> 3);
            let sigma1 = words[index - 2].rotate_right(17)
                ^ words[index - 2].rotate_right(19)
                ^ (words[index - 2] >> 10);
            words[index] = words[index - 16]
                .wrapping_add(sigma0)
                .wrapping_add(words[index - 7])
                .wrapping_add(sigma1);
        }

        let mut working = state;
        for index in 0..64 {
            let sigma1 = working[4].rotate_right(6)
                ^ working[4].rotate_right(11)
                ^ working[4].rotate_right(25);
            let choose = (working[4] & working[5]) ^ ((!working[4]) & working[6]);
            let temporary1 = working[7]
                .wrapping_add(sigma1)
                .wrapping_add(choose)
                .wrapping_add(ROUND[index])
                .wrapping_add(words[index]);
            let sigma0 = working[0].rotate_right(2)
                ^ working[0].rotate_right(13)
                ^ working[0].rotate_right(22);
            let majority =
                (working[0] & working[1]) ^ (working[0] & working[2]) ^ (working[1] & working[2]);
            let temporary2 = sigma0.wrapping_add(majority);
            working = [
                temporary1.wrapping_add(temporary2),
                working[0],
                working[1],
                working[2],
                temporary1.wrapping_add(working[3]),
                working[4],
                working[5],
                working[6],
            ];
        }
        for (target, source) in state.iter_mut().zip(working) {
            *target = target.wrapping_add(source);
        }
    }

    let mut digest = [0_u8; 32];
    for (index, word) in state.iter().enumerate() {
        digest[index * 4..index * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    digest
}

fn two_to_power_256_modulo(modulus: usize) -> usize {
    let mut value = 1 % modulus;
    for _ in 0..256 {
        value = add_modulo(value, value, modulus);
    }
    value
}

fn digest_modulo(digest: &[u8; 32], modulus: usize) -> usize {
    let mut value = 0;
    for byte in digest {
        for shift in (0..8).rev() {
            value = add_modulo(value, value, modulus);
            if (byte >> shift) & 1 == 1 {
                value = add_modulo(value, 1, modulus);
            }
        }
    }
    value
}

fn add_modulo(left: usize, right: usize, modulus: usize) -> usize {
    debug_assert!(left < modulus && right < modulus && modulus > 0);
    if left >= modulus - right {
        left - (modulus - right)
    } else {
        left + right
    }
}

fn rejection_threshold(remainder: usize) -> [u8; 32] {
    debug_assert!(remainder > 0);
    // This is the 256-bit representation of 2^256 - remainder.  Any digest
    // at or above it lies in the final `remainder` values and is rejected.
    let mut threshold = [u8::MAX; 32];
    let mut amount = remainder - 1;
    let mut borrow = 0_u16;
    for index in (24..32).rev() {
        let subtrahend = (amount & 0xff) as u16 + borrow;
        let current = threshold[index] as u16;
        threshold[index] = current.wrapping_sub(subtrahend) as u8;
        borrow = u16::from(current < subtrahend);
        amount >>= 8;
    }
    debug_assert_eq!(borrow, 0);
    threshold
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_matches_the_standard_empty_input_vector() {
        let digest = sha256_digest(b"");
        assert_eq!(
            digest,
            [
                0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f,
                0xb9, 0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b,
                0x78, 0x52, 0xb8, 0x55,
            ]
        );
    }

    #[test]
    fn rejects_the_high_tail_instead_of_using_a_biased_modulo() {
        // 2^256 mod 3 is one, so only the all-ones digest is rejected.
        // A modulo-only implementation would incorrectly select bucket zero.
        assert_eq!(unbiased_digest_index(&[u8::MAX; 32], 3), None);
        assert!(unbiased_digest_index(&[u8::MAX - 1; 32], 3).is_some());
    }
}
