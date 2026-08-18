//! Deterministic proposal scheduling and parent-selection primitives.
//!
//! Grammar and archive objects intentionally remain outside this module.  It
//! only determines *when* a parent is needed and the reproducible scalar draw
//! stream used by the existing archive selector.

use thiserror::Error;

use crate::{
    identity::{CanonicalValue, archive_parent_seed_material, canonical_sha256_object},
    selector::unbiased_digest_index,
};

pub const ROTATING_PARENT_SCHEDULE_SCHEMA: &str = "temporal_qd_rotating_parent_schedule_v2";
pub const ROTATING_PARENT_SCHEDULE_SCHEMA_LEGACY: &str = "temporal_qd_rotating_parent_schedule_v1";
pub const RATIONAL_PREFIX_BALANCE_METHOD: &str = "accepted_quota_prefix_balance_v1";
/// The largest ordinal representable by the Rust rational-prefix API.
///
/// The schedule compares the prefixes at `ordinal` and `ordinal + 1`, so its
/// public domain is explicitly `0..u64::MAX`.  CPython can evaluate the final
/// `u64::MAX` ordinal with arbitrary-precision integers; this bounded Rust API
/// instead rejects that value deterministically.
pub const MAX_SCHEDULED_PROPOSAL_ORDINAL: u64 = u64::MAX - 1;

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ScheduleError {
    #[error("rotating parent schedule width must be positive")]
    ZeroBreederWidth,
    #[error("rotating parent schedule parent count must not exceed its width")]
    ParentCountExceedsWidth,
    #[error("rotating parent schedule is not the Python-defined 4/5 capped policy")]
    InvalidRotatingSchedule,
    #[error("pair selection bucket size must be positive")]
    EmptyBucket,
    #[error("pair mutation depth bucket is outside 0..19")]
    InvalidMutationDepthBucket,
    #[error("there are no explicit parents to select")]
    NoExplicitParents,
    #[error("proposal ordinal must be strictly below u64::MAX for rational-prefix scheduling")]
    ProposalOrdinalExhausted,
    #[error("CPython getrandbits request exceeds the unsigned 64-bit Rust API")]
    UnsupportedRandomBitWidth,
}

/// The validated minimum-immigrant schedule committed by a rotating parent
/// projection.  Valid sparse parents are a replacement-sampled reservoir;
/// they do not reduce the evaluated offspring quota.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RotatingParentSchedule {
    pub breeder_width: u64,
    pub breeder_parent_count: u64,
    pub offspring_numerator: u64,
    pub offspring_denominator: u64,
}

impl RotatingParentSchedule {
    /// Construct the exact 4/5 offspring / 1/5 immigrant floor used by Python.
    pub fn from_counts(
        breeder_width: u64,
        breeder_parent_count: u64,
    ) -> Result<Self, ScheduleError> {
        if breeder_width == 0 {
            return Err(ScheduleError::ZeroBreederWidth);
        }
        if breeder_parent_count > breeder_width {
            return Err(ScheduleError::ParentCountExceedsWidth);
        }
        let (offspring_numerator, offspring_denominator) = (4, 5);
        Ok(Self {
            breeder_width,
            breeder_parent_count,
            offspring_numerator,
            offspring_denominator,
        })
    }

    /// Validate a schedule decoded at a higher boundary without reinterpreting
    /// any of its fields.
    pub fn validated(
        breeder_width: u64,
        breeder_parent_count: u64,
        offspring_numerator: u64,
        offspring_denominator: u64,
    ) -> Result<Self, ScheduleError> {
        let expected = Self::from_counts(breeder_width, breeder_parent_count)?;
        if expected.offspring_numerator != offspring_numerator
            || expected.offspring_denominator != offspring_denominator
        {
            return Err(ScheduleError::InvalidRotatingSchedule);
        }
        Ok(expected)
    }

    /// Validate the historical v1 sparse-parent projection exactly as it was
    /// sealed.  v1 either used its available-parent share or the 4/5 cap;
    /// accepting another self-hashed ratio would reinterpret archived work.
    pub fn validated_legacy_fields(
        breeder_width: u64,
        breeder_parent_count: u64,
        offspring_numerator: u64,
        offspring_denominator: u64,
    ) -> Result<Self, ScheduleError> {
        if offspring_denominator == 0 || offspring_numerator > offspring_denominator {
            return Err(ScheduleError::InvalidRotatingSchedule);
        }
        let expected = if u128::from(breeder_parent_count) * 5 < u128::from(breeder_width) * 4 {
            (breeder_parent_count, breeder_width)
        } else {
            (4, 5)
        };
        if (offspring_numerator, offspring_denominator) != expected {
            return Err(ScheduleError::InvalidRotatingSchedule);
        }
        // The v2 runtime deliberately replacement-samples valid parents and
        // retains its fixed accepted-population quota.  Return that execution
        // schedule only after proving the historical record is authentic.
        Self::from_counts(breeder_width, breeder_parent_count)
    }

    /// Python's v2 `scheduleSha256`, excluding the final hash field itself.
    pub fn schedule_sha256(self) -> String {
        canonical_sha256_object(&[
            (
                "schemaVersion",
                CanonicalValue::String(ROTATING_PARENT_SCHEDULE_SCHEMA),
            ),
            ("breederWidth", CanonicalValue::Unsigned(self.breeder_width)),
            (
                "breederParentCount",
                CanonicalValue::Unsigned(self.breeder_parent_count),
            ),
            ("minimumImmigrantNumerator", CanonicalValue::Unsigned(1)),
            ("minimumImmigrantDenominator", CanonicalValue::Unsigned(5)),
            (
                "parentSampling",
                CanonicalValue::String("with_replacement_supported_parents_v1"),
            ),
            (
                "unsupportedParentPolicy",
                CanonicalValue::String("immigrant_only_authority_bound_v1"),
            ),
            (
                "schedulingMethod",
                CanonicalValue::String(RATIONAL_PREFIX_BALANCE_METHOD),
            ),
        ])
    }

    /// Compatibility identity for archived v1 schedules.  v1's sparse
    /// numerator is accepted for recovery but never reused as an allocation.
    pub fn legacy_schedule_sha256(
        breeder_width: u64,
        breeder_parent_count: u64,
        offspring_numerator: u64,
        offspring_denominator: u64,
    ) -> String {
        canonical_sha256_object(&[
            (
                "schemaVersion",
                CanonicalValue::String(ROTATING_PARENT_SCHEDULE_SCHEMA_LEGACY),
            ),
            ("breederWidth", CanonicalValue::Unsigned(breeder_width)),
            (
                "breederParentCount",
                CanonicalValue::Unsigned(breeder_parent_count),
            ),
            ("maximumOffspringNumerator", CanonicalValue::Unsigned(4)),
            ("maximumOffspringDenominator", CanonicalValue::Unsigned(5)),
            (
                "offspringNumerator",
                CanonicalValue::Unsigned(offspring_numerator),
            ),
            (
                "offspringDenominator",
                CanonicalValue::Unsigned(offspring_denominator),
            ),
            (
                "immigrantsFillUnsupportedShare",
                CanonicalValue::Boolean(true),
            ),
            (
                "schedulingMethod",
                CanonicalValue::String("deterministic_rational_prefix_balance"),
            ),
        ])
    }

    pub fn is_immigrant(self, proposal_ordinal: u64) -> Result<bool, ScheduleError> {
        if proposal_ordinal > MAX_SCHEDULED_PROPOSAL_ORDINAL {
            return Err(ScheduleError::ProposalOrdinalExhausted);
        }
        let next = proposal_ordinal
            .checked_add(1)
            .ok_or(ScheduleError::ProposalOrdinalExhausted)?;
        let offspring = (u128::from(next) * u128::from(self.offspring_numerator))
            / u128::from(self.offspring_denominator)
            > (u128::from(proposal_ordinal) * u128::from(self.offspring_numerator))
                / u128::from(self.offspring_denominator);
        Ok(!offspring)
    }
}

/// Frozen accepted-population immigrant floor used by Python v2 allocation.
/// This uses division/remainder rather than `target + 4` so all `u64` targets
/// remain deterministic without overflow.
pub const fn accepted_quota_immigrant_count(
    target_unique_candidates: u64,
    has_supported_parents: bool,
) -> u64 {
    if !has_supported_parents {
        return target_unique_candidates;
    }
    target_unique_candidates / 5
        + if target_unique_candidates % 5 == 0 {
            0
        } else {
            1
        }
}

/// Integer round-half-up offspring quota used by breeding-confidence freeze.
/// Immigrants receive the remainder so the pair always sums to `target`.
pub const fn breeding_confidence_quota_counts(
    target_unique_candidates: u64,
    offspring_numerator: u64,
    offspring_denominator: u64,
) -> (u64, u64) {
    let offspring = ((target_unique_candidates as u128) * (offspring_numerator as u128)
        + (offspring_denominator as u128) / 2)
        / (offspring_denominator as u128);
    let offspring = offspring as u64;
    (offspring, target_unique_candidates - offspring)
}

/// Mirrors `_scheduled_immigrant`.  An archive-free run always starts from
/// immigrants; without an opt-in rotating schedule, one in five slots is an
/// immigrant (`ordinal % 5 == 4`).
pub fn scheduled_immigrant(
    has_parents: bool,
    proposal_ordinal: u64,
    parent_schedule: Option<RotatingParentSchedule>,
) -> Result<bool, ScheduleError> {
    if !has_parents {
        return Ok(true);
    }
    match parent_schedule {
        Some(schedule) => schedule.is_immigrant(proposal_ordinal),
        None => Ok(proposal_ordinal % 5 == 4),
    }
}

/// Select the next attempted origin from accepted evaluated quota state.  A
/// rejection leaves the same deficit active, so retries cannot silently move
/// the final evaluated population away from the frozen scientific allocation.
pub fn scheduled_immigrant_for_accepted_quota(
    desired_offspring: u64,
    desired_immigrants: u64,
    accepted_offspring: u64,
    accepted_immigrants: u64,
) -> Result<bool, ScheduleError> {
    if desired_offspring == 0 {
        return Ok(true);
    }
    if accepted_offspring >= desired_offspring {
        return Ok(true);
    }
    if accepted_immigrants >= desired_immigrants {
        return Ok(false);
    }
    let target = desired_offspring
        .checked_add(desired_immigrants)
        .ok_or(ScheduleError::ProposalOrdinalExhausted)?;
    let accepted = accepted_offspring
        .checked_add(accepted_immigrants)
        .ok_or(ScheduleError::ProposalOrdinalExhausted)?;
    let next = accepted
        .checked_add(1)
        .ok_or(ScheduleError::ProposalOrdinalExhausted)?;
    Ok(
        (u128::from(next) * u128::from(desired_immigrants)) / u128::from(target)
            > (u128::from(accepted) * u128::from(desired_immigrants)) / u128::from(target),
    )
}

/// The same-side crossover slot rule.  It is only meaningful on an offspring
/// slot and only when there is more than one available parent source.
pub const fn is_crossover_slot(
    use_immigrant: bool,
    proposal_ordinal: u64,
    archive_cell_count: usize,
    explicit_parent_count: usize,
) -> bool {
    !use_immigrant
        && proposal_ordinal % 7 == 6
        && (archive_cell_count > 1 || explicit_parent_count > 1)
}

/// Twenty equal-probability buckets encode the exact 70 / 25 / 5 mutation
/// depth distribution: 14 buckets at depth one, five at depth two, one at
/// depth three.
pub fn mutation_depth_from_bucket(bucket: usize) -> Result<u8, ScheduleError> {
    match bucket {
        0..=13 => Ok(1),
        14..=18 => Ok(2),
        19 => Ok(3),
        _ => Err(ScheduleError::InvalidMutationDepthBucket),
    }
}

pub fn unbiased_choice(seed: &str, size: usize) -> Result<usize, ScheduleError> {
    if size == 0 {
        return Err(ScheduleError::EmptyBucket);
    }
    let mut attempt = 0_u64;
    loop {
        let hash = canonical_sha256_object(&[
            ("seed", CanonicalValue::String(seed)),
            ("attempt", CanonicalValue::Unsigned(attempt)),
        ]);
        let digest = digest_from_prefixed_sha256(&hash);
        if let Some(index) = unbiased_digest_index(&digest, size) {
            return Ok(index);
        }
        // A SHA-256 rejection for the small fixed mutation bucket cannot
        // realistically exhaust this, but preserving a checked counter keeps
        // behavior defined for all finite axis sizes.
        attempt = attempt
            .checked_add(1)
            .ok_or(ScheduleError::ProposalOrdinalExhausted)?;
    }
}

pub fn mutation_depth_for_seed(seed: &str) -> Result<u8, ScheduleError> {
    mutation_depth_from_bucket(unbiased_choice(seed, 20)?)
}

/// Python sorts frozen pairs by `identity_sha256` before using the explicit
/// parent ring.  Sorting the identities directly is equivalent because the
/// field is the only key.
pub fn sort_explicit_parent_identities<I, S>(identities: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut sorted = identities.into_iter().map(Into::into).collect::<Vec<_>>();
    sorted.sort_unstable();
    sorted
}

pub fn explicit_parent_index(
    structural_parent_selections: u64,
    parent_count: usize,
) -> Result<usize, ScheduleError> {
    if parent_count == 0 {
        return Err(ScheduleError::NoExplicitParents);
    }
    Ok((structural_parent_selections % parent_count as u64) as usize)
}

/// Exact MT19937 behavior used by Python's `random.Random(int_seed)` for the
/// non-negative 64-bit archive-parent seed in the proposal loop.
#[derive(Clone, Debug)]
pub struct PythonRandom {
    state: [u32; 624],
    index: usize,
}

impl PythonRandom {
    pub fn from_nonnegative_u64(seed: u64) -> Self {
        let key = if seed <= u64::from(u32::MAX) {
            vec![seed as u32]
        } else {
            vec![seed as u32, (seed >> 32) as u32]
        };
        Self::from_python_key(&key)
    }

    /// Equivalent to Python's `Random.random()` (53 random bits formed from
    /// two MT draws).
    pub fn random(&mut self) -> f64 {
        let upper = self.next_u32() >> 5;
        let lower = self.next_u32() >> 6;
        ((u64::from(upper) << 26) | u64::from(lower)) as f64 / 9_007_199_254_740_992.0
    }

    /// Equivalent to Python's `_randbelow_with_getrandbits(n)` for a positive
    /// `n`, including `n.bit_length()` rather than a modulo reduction.
    pub fn randbelow(&mut self, upper: u64) -> Result<u64, ScheduleError> {
        if upper == 0 {
            return Err(ScheduleError::EmptyBucket);
        }
        let bit_length = 64 - upper.leading_zeros();
        loop {
            let draw = self.getrandbits(bit_length)?;
            if draw < upper {
                return Ok(draw);
            }
        }
    }

    pub fn archive_parent_seed(generation_seed: &str, selection_ordinal: u64, label: &str) -> u64 {
        let hash = Self::archive_parent_seed_sha256(generation_seed, selection_ordinal, label);
        u64::from_str_radix(&hash[7..23], 16)
            .expect("shared canonical SHA-256 must have sixteen hexadecimal characters")
    }

    pub fn archive_parent_seed_sha256(
        generation_seed: &str,
        selection_ordinal: u64,
        label: &str,
    ) -> String {
        archive_parent_seed_material(generation_seed, selection_ordinal, label)
    }

    pub fn archive_parent_rng(generation_seed: &str, selection_ordinal: u64, label: &str) -> Self {
        Self::from_nonnegative_u64(Self::archive_parent_seed(
            generation_seed,
            selection_ordinal,
            label,
        ))
    }

    fn from_python_key(key: &[u32]) -> Self {
        debug_assert!(!key.is_empty());
        let mut state = [0_u32; 624];
        state[0] = 19_650_218;
        for index in 1..624 {
            state[index] = 1_812_433_253_u32
                .wrapping_mul(state[index - 1] ^ (state[index - 1] >> 30))
                .wrapping_add(index as u32);
        }

        let mut state_index = 1_usize;
        let mut key_index = 0_usize;
        for _ in 0..624.max(key.len()) {
            state[state_index] = (state[state_index]
                ^ ((state[state_index - 1] ^ (state[state_index - 1] >> 30))
                    .wrapping_mul(1_664_525)))
            .wrapping_add(key[key_index])
            .wrapping_add(key_index as u32);
            state_index += 1;
            key_index += 1;
            if state_index == 624 {
                state[0] = state[623];
                state_index = 1;
            }
            if key_index == key.len() {
                key_index = 0;
            }
        }
        for _ in 0..623 {
            state[state_index] = (state[state_index]
                ^ ((state[state_index - 1] ^ (state[state_index - 1] >> 30))
                    .wrapping_mul(1_566_083_941)))
            .wrapping_sub(state_index as u32);
            state_index += 1;
            if state_index == 624 {
                state[0] = state[623];
                state_index = 1;
            }
        }
        state[0] = 0x8000_0000;
        Self { state, index: 624 }
    }

    /// Equivalent to CPython's `Random.getrandbits(k)` for `k <= 64`.
    ///
    /// The archive-parent selector only consumes values in this range.  The
    /// explicit error makes the Rust `u64` boundary visible instead of silently
    /// truncating CPython's arbitrarily large integers.
    pub fn getrandbits(&mut self, bits: u32) -> Result<u64, ScheduleError> {
        if bits > 64 {
            return Err(ScheduleError::UnsupportedRandomBitWidth);
        }
        Ok(match bits {
            0 => 0,
            1..=32 => u64::from(self.next_u32() >> (32 - bits)),
            33..=64 => {
                // CPython fills a little-endian word array, so the first MT
                // word is the low 32 bits of the Python integer.
                let low = u64::from(self.next_u32());
                let high_bits = bits - 32;
                let high = u64::from(self.next_u32() >> (32 - high_bits));
                low | (high << 32)
            }
            _ => unreachable!(),
        })
    }

    fn next_u32(&mut self) -> u32 {
        const UPPER_MASK: u32 = 0x8000_0000;
        const LOWER_MASK: u32 = 0x7fff_ffff;
        const MATRIX_A: u32 = 0x9908_b0df;
        if self.index >= 624 {
            for index in 0..(624 - 397) {
                let value = (self.state[index] & UPPER_MASK) | (self.state[index + 1] & LOWER_MASK);
                self.state[index] = self.state[index + 397]
                    ^ (value >> 1)
                    ^ if value & 1 == 0 { 0 } else { MATRIX_A };
            }
            for index in (624 - 397)..623 {
                let value = (self.state[index] & UPPER_MASK) | (self.state[index + 1] & LOWER_MASK);
                self.state[index] = self.state[index + 397 - 624]
                    ^ (value >> 1)
                    ^ if value & 1 == 0 { 0 } else { MATRIX_A };
            }
            let value = (self.state[623] & UPPER_MASK) | (self.state[0] & LOWER_MASK);
            self.state[623] =
                self.state[396] ^ (value >> 1) ^ if value & 1 == 0 { 0 } else { MATRIX_A };
            self.index = 0;
        }
        let mut value = self.state[self.index];
        self.index += 1;
        value ^= value >> 11;
        value ^= (value << 7) & 0x9d2c_5680;
        value ^= (value << 15) & 0xefc6_0000;
        value ^= value >> 18;
        value
    }
}

fn digest_from_prefixed_sha256(hash: &str) -> [u8; 32] {
    debug_assert_eq!(hash.len(), 71);
    let mut digest = [0_u8; 32];
    for (index, destination) in digest.iter_mut().enumerate() {
        *destination = u8::from_str_radix(&hash[7 + index * 2..9 + index * 2], 16)
            .expect("shared SHA-256 must use lowercase hexadecimal");
    }
    digest
}
