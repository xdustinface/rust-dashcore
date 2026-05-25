//! Difficulty retargeting for Dash.
//!
//! Ports the Dark Gravity Wave v3 algorithm from `dashd`'s `pow.cpp`. Only the
//! DGW v3 branch is implemented because the staged-fork pipeline only ingests
//! forks at heights well past `nPowDGWHeight` on every network (34140 on
//! mainnet, 4001/4002 on testnet/regtest/devnet).

use std::cmp::Ordering;

use dashcore::consensus::Params;
use dashcore::{CompactTarget, Header, Network, Target};

/// Number of blocks DGW v3 averages over.
const DGW_PAST_BLOCKS: u32 = 24;

/// Compute the next nBits target using DGW v3.
///
/// `previous_headers` must contain at least the most recent `DGW_PAST_BLOCKS`
/// headers in chain order (oldest first, `previous_headers.last()` being the
/// tip the new block will extend). The `tip_height` is the height of the last
/// entry. Returns `pow_limit` for heights below the DGW window per the dashd
/// pre-window short-circuit.
///
/// Networks with retargeting disabled (`no_pow_retargeting`, regtest) or
/// without enough history return `pow_limit` directly, matching dashd's
/// behavior on those branches.
pub(crate) fn next_work_required_dgw_v3(
    previous_headers: &[Header],
    tip_height: u32,
    params: &Params,
) -> CompactTarget {
    let pow_limit_target = pow_limit_target(params.network);
    let pow_limit_bits = pow_limit_target.to_compact_lossy();

    if tip_height < DGW_PAST_BLOCKS {
        return pow_limit_bits;
    }

    if params.no_pow_retargeting {
        return pow_limit_bits;
    }

    // dashd's `fPowAllowMinDifficultyBlocks` branch (testnet/devnet/regtest)
    // reverts to `pow_limit` when the candidate block is too far in the future
    // of the tip. The staged-fork pipeline does not yet have the candidate
    // block's own time at this call site, so it falls through to the standard
    // DGW average, which is strictly stricter, never looser, than dashd's
    // rule. Fork acceptance can therefore only be over-cautious on those
    // networks, not under-cautious.

    if (previous_headers.len() as u32) < DGW_PAST_BLOCKS {
        return pow_limit_bits;
    }

    let window = &previous_headers[previous_headers.len() - DGW_PAST_BLOCKS as usize..];

    let mut past_target_avg = U256::ZERO;
    for (i, header) in window.iter().rev().enumerate() {
        let count = (i + 1) as u64;
        let target = U256::from_target(Target::from_compact(header.bits));
        if count == 1 {
            past_target_avg = target;
        } else {
            // past_target_avg = (past_target_avg * count + target) / (count + 1)
            let scaled = past_target_avg.mul_u64(count);
            let summed = scaled.add(target);
            past_target_avg = summed.div_u64(count + 1);
        }
    }

    let last = window.last().expect("window length checked above");
    let first = window.first().expect("window length checked above");
    let actual = (last.time as i64 - first.time as i64).max(0) as u64;
    let target_timespan = (DGW_PAST_BLOCKS as u64) * params.pow_target_spacing;

    let actual_clamped = actual.max(target_timespan / 3).min(target_timespan.saturating_mul(3));

    let scaled = past_target_avg.mul_u64(actual_clamped);
    let mut bn_new = scaled.div_u64(target_timespan);

    let limit = U256::from_be_bytes(pow_limit_target.to_be_bytes());
    if bn_new > limit {
        bn_new = limit;
    }

    Target::from_be_bytes(bn_new.to_be_bytes()).to_compact_lossy()
}

fn pow_limit_target(network: Network) -> Target {
    // dashcore stores network `pow_limit` as a `Work` value whose `to_target`
    // inverse does not match dashd's `consensus.powLimit` for the network
    // constants. Use the dashd values directly here so the DGW clamp uses the
    // same upper bound the network enforces.
    let mut bytes = [0u8; 32];
    match network {
        // dashd mainnet/testnet: uint256S("00000fffffff...ff"), ~2^236.
        Network::Mainnet | Network::Testnet => {
            bytes[3] = 0x0f;
            for b in bytes.iter_mut().skip(4) {
                *b = 0xff;
            }
        }
        // dashd regtest/devnet: uint256S("7fffffff...ff"), ~2^255.
        Network::Regtest | Network::Devnet => {
            bytes[0] = 0x7f;
            for b in bytes.iter_mut().skip(1) {
                *b = 0xff;
            }
        }
    }
    Target::from_be_bytes(bytes)
}

/// Minimal 256-bit unsigned big integer.
///
/// Stored as four `u64` limbs, little-endian (`limbs[0]` is least significant).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct U256 {
    limbs: [u64; 4],
}

impl Ord for U256 {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare from most significant limb down.
        for i in (0..4).rev() {
            match self.limbs[i].cmp(&other.limbs[i]) {
                Ordering::Equal => continue,
                ord => return ord,
            }
        }
        Ordering::Equal
    }
}

impl PartialOrd for U256 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl U256 {
    const ZERO: U256 = U256 {
        limbs: [0; 4],
    };

    fn from_be_bytes(bytes: [u8; 32]) -> U256 {
        let mut limbs = [0u64; 4];
        for (i, limb) in limbs.iter_mut().rev().enumerate() {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes[i * 8..(i + 1) * 8]);
            *limb = u64::from_be_bytes(buf);
        }
        U256 {
            limbs,
        }
    }

    fn to_be_bytes(self) -> [u8; 32] {
        let mut out = [0u8; 32];
        for (i, limb) in self.limbs.iter().rev().enumerate() {
            let bytes = limb.to_be_bytes();
            out[i * 8..(i + 1) * 8].copy_from_slice(&bytes);
        }
        out
    }

    fn from_target(target: Target) -> U256 {
        U256::from_be_bytes(target.to_be_bytes())
    }

    fn add(self, other: U256) -> U256 {
        let mut out = [0u64; 4];
        let mut carry: u128 = 0;
        for (i, slot) in out.iter_mut().enumerate() {
            let sum = self.limbs[i] as u128 + other.limbs[i] as u128 + carry;
            *slot = sum as u64;
            carry = sum >> 64;
        }
        U256 {
            limbs: out,
        }
    }

    fn mul_u64(self, rhs: u64) -> U256 {
        let mut out = [0u64; 4];
        let mut carry: u128 = 0;
        for (i, slot) in out.iter_mut().enumerate() {
            let prod = self.limbs[i] as u128 * rhs as u128 + carry;
            *slot = prod as u64;
            carry = prod >> 64;
        }
        U256 {
            limbs: out,
        }
    }

    fn div_u64(self, rhs: u64) -> U256 {
        assert!(rhs != 0, "U256::div_u64 by zero");
        let mut out = [0u64; 4];
        let mut rem: u128 = 0;
        let rhs128 = rhs as u128;
        for (i, slot) in out.iter_mut().enumerate().rev() {
            let cur = (rem << 64) | self.limbs[i] as u128;
            *slot = (cur / rhs128) as u64;
            rem = cur % rhs128;
        }
        U256 {
            limbs: out,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dashcore::block::Version;
    use dashcore::{BlockHash, CompactTarget, Header, TxMerkleNode};
    use dashcore_hashes::Hash;

    fn synthetic_header(bits: u32, time: u32) -> Header {
        Header {
            version: Version::ONE,
            prev_blockhash: BlockHash::all_zeros(),
            merkle_root: TxMerkleNode::all_zeros(),
            time,
            bits: CompactTarget::from_consensus(bits),
            nonce: 0,
        }
    }

    fn pow_limit_compact(params: &Params) -> CompactTarget {
        pow_limit_target(params.network).to_compact_lossy()
    }

    #[test]
    fn pow_limit_returned_below_window() {
        let params = Params::new(Network::Mainnet);
        let bits = next_work_required_dgw_v3(&[], 10, &params);
        assert_eq!(bits, pow_limit_compact(&params));
    }

    #[test]
    fn pow_limit_returned_for_regtest_no_retargeting() {
        let params = Params::new(Network::Regtest);
        let window: Vec<Header> =
            (0..DGW_PAST_BLOCKS).map(|i| synthetic_header(0x1d00ffff, 100 + i * 150)).collect();
        let bits = next_work_required_dgw_v3(&window, 100, &params);
        assert_eq!(bits, pow_limit_compact(&params));
    }

    #[test]
    fn constant_difficulty_window_tightens_slightly() {
        // 24 blocks at exactly target spacing. Because dashd's DGW only counts
        // 23 intervals over 24 blocks, the new target trends slightly stricter
        // (~23/24) even on a perfectly on-pace window. This is the documented
        // bias in `pow.cpp`. We assert the direction and magnitude rather than
        // strict equality.
        let params = Params::new(Network::Mainnet);
        let spacing = params.pow_target_spacing as u32;
        let bits = 0x1b0404cb_u32;
        let window: Vec<Header> = (0..DGW_PAST_BLOCKS)
            .map(|i| synthetic_header(bits, 1_500_000_000 + i * spacing))
            .collect();

        let next = next_work_required_dgw_v3(&window, 100_000, &params);
        let next_target = Target::from_compact(next);
        let in_target = Target::from_compact(CompactTarget::from_consensus(bits));
        assert!(
            next_target < in_target,
            "DGW bias should yield a strictly tighter target on steady spacing"
        );
        // The expected factor is 23/24, so the new target should be at least 90% of the old.
        // We bound via a scaled comparison: in_target * 9 / 10 < next_target < in_target.
        let scaled_floor = U256::from_be_bytes(in_target.to_be_bytes()).mul_u64(9).div_u64(10);
        let next_u = U256::from_be_bytes(next_target.to_be_bytes());
        assert!(
            next_u > scaled_floor,
            "DGW bias should be modest (< 10%), got {:?} vs input {:?}",
            next,
            bits
        );
    }

    #[test]
    fn fast_blocks_raise_difficulty() {
        // Blocks half the spacing apart should lower the target (raise difficulty).
        let params = Params::new(Network::Mainnet);
        let spacing = (params.pow_target_spacing / 2) as u32;
        let bits = 0x1b0404cb_u32;
        let window: Vec<Header> = (0..DGW_PAST_BLOCKS)
            .map(|i| synthetic_header(bits, 1_500_000_000 + i * spacing))
            .collect();
        let next = next_work_required_dgw_v3(&window, 100_000, &params);

        let next_target = Target::from_compact(next);
        let prev_target = Target::from_compact(CompactTarget::from_consensus(bits));
        assert!(
            next_target < prev_target,
            "fast blocks should produce stricter target: next {:?} >= prev {:#x}",
            next,
            bits
        );
    }

    #[test]
    fn slow_blocks_lower_difficulty() {
        // Blocks 2x apart should raise the target (lower difficulty).
        let params = Params::new(Network::Mainnet);
        let spacing = (params.pow_target_spacing * 2) as u32;
        let bits = 0x1b0404cb_u32;
        let window: Vec<Header> = (0..DGW_PAST_BLOCKS)
            .map(|i| synthetic_header(bits, 1_500_000_000 + i * spacing))
            .collect();
        let next = next_work_required_dgw_v3(&window, 100_000, &params);

        let next_target = Target::from_compact(next);
        let prev_target = Target::from_compact(CompactTarget::from_consensus(bits));
        assert!(
            next_target > prev_target,
            "slow blocks should produce looser target: next {:?} <= prev {:#x}",
            next,
            bits
        );
    }

    #[test]
    fn u256_roundtrip_bytes() {
        let mut bytes = [0u8; 32];
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = i as u8;
        }
        let value = U256::from_be_bytes(bytes);
        assert_eq!(value.to_be_bytes(), bytes);
    }

    #[test]
    fn u256_mul_div_roundtrip() {
        let bytes: [u8; 32] = [
            0x00, 0x00, 0x00, 0x00, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22,
            0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00,
            0x11, 0x22, 0x33, 0x44,
        ];
        let value = U256::from_be_bytes(bytes);
        let scaled = value.mul_u64(7);
        let back = scaled.div_u64(7);
        assert_eq!(back, value);
    }
}
