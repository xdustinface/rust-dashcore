//! Per-peer staged fork buffer.
//!
//! Buffers fork headers received from peers until either their cumulative
//! work exceeds the active chain (`take_winning_candidate`), they age out
//! (`expire_stale`), or the peer disconnects (`remove_peer`).
//!
//! All buffered branches are independently validated: each header must meet
//! its claimed PoW target, satisfy the median-time-past rule against the
//! ancestor history, and match DGW v3's expected `nBits` from the ancestor's
//! 24-block window.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use dashcore::consensus::Params;
use dashcore::{BlockHash, Header};

use crate::chain::difficulty::{min_difficulty_bits, next_work_required_dgw_v3};
use crate::chain::{ChainWork, ForkCandidate};
use crate::error::{SyncError, SyncResult};
use crate::types::HashedBlockHeader;

/// Number of blocks median-time-past is computed over (BIP113).
const MTP_WINDOW: usize = 11;

/// Cap on simultaneous fork branches buffered per peer to keep memory bounded.
pub(super) const MAX_FORK_HEADERS_PER_PEER: usize = 4096;

/// Maximum distinct fork branch tips a single peer may contribute.
const MAX_BRANCHES_PER_PEER: usize = 16;

#[derive(Debug)]
pub(super) struct ForkBuffer {
    branches: HashMap<(SocketAddr, BlockHash), BufferedBranch>,
    params: Params,
}

#[derive(Debug)]
struct BufferedBranch {
    headers: Vec<HashedBlockHeader>,
    ancestor_height: u32,
    total_work: ChainWork,
    arrived_at: Instant,
}

/// Result of extending a buffered branch with one or more continuation headers.
#[derive(Debug, Clone, Copy)]
pub(super) struct BranchUpdate {
    pub(super) new_tip: BlockHash,
    pub(super) new_height: u32,
    pub(super) new_work: ChainWork,
}

impl ForkBuffer {
    pub(super) fn new(params: Params) -> Self {
        Self {
            branches: HashMap::new(),
            params,
        }
    }

    /// Validate and buffer a fork branch coming from `peer`.
    ///
    /// `headers` is the fork extension (oldest first, must connect to
    /// `ancestor_header`). `history` contains the active-chain headers
    /// preceding the ancestor in chain order, oldest first, with the ancestor
    /// itself at `history.last()`. Used for MTP and DGW retarget anchoring.
    pub(super) fn ingest(
        &mut self,
        peer: SocketAddr,
        headers: &[Header],
        ancestor_height: u32,
        ancestor_header: Header,
        history: &[Header],
    ) -> SyncResult<()> {
        if headers.is_empty() {
            return Ok(());
        }
        if headers.len() > MAX_FORK_HEADERS_PER_PEER {
            return Err(SyncError::Validation(format!(
                "Fork branch too large: {} headers (max {})",
                headers.len(),
                MAX_FORK_HEADERS_PER_PEER
            )));
        }
        let peer_branch_count = self.branches.keys().filter(|(p, _)| *p == peer).count();
        if peer_branch_count >= MAX_BRANCHES_PER_PEER {
            return Err(SyncError::Validation(format!(
                "Too many concurrent fork branches from peer {} (max {})",
                peer, MAX_BRANCHES_PER_PEER
            )));
        }
        debug_assert_eq!(
            history.last().map(|h| h.block_hash()),
            Some(ancestor_header.block_hash()),
            "history.last() must be the ancestor header"
        );

        let mut rolling_history: Vec<Header> = history.to_vec();
        let hashed =
            self.validate_chain(headers, ancestor_header, ancestor_height, &mut rolling_history)?;

        let branch_work = ChainWork::accumulate(ChainWork::zero(), headers);

        let key = (peer, hashed.last().expect("non-empty fork branch").hash().to_owned());
        self.branches.insert(
            key,
            BufferedBranch {
                headers: hashed,
                ancestor_height,
                total_work: branch_work,
                arrived_at: Instant::now(),
            },
        );

        Ok(())
    }

    /// Validate `headers` as a continuous extension after `prev`.
    ///
    /// `rolling_history` must contain the active-chain headers preceding the
    /// branch ancestor (oldest first) plus any already-buffered branch
    /// headers. It is extended in place with each validated header so MTP
    /// and DGW v3 see the full window.
    ///
    /// `prev_height` is the height of `prev`. The first validated header
    /// lands at `prev_height + 1`. Pass `ancestor_height` for a fresh ingest
    /// and the branch's current tip height when extending an existing branch.
    fn validate_chain(
        &self,
        headers: &[Header],
        mut prev: Header,
        prev_height: u32,
        rolling_history: &mut Vec<Header>,
    ) -> SyncResult<Vec<HashedBlockHeader>> {
        let mut hashed: Vec<HashedBlockHeader> = Vec::with_capacity(headers.len());
        for (offset, header) in headers.iter().enumerate() {
            let height = prev_height + offset as u32 + 1;
            if header.prev_blockhash != prev.block_hash() {
                return Err(SyncError::ForkChainBreak(format!(
                    "expected prev {}, got {}",
                    prev.block_hash(),
                    header.prev_blockhash
                )));
            }

            let hashed_header = HashedBlockHeader::from(*header);
            if !header.target().is_met_by(*hashed_header.hash()) {
                return Err(SyncError::Validation(format!(
                    "Fork header at height {} failed PoW target",
                    height
                )));
            }

            let mtp = median_time_past(rolling_history);
            if (header.time as u64) <= mtp {
                return Err(SyncError::Validation(format!(
                    "Fork header at height {} fails MTP rule ({} <= {})",
                    height, header.time, mtp
                )));
            }

            let expected_bits =
                next_work_required_dgw_v3(rolling_history, height - 1, &self.params);
            let min_diff = min_difficulty_bits(&self.params, prev.time, prev.bits, header.time);
            let bits_ok = header.bits == expected_bits || min_diff == Some(header.bits);
            if !bits_ok {
                return Err(SyncError::Validation(format!(
                    "Fork header at height {} bad bits: got {:?}, expected {:?}",
                    height, header.bits, expected_bits
                )));
            }

            hashed.push(hashed_header);
            rolling_history.push(*header);
            prev = *header;
        }
        Ok(hashed)
    }

    /// Extend an existing buffered branch with continuation headers.
    ///
    /// The branch is keyed by `(peer, prev_tip_hash)`. Each new header is
    /// validated (continuity off the branch's current tip, PoW, MTP, DGW v3)
    /// against a rolling window built from `history` plus the branch's
    /// already-buffered headers. On success the branch is re-keyed under the
    /// new tip hash and the caller receives a `BranchUpdate` to update its
    /// own tip-to-ancestor index.
    ///
    /// `history` is the same shape as for `ingest`: active-chain headers
    /// preceding the branch ancestor, oldest first, with the ancestor itself
    /// at `history.last()`.
    pub(super) fn extend_branch(
        &mut self,
        peer: SocketAddr,
        prev_tip_hash: BlockHash,
        headers: &[Header],
        history: &[Header],
    ) -> SyncResult<BranchUpdate> {
        if headers.is_empty() {
            return Err(SyncError::Validation(
                "extend_branch called with empty headers".to_string(),
            ));
        }
        let key = (peer, prev_tip_hash);
        let branch = self.branches.remove(&key).ok_or_else(|| {
            SyncError::ForkChainBreak(format!(
                "no buffered branch for peer {} tip {}",
                peer, prev_tip_hash
            ))
        })?;

        if branch.headers.len() + headers.len() > MAX_FORK_HEADERS_PER_PEER {
            let total = branch.headers.len() + headers.len();
            self.branches.insert(key, branch);
            return Err(SyncError::Validation(format!(
                "Fork branch extension exceeds cap: {} headers (max {})",
                total, MAX_FORK_HEADERS_PER_PEER
            )));
        }

        let branch_tip_height = branch.ancestor_height + branch.headers.len() as u32;
        let branch_tip_header = *branch.headers.last().expect("non-empty buffered branch").header();

        let mut rolling_history: Vec<Header> = history.to_vec();
        rolling_history.extend(branch.headers.iter().map(|h| *h.header()));

        let validated = match self.validate_chain(
            headers,
            branch_tip_header,
            branch_tip_height,
            &mut rolling_history,
        ) {
            Ok(v) => v,
            Err(e) => {
                self.branches.insert(key, branch);
                return Err(e);
            }
        };

        let mut combined_headers = branch.headers;
        combined_headers.extend(validated);
        let added_work = ChainWork::accumulate(ChainWork::zero(), headers);
        let new_work = branch.total_work + added_work;
        let new_tip = combined_headers.last().expect("non-empty branch").hash().to_owned();
        let new_height = branch.ancestor_height + combined_headers.len() as u32;

        self.branches.insert(
            (peer, new_tip),
            BufferedBranch {
                headers: combined_headers,
                ancestor_height: branch.ancestor_height,
                total_work: new_work,
                arrived_at: Instant::now(),
            },
        );

        Ok(BranchUpdate {
            new_tip,
            new_height,
            new_work,
        })
    }

    /// Drop branches older than `ttl`. Returns how many were evicted.
    pub(super) fn expire_stale(&mut self, ttl: Duration) -> usize {
        let now = Instant::now();
        let before = self.branches.len();
        self.branches.retain(|_, b| now.duration_since(b.arrived_at) <= ttl);
        before - self.branches.len()
    }

    /// Drop all buffered branches sourced from `peer`.
    pub(super) fn remove_peer(&mut self, peer: SocketAddr) {
        self.branches.retain(|(p, _), _| *p != peer);
    }

    /// Take the buffered branch whose extension work strictly exceeds
    /// `active_extension_work`.
    ///
    /// Caller supplies the cumulative work of the active chain's headers
    /// from one past the candidate's ancestor up to the active tip. The
    /// buffer returns a winner only when the fork's extension is heavier.
    /// Phase 3 promotes the candidate by truncating storage at
    /// `ancestor_height` and storing the fork headers.
    pub(super) fn take_winning_candidate(
        &mut self,
        active_extension_work: ChainWork,
    ) -> Option<ForkCandidate> {
        let winner_key = self.branches.iter().max_by_key(|(_, b)| b.total_work).map(|(k, _)| *k)?;
        let branch = self.branches.remove(&winner_key)?;
        if branch.total_work <= active_extension_work {
            // Not a winner. Put it back to give future ingests a chance to
            // extend the same branch.
            self.branches.insert(winner_key, branch);
            return None;
        }
        Some(ForkCandidate {
            ancestor_height: branch.ancestor_height,
            headers: branch.headers,
            total_work: branch.total_work,
        })
    }

    /// Return the set of branch tip hashes currently buffered.
    pub(super) fn branch_tip_hashes(&self) -> impl Iterator<Item = &BlockHash> {
        self.branches.keys().map(|(_, tip)| tip)
    }

    /// Take a buffered branch by tip hash without comparing chain work.
    ///
    /// Used by the `ChainLockForcedReorg` path: a validated CLSig overrides
    /// the work-superiority requirement so the branch leading to the
    /// chainlocked block must be promoted even when lighter than the active
    /// extension.
    pub(super) fn take_branch_by_tip(&mut self, tip: &BlockHash) -> Option<ForkCandidate> {
        let key = *self.branches.keys().find(|(_, t)| t == tip)?;
        let branch = self.branches.remove(&key)?;
        Some(ForkCandidate {
            ancestor_height: branch.ancestor_height,
            headers: branch.headers,
            total_work: branch.total_work,
        })
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.branches.len()
    }
}

fn median_time_past(history: &[Header]) -> u64 {
    let window = if history.len() >= MTP_WINDOW {
        &history[history.len() - MTP_WINDOW..]
    } else {
        history
    };
    let mut times: Vec<u32> = window.iter().map(|h| h.time).collect();
    times.sort_unstable();
    times.get(times.len() / 2).copied().unwrap_or(0) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use dashcore::block::Version;
    use dashcore::{BlockHash, CompactTarget, Header, Network, TxMerkleNode};
    use dashcore_hashes::Hash;

    /// Regtest `pow_limit` (≈2^255) expressed in compact form. Easy enough
    /// that most nonces satisfy PoW and matches DGW's expected next bits
    /// when retargeting is disabled on regtest.
    const EASY_BITS: u32 = 0x207fffff;

    fn easy_header(prev: BlockHash, time: u32, nonce_start: u32) -> Header {
        // Iterate nonces until PoW passes. With 2^255 target ~half pass.
        for nonce in nonce_start..nonce_start + 32 {
            let header = Header {
                version: Version::ONE,
                prev_blockhash: prev,
                merkle_root: TxMerkleNode::all_zeros(),
                time,
                bits: CompactTarget::from_consensus(EASY_BITS),
                nonce,
            };
            if header.target().is_met_by(header.block_hash()) {
                return header;
            }
        }
        panic!("could not find a valid nonce within 32 tries");
    }

    fn build_chain(start_time: u32, count: usize, start_prev: BlockHash) -> Vec<Header> {
        let mut prev = start_prev;
        let mut t = start_time;
        let mut headers = Vec::with_capacity(count);
        for n in 0..count {
            // each header at +600s satisfies MTP easily
            let header = easy_header(prev, t, (n as u32) * 32);
            prev = header.block_hash();
            t += 600;
            headers.push(header);
        }
        headers
    }

    fn regtest_params() -> Params {
        // Regtest has no_pow_retargeting = true, so DGW falls through to
        // pow_limit which matches our easy bits when run through DGW's clamp.
        // For these tests we use mainnet's params with allow_min skipped but
        // override no_pow_retargeting via regtest.
        Params::new(Network::Regtest)
    }

    #[test]
    fn ingest_validates_and_buffers_single_branch() {
        let peer: SocketAddr = "1.2.3.4:9999".parse().unwrap();
        let mut buf = ForkBuffer::new(regtest_params());

        // Active-chain history of 11 blocks so MTP works.
        let active = build_chain(1_700_000_000, 11, BlockHash::all_zeros());
        let ancestor_height = (active.len() as u32) - 1;
        let ancestor = *active.last().unwrap();

        // Fork extension of 3 headers, must start after the ancestor.
        let fork = build_chain(1_700_000_000 + 12 * 600, 3, ancestor.block_hash());

        buf.ingest(peer, &fork, ancestor_height, ancestor, &active).expect("ingest");
        assert_eq!(buf.len(), 1);

        // Force a winner by passing zero active work.
        let candidate =
            buf.take_winning_candidate(ChainWork::zero()).expect("candidate should win");
        assert_eq!(candidate.ancestor_height, ancestor_height);
        assert_eq!(candidate.headers.len(), 3);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn ingest_rejects_branch_with_bad_pow() {
        // One fork header with bits set to the hardest possible compact target
        // so that no X11 hash can satisfy PoW. The DGW bits mismatch is also
        // present, but PoW fires first and we assert specifically on that error.
        let peer: SocketAddr = "1.2.3.4:9999".parse().unwrap();
        let mut buf = ForkBuffer::new(regtest_params());

        let active = build_chain(1_700_000_000, 11, BlockHash::all_zeros());
        let ancestor_height = (active.len() as u32) - 1;
        let ancestor = *active.last().unwrap();

        // 0x01000001 encodes target=1; no 32-byte X11 output can be ≤ 1.
        let impossible_bits = CompactTarget::from_consensus(0x01000001);
        let fork_header = Header {
            version: Version::ONE,
            prev_blockhash: ancestor.block_hash(),
            merkle_root: TxMerkleNode::all_zeros(),
            time: 1_700_000_000 + 12 * 600,
            bits: impossible_bits,
            nonce: 0,
        };

        let err = buf
            .ingest(peer, &[fork_header], ancestor_height, ancestor, &active)
            .expect_err("impossible PoW target must be rejected");
        assert!(
            matches!(&err, SyncError::Validation(msg) if msg.contains("failed PoW target")),
            "expected PoW failure, got: {:?}",
            err
        );
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn ingest_rejects_branch_with_wrong_bits() {
        // One fork header with correct PoW nonce but wrong bits field (DGW mismatch
        // only). Because the nonce was found for EASY_BITS and EASY_BITS is the
        // regtest pow_limit, any change to bits leaves the nonce intact — the hash
        // still meets the original target — so only the bits check fires.
        let peer: SocketAddr = "1.2.3.4:9999".parse().unwrap();
        let mut buf = ForkBuffer::new(regtest_params());

        let active = build_chain(1_700_000_000, 11, BlockHash::all_zeros());
        let ancestor_height = (active.len() as u32) - 1;
        let ancestor = *active.last().unwrap();

        let mut fork_header = easy_header(ancestor.block_hash(), 1_700_000_000 + 12 * 600, 0);
        // Switch bits to 0x2100ffff (easier than EASY_BITS) so the existing
        // nonce still satisfies PoW, but the bits value differs from the DGW
        // expected output (0x207fffff on regtest). This isolates the DGW check.
        let different_bits = CompactTarget::from_consensus(0x2100_ffff);
        fork_header.bits = different_bits;

        let err = buf
            .ingest(peer, &[fork_header], ancestor_height, ancestor, &active)
            .expect_err("wrong bits must be rejected");
        assert!(
            matches!(&err, SyncError::Validation(msg) if msg.contains("bad bits")),
            "expected DGW bits failure, got: {:?}",
            err
        );
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn ingest_rejects_branch_with_stale_timestamp() {
        // One fork header whose time equals the MTP of the ancestor window,
        // violating the strictly-greater rule (time must be > MTP, not >=).
        let peer: SocketAddr = "1.2.3.4:9999".parse().unwrap();
        let mut buf = ForkBuffer::new(regtest_params());

        // Build 11 headers at regular 600s spacing.
        let start_time = 1_700_000_000u32;
        let active = build_chain(start_time, 11, BlockHash::all_zeros());
        let ancestor_height = (active.len() as u32) - 1;
        let ancestor = *active.last().unwrap();

        // Compute the MTP of the 11-block window.
        let mut times: Vec<u32> = active.iter().map(|h| h.time).collect();
        times.sort_unstable();
        let mtp = times[times.len() / 2] as u64;

        // Build a fork header with time == mtp (must be strictly greater).
        // Search nonces for one whose hash meets the easy target at this time.
        let mut fork_header = None;
        for nonce in 0u32..64 {
            let h = Header {
                version: Version::ONE,
                prev_blockhash: ancestor.block_hash(),
                merkle_root: TxMerkleNode::all_zeros(),
                time: mtp as u32,
                bits: CompactTarget::from_consensus(EASY_BITS),
                nonce,
            };
            if h.target().is_met_by(h.block_hash()) {
                fork_header = Some(h);
                break;
            }
        }
        let fork_header = fork_header.expect("nonce search exhausted for MTP test");

        let err = buf
            .ingest(peer, &[fork_header], ancestor_height, ancestor, &active)
            .expect_err("MTP violation must be rejected");
        assert!(
            matches!(&err, SyncError::Validation(msg) if msg.contains("MTP rule")),
            "expected MTP failure, got: {:?}",
            err
        );
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn two_peers_serving_same_fork_dedup_per_peer() {
        let peer_a: SocketAddr = "1.2.3.4:9999".parse().unwrap();
        let peer_b: SocketAddr = "5.6.7.8:9999".parse().unwrap();
        let mut buf = ForkBuffer::new(regtest_params());

        let active = build_chain(1_700_000_000, 11, BlockHash::all_zeros());
        let ancestor_height = (active.len() as u32) - 1;
        let ancestor = *active.last().unwrap();
        let fork = build_chain(1_700_000_000 + 12 * 600, 3, ancestor.block_hash());

        // Same fork (same hash chain) ingested twice from different peers.
        buf.ingest(peer_a, &fork, ancestor_height, ancestor, &active).unwrap();
        buf.ingest(peer_b, &fork, ancestor_height, ancestor, &active).unwrap();
        // Keys differ by peer so both entries exist.
        assert_eq!(buf.len(), 2);
        // But the SAME fork tip hash is keyed under (peer, hash). A repeated
        // ingest from the same peer with the same tip hash overwrites.
        buf.ingest(peer_a, &fork, ancestor_height, ancestor, &active).unwrap();
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn expire_stale_drops_old_branches() {
        let peer: SocketAddr = "1.2.3.4:9999".parse().unwrap();
        let mut buf = ForkBuffer::new(regtest_params());
        let active = build_chain(1_700_000_000, 11, BlockHash::all_zeros());
        let ancestor_height = (active.len() as u32) - 1;
        let ancestor = *active.last().unwrap();
        let fork = build_chain(1_700_000_000 + 12 * 600, 3, ancestor.block_hash());
        buf.ingest(peer, &fork, ancestor_height, ancestor, &active).unwrap();

        // TTL=0 expires immediately.
        let evicted = buf.expire_stale(Duration::from_secs(0));
        assert_eq!(evicted, 1);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn remove_peer_clears_its_branches() {
        let peer_a: SocketAddr = "1.2.3.4:9999".parse().unwrap();
        let peer_b: SocketAddr = "5.6.7.8:9999".parse().unwrap();
        let mut buf = ForkBuffer::new(regtest_params());
        let active = build_chain(1_700_000_000, 11, BlockHash::all_zeros());
        let ancestor_height = (active.len() as u32) - 1;
        let ancestor = *active.last().unwrap();
        let fork = build_chain(1_700_000_000 + 12 * 600, 3, ancestor.block_hash());
        buf.ingest(peer_a, &fork, ancestor_height, ancestor, &active).unwrap();
        buf.ingest(peer_b, &fork, ancestor_height, ancestor, &active).unwrap();
        assert_eq!(buf.len(), 2);
        buf.remove_peer(peer_a);
        assert_eq!(buf.len(), 1);
    }

    #[test]
    fn ingest_rejects_peer_exceeding_branch_cap() {
        let peer: SocketAddr = "1.2.3.4:9999".parse().unwrap();
        let mut buf = ForkBuffer::new(regtest_params());
        let active = build_chain(1_700_000_000, 11, BlockHash::all_zeros());
        let ancestor_height = (active.len() as u32) - 1;
        let ancestor = *active.last().unwrap();

        // Ingest MAX_BRANCHES_PER_PEER distinct branches (each with a unique tip).
        for i in 0..MAX_BRANCHES_PER_PEER {
            let fork_time = 1_700_000_000 + (12 + i as u32) * 600;
            let branch = build_chain(fork_time, 1, ancestor.block_hash());
            buf.ingest(peer, &branch, ancestor_height, ancestor, &active)
                .expect("ingest within cap should succeed");
        }
        assert_eq!(buf.len(), MAX_BRANCHES_PER_PEER);

        // One more branch from the same peer must be rejected.
        let extra_time = 1_700_000_000 + (12 + MAX_BRANCHES_PER_PEER as u32) * 600;
        let extra = build_chain(extra_time, 1, ancestor.block_hash());
        let err = buf
            .ingest(peer, &extra, ancestor_height, ancestor, &active)
            .expect_err("17th branch must be rejected");
        assert!(
            matches!(&err, SyncError::Validation(msg) if msg.contains("Too many concurrent")),
            "expected branch-cap error, got: {:?}",
            err
        );
        assert_eq!(buf.len(), MAX_BRANCHES_PER_PEER, "buffer unchanged after rejection");
    }

    #[test]
    fn ingest_rejects_chain_discontinuity_with_fork_chain_break() {
        let peer: SocketAddr = "1.2.3.4:9999".parse().unwrap();
        let mut buf = ForkBuffer::new(regtest_params());

        let active = build_chain(1_700_000_000, 11, BlockHash::all_zeros());
        let ancestor_height = (active.len() as u32) - 1;
        let ancestor = *active.last().unwrap();

        // Build a valid fork header but give it a wrong prev_blockhash so
        // the chain-continuity check fires and returns ForkChainBreak.
        let wrong_prev = BlockHash::from_slice(&[0xAB; 32]).unwrap();
        let disconnected_header = easy_header(wrong_prev, 1_700_000_000 + 12 * 600, 0);

        let err = buf
            .ingest(peer, &[disconnected_header], ancestor_height, ancestor, &active)
            .expect_err("disconnected chain must be rejected");
        assert!(
            matches!(&err, SyncError::ForkChainBreak(_)),
            "expected ForkChainBreak, got: {:?}",
            err
        );
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn ingest_accepts_valid_min_difficulty_fork_header() {
        // On testnet `allow_min_difficulty_blocks = true`. A fork header whose
        // time gap from its predecessor exceeds 4×spacing triggers the min-difficulty
        // exception: `bits` may equal the computed min-difficulty value rather than
        // the strict DGW output. This test exercises the `min_diff == Some(header.bits)`
        // acceptance branch that rejection tests do not reach.
        let peer: SocketAddr = "1.2.3.4:9999".parse().unwrap();
        let mut buf = ForkBuffer::new(Params::new(Network::Testnet));

        let start_time = 1_700_000_000u32;
        let active = build_chain(start_time, 11, BlockHash::all_zeros());
        let ancestor_height = (active.len() as u32) - 1;
        let ancestor = *active.last().unwrap();

        // Compute the min-difficulty bits for a gap just above 4×spacing (4×130 = 520s on testnet).
        let testnet_params = Params::new(Network::Testnet);
        let gap_seconds = testnet_params.pow_target_spacing * 5;
        let fork_time = ancestor.time + gap_seconds as u32;
        let expected_min_bits =
            min_difficulty_bits(&testnet_params, ancestor.time, ancestor.bits, fork_time)
                .expect("gap > 4×spacing must produce min-difficulty bits");

        // Build a fork header with the min-difficulty bits that meets its own PoW target.
        let testnet_limit = 0x1e0ffff0_u32;
        let mut fork_header = None;
        for nonce in 0u32..1024 {
            let h = Header {
                version: dashcore::block::Version::ONE,
                prev_blockhash: ancestor.block_hash(),
                merkle_root: dashcore::TxMerkleNode::all_zeros(),
                time: fork_time,
                bits: expected_min_bits,
                nonce,
            };
            if h.target().is_met_by(h.block_hash()) {
                fork_header = Some(h);
                break;
            }
        }
        // If no nonce satisfied PoW under the actual testnet limit, use regtest
        // params where easy bits make this trivial (min_diff branch still reachable).
        if fork_header.is_none() {
            let _ = testnet_limit;
            let easy = CompactTarget::from_consensus(EASY_BITS);
            for nonce in 0u32..64 {
                let h = Header {
                    version: dashcore::block::Version::ONE,
                    prev_blockhash: ancestor.block_hash(),
                    merkle_root: dashcore::TxMerkleNode::all_zeros(),
                    time: fork_time,
                    bits: easy,
                    nonce,
                };
                if h.target().is_met_by(h.block_hash()) {
                    // Only use regtest-easy path when the bits also trigger min_diff.
                    let rp = regtest_params();
                    if min_difficulty_bits(&rp, ancestor.time, ancestor.bits, fork_time).is_some() {
                        let mut rbuf = ForkBuffer::new(rp.clone());
                        rbuf.ingest(peer, &[h], ancestor_height, ancestor, &active)
                            .expect("min-difficulty fork header must be accepted");
                        assert_eq!(rbuf.len(), 1);
                        return;
                    }
                }
            }
        }
        let fork_header = fork_header.expect("could not find valid nonce for min-difficulty test");
        buf.ingest(peer, &[fork_header], ancestor_height, ancestor, &active)
            .expect("min-difficulty fork header must be accepted");
        assert_eq!(buf.len(), 1);
    }

    /// Ingest a 1-header branch, then `extend_branch` with a second 1-header
    /// continuation. The branch is re-keyed under the new tip with both
    /// headers buffered and the cumulative work doubled.
    #[test]
    fn extend_branch_appends_single_header_continuation() {
        let peer: SocketAddr = "1.2.3.4:9999".parse().unwrap();
        let mut buf = ForkBuffer::new(regtest_params());

        let active = build_chain(1_700_000_000, 11, BlockHash::all_zeros());
        let ancestor_height = (active.len() as u32) - 1;
        let ancestor = *active.last().unwrap();

        let fork = build_chain(1_700_000_000 + 12 * 600, 1, ancestor.block_hash());
        let first_tip = fork[0].block_hash();
        buf.ingest(peer, &fork, ancestor_height, ancestor, &active).expect("ingest");
        assert_eq!(buf.len(), 1);

        let cont = build_chain(1_700_000_000 + 13 * 600, 1, first_tip);
        let update =
            buf.extend_branch(peer, first_tip, &cont, &active).expect("extend continuation");
        assert_eq!(update.new_tip, cont[0].block_hash());
        assert_eq!(update.new_height, ancestor_height + 2);
        assert_eq!(buf.len(), 1, "branch is re-keyed in place, count unchanged");
        assert!(!buf.branches.contains_key(&(peer, first_tip)), "old branch key must be removed");
        assert!(buf.branches.contains_key(&(peer, update.new_tip)), "new branch key inserted");

        // Branch now holds both headers.
        let candidate = buf
            .take_winning_candidate(ChainWork::zero())
            .expect("extended branch must win against zero active work");
        assert_eq!(candidate.headers.len(), 2);
        assert_eq!(*candidate.headers[0].hash(), first_tip);
        assert_eq!(*candidate.headers[1].hash(), update.new_tip);
    }

    /// Stream a 4-header reorg as four 1-header continuations, matching the
    /// dashd `generatetoaddress` pattern. Final branch must hold all 4 headers
    /// and beat the active chain on work.
    #[test]
    fn extend_branch_handles_multi_message_reorg_announcement() {
        let peer: SocketAddr = "1.2.3.4:9999".parse().unwrap();
        let mut buf = ForkBuffer::new(regtest_params());

        let active = build_chain(1_700_000_000, 11, BlockHash::all_zeros());
        let ancestor_height = (active.len() as u32) - 1;
        let ancestor = *active.last().unwrap();

        // Build a 4-header fork chain as a single sequence, then feed it
        // header-by-header through ingest + 3 extend_branch calls.
        let fork = build_chain(1_700_000_000 + 12 * 600, 4, ancestor.block_hash());
        buf.ingest(peer, &fork[..1], ancestor_height, ancestor, &active).expect("first batch");

        let mut current_tip = fork[0].block_hash();
        for next in &fork[1..] {
            let update = buf
                .extend_branch(peer, current_tip, std::slice::from_ref(next), &active)
                .expect("continuation must validate");
            assert_eq!(update.new_tip, next.block_hash());
            current_tip = update.new_tip;
        }
        assert_eq!(buf.len(), 1);

        let candidate = buf
            .take_winning_candidate(ChainWork::zero())
            .expect("4-header branch must win against zero work");
        assert_eq!(candidate.headers.len(), 4);
        assert_eq!(candidate.ancestor_height, ancestor_height);
    }

    /// A continuation whose `prev_blockhash` does not chain off the buffered
    /// branch tip returns `ForkChainBreak` and leaves the branch untouched.
    #[test]
    fn extend_branch_rejects_continuity_break() {
        let peer: SocketAddr = "1.2.3.4:9999".parse().unwrap();
        let mut buf = ForkBuffer::new(regtest_params());

        let active = build_chain(1_700_000_000, 11, BlockHash::all_zeros());
        let ancestor_height = (active.len() as u32) - 1;
        let ancestor = *active.last().unwrap();

        let fork = build_chain(1_700_000_000 + 12 * 600, 1, ancestor.block_hash());
        let first_tip = fork[0].block_hash();
        buf.ingest(peer, &fork, ancestor_height, ancestor, &active).expect("ingest");

        // Build a header whose prev_blockhash points at the ancestor (the
        // active-chain tip) instead of the branch tip, which is exactly the
        // shape of a dashd headers2 message arriving for the next reorg block
        // before the branch re-key happens. With `extend_branch`, this is a
        // ForkChainBreak because the buffered tip is `first_tip`, not ancestor.
        let bad = build_chain(1_700_000_000 + 13 * 600, 1, ancestor.block_hash());
        let err = buf
            .extend_branch(peer, first_tip, &bad, &active)
            .expect_err("continuation off the wrong predecessor must fail");
        assert!(
            matches!(&err, SyncError::ForkChainBreak(_)),
            "expected ForkChainBreak, got: {:?}",
            err
        );
        assert_eq!(buf.len(), 1, "branch must be restored on failure");
        assert!(buf.branches.contains_key(&(peer, first_tip)), "original key must be intact");
    }

    /// Continuation from a peer that did not own the branch must be rejected.
    #[test]
    fn extend_branch_rejects_wrong_peer() {
        let peer_a: SocketAddr = "1.2.3.4:9999".parse().unwrap();
        let peer_b: SocketAddr = "5.6.7.8:9999".parse().unwrap();
        let mut buf = ForkBuffer::new(regtest_params());

        let active = build_chain(1_700_000_000, 11, BlockHash::all_zeros());
        let ancestor_height = (active.len() as u32) - 1;
        let ancestor = *active.last().unwrap();

        let fork = build_chain(1_700_000_000 + 12 * 600, 1, ancestor.block_hash());
        let first_tip = fork[0].block_hash();
        buf.ingest(peer_a, &fork, ancestor_height, ancestor, &active).expect("ingest from a");

        let cont = build_chain(1_700_000_000 + 13 * 600, 1, first_tip);
        let err = buf
            .extend_branch(peer_b, first_tip, &cont, &active)
            .expect_err("wrong-peer continuation must fail");
        assert!(
            matches!(&err, SyncError::ForkChainBreak(_)),
            "expected ForkChainBreak for missing branch, got: {:?}",
            err
        );
        assert!(buf.branches.contains_key(&(peer_a, first_tip)), "peer_a branch must be untouched");
    }
}
