// Written in 2014 by Andrew Poelstra <apoelstra@wpsoftware.net>
// SPDX-License-Identifier: CC0-1.0

//! Blockdata constants.
//!
//! This module provides various constants relating to the blockchain and
//! consensus code. In particular, it defines the genesis block header.
//!

use hashes::{Hash, sha256d};
use hex_lit::hex;

use crate::blockdata::block;
use crate::pow::CompactTarget;
use dash_network::Network;

/// How many satoshis are in "one dash".
pub const COIN_VALUE: u64 = 100_000_000;
/// How many seconds between blocks we expect on average.
pub const TARGET_BLOCK_SPACING: u32 = 600;
/// How many blocks between diffchanges.
pub const DIFFCHANGE_INTERVAL: u32 = 2016;
/// How much time on average should occur between diffchanges.
pub const DIFFCHANGE_TIMESPAN: u32 = 14 * 24 * 3600;
/// The maximum allowed weight for a block, see BIP 141 (network rule).
pub const MAX_BLOCK_WEIGHT: u32 = 4_000_000;
/// The minimum transaction weight for a valid serialized transaction.
pub const MIN_TRANSACTION_WEIGHT: u32 = 4 * 60;
/// The factor that non-witness serialization data is multiplied by during weight calculation.
pub const WITNESS_SCALE_FACTOR: usize = 4;
/// The maximum allowed number of signature check operations in a block.
pub const MAX_BLOCK_SIGOPS_COST: i64 = 80_000;
/// Mainnet (dash) pubkey address prefix.
pub const PUBKEY_ADDRESS_PREFIX_MAIN: u8 = 76;
// 0x4C
/// Mainnet (dash) script address prefix.
pub const SCRIPT_ADDRESS_PREFIX_MAIN: u8 = 16;
// 0x10
/// Test (testnet, devnet, regtest) pubkey address prefix.
pub const PUBKEY_ADDRESS_PREFIX_TEST: u8 = 140;
// 0x8C
/// Test (testnet, devnet, regtest) script address prefix.
pub const SCRIPT_ADDRESS_PREFIX_TEST: u8 = 19;
// 0x13
/// The maximum allowed script size.
pub const MAX_SCRIPT_ELEMENT_SIZE: usize = 520;
/// How many blocks between halvings.
pub const SUBSIDY_HALVING_INTERVAL: u32 = 210_000;
/// Maximum allowed value for an integer in Script.
pub const MAX_SCRIPTNUM_VALUE: u32 = 0x80000000;
// 2^31
/// Number of blocks needed for an output from a coinbase transaction to be spendable.
pub const COINBASE_MATURITY: u32 = 100;

/// The maximum value allowed in an output (useful for sanity checking,
/// since keeping everything below this value should prevent overflows
/// if you are doing anything remotely sane with monetary values).
pub const MAX_MONEY: u64 = 21_000_000 * COIN_VALUE;

/// Returns the genesis block header for the given network.
pub fn genesis_header(network: Network) -> block::Header {
    // All networks use the same merkle root (from the Dash genesis transaction)
    let merkle_bytes = hex!("c762a6567f3cc092f0684bb62b7e00a84890b990f07cc71a6bb58d64b98e02e0");
    let merkle_root = sha256d::Hash::from_slice(&merkle_bytes).unwrap().into();

    match network {
        Network::Dash => block::Header {
            version: block::Version::ONE,
            prev_blockhash: Hash::all_zeros(),
            merkle_root,
            time: 1390095618,
            bits: CompactTarget::from_consensus(0x1e0ffff0),
            nonce: 28917698,
        },
        Network::Testnet => block::Header {
            version: block::Version::ONE,
            prev_blockhash: Hash::all_zeros(),
            merkle_root,
            time: 1390666206,
            bits: CompactTarget::from_consensus(0x1e0ffff0),
            nonce: 3861367235,
        },
        Network::Devnet | Network::Regtest => block::Header {
            version: block::Version::ONE,
            prev_blockhash: Hash::all_zeros(),
            merkle_root,
            time: 1417713337,
            bits: CompactTarget::from_consensus(0x207fffff),
            nonce: 1096447,
        },
        // Any new network variant must be handled explicitly.
        _ => unreachable!("genesis_header(): unsupported network variant {network:?}"),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use dash_network::Network;

    #[test]
    fn dash_genesis_header() {
        let header = genesis_header(Network::Dash);

        assert_eq!(header.version, block::Version::ONE);
        assert_eq!(header.prev_blockhash, Hash::all_zeros());
        assert_eq!(
            header.merkle_root.to_string(),
            "e0028eb9648db56b1ac77cf090b99048a8007e2bb64b68f092c03c7f56a662c7"
        );
        assert_eq!(header.time, 1390095618);
        assert_eq!(header.bits, CompactTarget::from_consensus(0x1e0ffff0));
        assert_eq!(header.nonce, 28917698);
        assert_eq!(
            header.block_hash().to_string(),
            "00000ffd590b1485b3caadc19b22e6379c733355108f107a430458cdf3407ab6"
        );
    }

    #[test]
    fn testnet_genesis_header() {
        let header = genesis_header(Network::Testnet);
        assert_eq!(header.version, block::Version::ONE);
        assert_eq!(header.prev_blockhash, Hash::all_zeros());
        assert_eq!(
            header.merkle_root.to_string(),
            "e0028eb9648db56b1ac77cf090b99048a8007e2bb64b68f092c03c7f56a662c7"
        );
        assert_eq!(header.time, 1390666206);
        assert_eq!(header.bits, CompactTarget::from_consensus(0x1e0ffff0));
        assert_eq!(header.nonce, 3861367235);
        assert_eq!(
            header.block_hash().to_string(),
            "00000bafbc94add76cb75e2ec92894837288a481e5c005f6563d91623bf8bc2c"
        );
    }

    #[test]
    fn devnet_genesis_header() {
        let header = genesis_header(Network::Devnet);
        assert_eq!(header.version, block::Version::ONE);
        assert_eq!(header.prev_blockhash, Hash::all_zeros());
        assert_eq!(
            header.merkle_root.to_string(),
            "e0028eb9648db56b1ac77cf090b99048a8007e2bb64b68f092c03c7f56a662c7"
        );
        assert_eq!(header.time, 1417713337);
        assert_eq!(header.bits, CompactTarget::from_consensus(0x207fffff));
        assert_eq!(header.nonce, 1096447);
        assert_eq!(
            header.block_hash().to_string(),
            "000008ca1832a4baf228eb1553c03d3a2c8e02399550dd6ea8d65cec3ef23d2e"
        );
    }

    #[test]
    fn regtest_genesis_header() {
        let header = genesis_header(Network::Regtest);
        assert_eq!(header.version, block::Version::ONE);
        assert_eq!(header.prev_blockhash, Hash::all_zeros());
        assert_eq!(
            header.merkle_root.to_string(),
            "e0028eb9648db56b1ac77cf090b99048a8007e2bb64b68f092c03c7f56a662c7"
        );
        assert_eq!(header.time, 1417713337);
        assert_eq!(header.bits, CompactTarget::from_consensus(0x207fffff));
        assert_eq!(header.nonce, 1096447);
        assert_eq!(
            header.block_hash().to_string(),
            "000008ca1832a4baf228eb1553c03d3a2c8e02399550dd6ea8d65cec3ef23d2e"
        );
    }
}
