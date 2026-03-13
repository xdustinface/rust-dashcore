//! Bloom filter builder for wallet addresses and outpoints.
//!
//! Builds BIP37 bloom filters from wallet data for peer-side transaction filtering.

use dashcore::address::Payload;
use dashcore::bloom::{BloomError, BloomFilter};
use dashcore::consensus::Encodable;
use dashcore::network::message_bloom::{BloomFlags, FilterLoad};
use dashcore::{Address, OutPoint};

/// Extract the raw hash payload bytes from an address for bloom filter insertion.
fn address_payload_bytes(addr: &Address) -> Option<Vec<u8>> {
    match addr.payload() {
        Payload::PubkeyHash(hash) => Some(<[u8; 20]>::from(*hash).to_vec()),
        Payload::ScriptHash(hash) => Some(<[u8; 20]>::from(*hash).to_vec()),
        Payload::WitnessProgram(prog) => Some(prog.program().as_bytes().to_vec()),
        _ => {
            tracing::warn!("skipping unknown address type for bloom filter: {:?}", addr);
            None
        }
    }
}

/// Build a bloom filter from wallet addresses and outpoints.
///
/// Addresses are inserted as their raw hash payload bytes (20-byte hash160
/// for P2PKH/P2SH). This matches what Dash Core's `CheckScript` extracts as
/// data pushes from scriptPubKeys.
///
/// Outpoints are inserted as consensus-serialized bytes (`txid || vout_le`)
/// to detect transactions spending our UTXOs.
pub(super) fn build_wallet_bloom_filter(
    addresses: &[Address],
    outpoints: &[OutPoint],
    false_positive_rate: f64,
    tweak: u32,
) -> Result<FilterLoad, BloomError> {
    let element_count = addresses.len() + outpoints.len();
    if element_count == 0 {
        let filter = BloomFilter::new(1, false_positive_rate, tweak, BloomFlags::All)?;
        return Ok(FilterLoad::from_bloom_filter(&filter));
    }

    let mut filter =
        BloomFilter::new(element_count as u32, false_positive_rate, tweak, BloomFlags::All)?;

    for addr in addresses {
        if let Some(payload) = address_payload_bytes(addr) {
            filter.insert(&payload);
        }
    }

    for outpoint in outpoints {
        let mut buf = Vec::new();
        outpoint.consensus_encode(&mut buf).expect("outpoint serialization to vec cannot fail");
        filter.insert(&buf);
    }

    Ok(FilterLoad::from_bloom_filter(&filter))
}

#[cfg(test)]
mod tests {
    use std::slice;

    use super::*;
    use dashcore::hashes::Hash;
    use dashcore::{Network, Txid};

    #[test]
    fn test_build_filter_from_addresses() {
        let addr = Address::dummy(Network::Testnet, 0);
        let filter_load =
            build_wallet_bloom_filter(slice::from_ref(&addr), &[], 0.0005, 0).unwrap();

        let filter = filter_load.to_bloom_filter().unwrap();
        assert!(filter.contains(&address_payload_bytes(&addr).unwrap()));
    }

    #[test]
    fn test_build_filter_from_outpoints() {
        let outpoint = OutPoint {
            txid: Txid::from_byte_array([1u8; 32]),
            vout: 0,
        };
        let filter_load = build_wallet_bloom_filter(&[], &[outpoint], 0.0005, 0).unwrap();

        let filter = filter_load.to_bloom_filter().unwrap();
        let mut buf = Vec::new();
        outpoint.consensus_encode(&mut buf).unwrap();
        assert!(filter.contains(&buf));
    }

    #[test]
    fn test_build_filter_mixed() {
        let addr = Address::dummy(Network::Testnet, 0);
        let outpoint = OutPoint {
            txid: Txid::from_byte_array([2u8; 32]),
            vout: 1,
        };

        let filter_load =
            build_wallet_bloom_filter(slice::from_ref(&addr), &[outpoint], 0.0005, 42).unwrap();
        let filter = filter_load.to_bloom_filter().unwrap();

        assert!(filter.contains(&address_payload_bytes(&addr).unwrap()));

        let mut buf = Vec::new();
        outpoint.consensus_encode(&mut buf).unwrap();
        assert!(filter.contains(&buf));

        assert!(!filter.contains(&[0xff; 20]));
    }

    #[test]
    fn test_build_filter_empty_inputs() {
        let filter_load = build_wallet_bloom_filter(&[], &[], 0.0005, 0).unwrap();
        let filter = filter_load.to_bloom_filter().unwrap();
        assert!(!filter.contains(&[1, 2, 3]));
    }

    #[test]
    fn test_build_filter_flags() {
        let addr = Address::dummy(Network::Testnet, 0);
        let filter_load = build_wallet_bloom_filter(&[addr], &[], 0.0005, 0).unwrap();
        assert_eq!(filter_load.flags, BloomFlags::All);
    }

    #[test]
    fn test_build_filter_tweak() {
        let addr = Address::dummy(Network::Testnet, 0);
        let filter_load = build_wallet_bloom_filter(&[addr], &[], 0.0005, 12345).unwrap();
        assert_eq!(filter_load.tweak, 12345);
    }

    #[test]
    fn test_unmatched_address_not_in_filter() {
        let addr1 = Address::dummy(Network::Testnet, 0);
        let addr2 = Address::dummy(Network::Testnet, 1);

        let filter_load = build_wallet_bloom_filter(&[addr1], &[], 0.0005, 0).unwrap();
        let filter = filter_load.to_bloom_filter().unwrap();

        assert!(!filter.contains(&address_payload_bytes(&addr2).unwrap()));
    }

    #[test]
    fn test_build_filter_rejects_invalid_fp_rates() {
        let addr = Address::dummy(Network::Testnet, 0);
        let addrs = slice::from_ref(&addr);

        assert!(build_wallet_bloom_filter(addrs, &[], 0.0, 0).is_err());
        assert!(build_wallet_bloom_filter(addrs, &[], -0.5, 0).is_err());
        assert!(build_wallet_bloom_filter(addrs, &[], 1.0, 0).is_err());
        assert!(build_wallet_bloom_filter(addrs, &[], 1.5, 0).is_err());
    }

    #[test]
    fn test_build_filter_with_extreme_fp_rates() {
        let addr = Address::dummy(Network::Testnet, 0);
        let addrs = slice::from_ref(&addr);
        let payload = address_payload_bytes(&addr).unwrap();

        // Very small rate: produces a larger filter but still works
        let filter =
            build_wallet_bloom_filter(addrs, &[], 0.000001, 0).unwrap().to_bloom_filter().unwrap();
        assert!(filter.contains(&payload));

        // Large rate near upper bound: produces a tiny filter but still works
        let filter =
            build_wallet_bloom_filter(addrs, &[], 0.999, 0).unwrap().to_bloom_filter().unwrap();
        assert!(filter.contains(&payload));
    }
}
