//! Diagnostic tool to inspect block segment files and find deserialization issues.
//!
//! Usage: cargo run --example diagnose_storage -- /path/to/spv/testnet/blocks

use std::env;
use std::fs::{self, File};
use std::io::{BufReader, Read, Seek, SeekFrom};

use dashcore::consensus::{Decodable, Encodable};
use dashcore::{Block, BlockHash};

fn main() {
    let args: Vec<String> = env::args().collect();
    let blocks_dir = args
        .get(1)
        .expect("Usage: diagnose_storage <path-to-blocks-dir>");

    println!("Inspecting block segments in: {}", blocks_dir);

    let mut entries: Vec<_> = fs::read_dir(blocks_dir)
        .expect("Failed to read blocks directory")
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with("segment_")
        })
        .collect();
    entries.sort_by_key(|e| e.file_name());

    for entry in &entries {
        let path = entry.path();
        let file_name = entry.file_name();
        let file_size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

        println!("\n--- {} ({} bytes) ---", file_name.to_string_lossy(), file_size);

        let file = File::open(&path).expect("Failed to open segment file");
        let mut reader = BufReader::new(file);

        let mut item_index = 0;
        let mut last_good_pos: u64 = 0;
        let mut sentinel_count = 0;
        let mut block_count = 0;

        // Pre-compute the sentinel for comparison
        let sentinel_hash = sentinel_block_hash();

        loop {
            let pos = reader.stream_position().unwrap_or(0);

            // Try to decode a HashedBlock: BlockHash (32 bytes) + Block
            match decode_hashed_block(&mut reader) {
                Ok((hash, block)) => {
                    let is_sentinel = hash == sentinel_hash;
                    if is_sentinel {
                        sentinel_count += 1;
                    } else {
                        block_count += 1;
                        println!(
                            "  [{}] block at offset {}: hash={}, txs={}, version={}",
                            item_index,
                            pos,
                            hash,
                            block.txdata.len(),
                            block.header.version.to_consensus(),
                        );
                        for (tx_idx, tx) in block.txdata.iter().enumerate() {
                            println!(
                                "    tx[{}]: version={}, type={:?}, inputs={}, outputs={}",
                                tx_idx,
                                tx.version,
                                tx.special_transaction_payload
                                    .as_ref()
                                    .map(|p| p.get_type())
                                    .unwrap_or(
                                        dashcore::blockdata::transaction::TransactionType::Classic
                                    ),
                                tx.input.len(),
                                tx.output.len(),
                            );

                            // Round-trip test: encode then decode
                            let mut encoded = Vec::new();
                            tx.consensus_encode(&mut encoded).unwrap();
                            match dashcore::Transaction::consensus_decode(&mut &encoded[..]) {
                                Ok(_) => {}
                                Err(e) => {
                                    println!(
                                        "    ** round-trip FAILED for tx[{}]: {}",
                                        tx_idx, e
                                    );
                                    println!(
                                        "    ** encoded bytes (first 64): {:02x?}",
                                        &encoded[..encoded.len().min(64)]
                                    );
                                }
                            }
                        }
                    }
                    last_good_pos = reader.stream_position().unwrap_or(0);
                    item_index += 1;
                }
                Err(ref e)
                    if e.to_string().contains("UnexpectedEof")
                        || e.to_string().contains("unexpected end") =>
                {
                    // Normal EOF
                    break;
                }
                Err(e) => {
                    let err_pos = reader.stream_position().unwrap_or(0);
                    println!(
                        "  [{}] DECODE ERROR at offset {} (started at {}): {}",
                        item_index, err_pos, pos, e
                    );

                    // Dump bytes around the error position
                    reader.seek(SeekFrom::Start(pos)).ok();
                    let mut bytes = vec![0u8; 128];
                    let n = reader.read(&mut bytes).unwrap_or(0);
                    bytes.truncate(n);
                    println!("    raw bytes at item start ({}): {:02x?}", pos, bytes);

                    // Also show what the last good position looks like
                    if last_good_pos < pos {
                        reader.seek(SeekFrom::Start(last_good_pos)).ok();
                        let mut prev_bytes = vec![0u8; 128];
                        let n = reader.read(&mut prev_bytes).unwrap_or(0);
                        prev_bytes.truncate(n);
                        println!(
                            "    raw bytes at last good end ({}): {:02x?}",
                            last_good_pos, prev_bytes
                        );
                    }

                    // Try to continue by scanning for the next item
                    break;
                }
            }
        }

        println!(
            "  Summary: {} blocks, {} sentinels, {} total items",
            block_count,
            sentinel_count,
            item_index,
        );
    }
}

fn sentinel_block_hash() -> BlockHash {
    use dashcore::block::{Header, Version};
    use dashcore::CompactTarget;
    use dashcore_hashes::Hash;

    let header = Header {
        version: Version::from_consensus(i32::MAX),
        prev_blockhash: BlockHash::from_byte_array([0xFF; 32]),
        merkle_root: dashcore::hashes::sha256d::Hash::from_byte_array([0xFF; 32]).into(),
        time: u32::MAX,
        bits: CompactTarget::from_consensus(0xFFFFFFFF),
        nonce: u32::MAX,
    };

    let block = Block {
        header,
        txdata: Vec::new(),
    };
    block.block_hash()
}

fn decode_hashed_block(reader: &mut BufReader<File>) -> Result<(BlockHash, Block), dashcore::consensus::encode::Error> {
    let hash = BlockHash::consensus_decode(reader)?;
    let block = Block::consensus_decode(reader)?;
    Ok((hash, block))
}
