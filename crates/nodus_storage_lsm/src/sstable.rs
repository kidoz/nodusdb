//! Block-based, indexed SSTable.
//!
//! On-disk layout (format v2):
//! ```text
//!   [data region]   contiguous entries, key-sorted, grouped into ~4 KiB blocks
//!   [index block]   one (first_key, offset, len) per data block — sparse index
//!   [bloom block]   bloom filter over all keys
//!   [footer]        fixed 32 bytes locating the index and bloom blocks
//! ```
//! Each entry is `[key_len: u32][key][val_len: u32][bincode(VersionChain)]`.
//!
//! `get` reads the footer, checks the bloom filter (skip the file entirely if the
//! key is definitely absent), binary-searches the index for the one block that
//! could hold the key, and scans just that block — O(log #blocks) seeks instead
//! of the previous O(#entries) full-file linear scan, with compact binary values
//! instead of JSON.

use anyhow::{Result, bail};
use bytes::Bytes;
use nodus_mvcc::VersionChain;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MAGIC: u32 = 0x4E4F_5353; // "NOSS"
const FORMAT_VERSION: u32 = 2;
/// Target uncompressed size of a data block before starting a new one.
const BLOCK_SIZE: usize = 4096;
/// `index_offset(u64) index_len(u32) bloom_offset(u64) bloom_len(u32) magic(u32) version(u32)`.
const FOOTER_LEN: u64 = 32;

#[derive(Debug)]
pub struct Sstable {
    pub path: PathBuf,
}

/// Sparse index entry: a data block's first key and byte extent.
struct BlockHandle {
    first_key: Vec<u8>,
    offset: u64,
    len: u32,
}

/// FNV-1a (64-bit) — a fast, *stable-across-process* hash, required because the
/// bloom filter is persisted and re-read after restart.
fn fnv1a(data: &[u8], seed: u64) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325 ^ seed;
    for &b in data {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// A simple bloom filter with double-hashing.
struct Bloom {
    num_bits: u64,
    num_hashes: u32,
    bits: Vec<u8>,
}

impl Bloom {
    fn new(num_keys: usize) -> Self {
        // ~10 bits/key keeps the false-positive rate around 1%.
        let num_bits = std::cmp::max(64, num_keys.saturating_mul(10)) as u64;
        Self {
            num_bits,
            num_hashes: 7,
            bits: vec![0u8; (num_bits as usize).div_ceil(8)],
        }
    }

    fn positions(&self, key: &[u8]) -> impl Iterator<Item = usize> + '_ {
        let h1 = fnv1a(key, 0);
        let h2 = fnv1a(key, 0x9e37_79b9_7f4a_7c15) | 1; // odd, so it strides the table
        (0..self.num_hashes as u64)
            .map(move |i| (h1.wrapping_add(i.wrapping_mul(h2)) % self.num_bits) as usize)
    }

    fn add(&mut self, key: &[u8]) {
        let positions: Vec<usize> = self.positions(key).collect();
        for pos in positions {
            self.bits[pos / 8] |= 1 << (pos % 8);
        }
    }

    fn maybe_contains(&self, key: &[u8]) -> bool {
        self.positions(key)
            .all(|pos| self.bits[pos / 8] & (1 << (pos % 8)) != 0)
    }

    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(12 + self.bits.len());
        out.extend_from_slice(&self.num_bits.to_le_bytes());
        out.extend_from_slice(&self.num_hashes.to_le_bytes());
        out.extend_from_slice(&self.bits);
        out
    }

    fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < 12 {
            bail!("corrupt bloom block");
        }
        let num_bits = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let num_hashes = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        Ok(Self {
            num_bits,
            num_hashes,
            bits: buf[12..].to_vec(),
        })
    }
}

fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn read_range(file: &mut File, offset: u64, len: usize) -> Result<Vec<u8>> {
    file.seek(SeekFrom::Start(offset))?;
    let mut buf = vec![0u8; len];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

impl Sstable {
    pub fn build<P: AsRef<Path>>(
        path: P,
        data: &std::collections::BTreeMap<Bytes, VersionChain>,
    ) -> Result<Self> {
        let path = path.as_ref();

        // Build the whole file image in memory (the source is the memtable, which
        // is already memory-resident), then publish it atomically.
        let mut image = Self::encode(data)?;

        // Atomic durable publish: temp → fsync → rename → fsync dir. A crash
        // mid-build leaves only the `.tmp`, which recovery ignores.
        let tmp_path = path.with_extension("sst.tmp");
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(&image)?;
            file.sync_all()?;
        }
        image.clear();
        std::fs::rename(&tmp_path, path)?;
        if let Some(dir) = path.parent()
            && let Ok(dir_file) = File::open(dir)
        {
            let _ = dir_file.sync_all();
        }

        Ok(Self {
            path: path.to_path_buf(),
        })
    }

    /// Serializes `data` into the v2 file image.
    fn encode(data: &std::collections::BTreeMap<Bytes, VersionChain>) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        let mut index: Vec<BlockHandle> = Vec::new();
        let mut bloom = Bloom::new(data.len());

        let mut block_start = 0usize;
        let mut block_first_key: Option<Vec<u8>> = None;

        for (key, chain) in data {
            bloom.add(key);
            if block_first_key.is_none() {
                block_first_key = Some(key.to_vec());
                block_start = out.len();
            }

            put_u32(&mut out, key.len() as u32);
            out.extend_from_slice(key);
            let val = bincode::serialize(chain)?;
            put_u32(&mut out, val.len() as u32);
            out.extend_from_slice(&val);

            if out.len() - block_start >= BLOCK_SIZE {
                index.push(BlockHandle {
                    first_key: block_first_key.take().unwrap(),
                    offset: block_start as u64,
                    len: (out.len() - block_start) as u32,
                });
            }
        }
        if let Some(first_key) = block_first_key.take() {
            index.push(BlockHandle {
                first_key,
                offset: block_start as u64,
                len: (out.len() - block_start) as u32,
            });
        }

        let index_offset = out.len() as u64;
        put_u32(&mut out, index.len() as u32);
        for bh in &index {
            put_u32(&mut out, bh.first_key.len() as u32);
            out.extend_from_slice(&bh.first_key);
            out.extend_from_slice(&bh.offset.to_le_bytes());
            put_u32(&mut out, bh.len);
        }
        let index_len = (out.len() as u64 - index_offset) as u32;

        let bloom_offset = out.len() as u64;
        out.extend_from_slice(&bloom.encode());
        let bloom_len = (out.len() as u64 - bloom_offset) as u32;

        out.extend_from_slice(&index_offset.to_le_bytes());
        put_u32(&mut out, index_len);
        out.extend_from_slice(&bloom_offset.to_le_bytes());
        put_u32(&mut out, bloom_len);
        put_u32(&mut out, MAGIC);
        put_u32(&mut out, FORMAT_VERSION);

        Ok(out)
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    fn read_footer(file: &mut File) -> Result<Option<Footer>> {
        let file_len = file.seek(SeekFrom::End(0))?;
        if file_len < FOOTER_LEN {
            return Ok(None);
        }
        let buf = read_range(file, file_len - FOOTER_LEN, FOOTER_LEN as usize)?;
        let magic = u32::from_le_bytes(buf[24..28].try_into().unwrap());
        let version = u32::from_le_bytes(buf[28..32].try_into().unwrap());
        if magic != MAGIC || version != FORMAT_VERSION {
            bail!("unrecognized sstable format (magic={magic:#x}, version={version})");
        }
        Ok(Some(Footer {
            index_offset: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            index_len: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            bloom_offset: u64::from_le_bytes(buf[12..20].try_into().unwrap()),
            bloom_len: u32::from_le_bytes(buf[20..24].try_into().unwrap()),
        }))
    }

    fn read_index(file: &mut File, footer: &Footer) -> Result<Vec<BlockHandle>> {
        let buf = read_range(file, footer.index_offset, footer.index_len as usize)?;
        let mut cur = 0usize;
        let take_u32 = |buf: &[u8], cur: &mut usize| -> u32 {
            let v = u32::from_le_bytes(buf[*cur..*cur + 4].try_into().unwrap());
            *cur += 4;
            v
        };
        let num_blocks = take_u32(&buf, &mut cur);
        let mut index = Vec::with_capacity(num_blocks as usize);
        for _ in 0..num_blocks {
            let klen = take_u32(&buf, &mut cur) as usize;
            let first_key = buf[cur..cur + klen].to_vec();
            cur += klen;
            let offset = u64::from_le_bytes(buf[cur..cur + 8].try_into().unwrap());
            cur += 8;
            let len = take_u32(&buf, &mut cur);
            index.push(BlockHandle {
                first_key,
                offset,
                len,
            });
        }
        Ok(index)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<VersionChain>> {
        let mut file = match File::open(&self.path) {
            Ok(f) => f,
            Err(_) => return Ok(None),
        };
        let Some(footer) = Self::read_footer(&mut file)? else {
            return Ok(None);
        };

        // Bloom: skip the whole file if the key is definitely not present.
        let bloom_buf = read_range(&mut file, footer.bloom_offset, footer.bloom_len as usize)?;
        if !Bloom::decode(&bloom_buf)?.maybe_contains(key) {
            return Ok(None);
        }

        // Locate the one block that could contain the key: the last block whose
        // first key is <= the search key.
        let index = Self::read_index(&mut file, &footer)?;
        let candidate = index.partition_point(|bh| bh.first_key.as_slice() <= key);
        if candidate == 0 {
            return Ok(None);
        }
        let block = &index[candidate - 1];

        // Scan just that block.
        let buf = read_range(&mut file, block.offset, block.len as usize)?;
        let mut cur = 0usize;
        while cur + 4 <= buf.len() {
            let klen = u32::from_le_bytes(buf[cur..cur + 4].try_into().unwrap()) as usize;
            cur += 4;
            let entry_key = &buf[cur..cur + klen];
            cur += klen;
            let vlen = u32::from_le_bytes(buf[cur..cur + 4].try_into().unwrap()) as usize;
            cur += 4;
            if entry_key == key {
                let chain: VersionChain = bincode::deserialize(&buf[cur..cur + vlen])?;
                return Ok(Some(chain));
            }
            cur += vlen;
        }
        Ok(None)
    }

    pub fn iter(&self) -> Result<SstableIterator> {
        let mut file = File::open(&self.path)?;
        let data_end = match Self::read_footer(&mut file)? {
            Some(footer) => footer.index_offset,
            None => 0,
        };
        file.seek(SeekFrom::Start(0))?;
        Ok(SstableIterator {
            file,
            position: 0,
            data_end,
        })
    }
}

struct Footer {
    index_offset: u64,
    index_len: u32,
    bloom_offset: u64,
    bloom_len: u32,
}

/// Iterates the data region in key order (blocks are written in sorted order).
pub struct SstableIterator {
    file: File,
    position: u64,
    data_end: u64,
}

impl Iterator for SstableIterator {
    type Item = Result<(Bytes, VersionChain)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.data_end {
            return None;
        }
        let mut read = |n: usize| -> std::io::Result<Vec<u8>> {
            let mut b = vec![0u8; n];
            self.file.read_exact(&mut b)?;
            Ok(b)
        };
        let result = (|| -> Result<(Bytes, VersionChain)> {
            let klen = u32::from_le_bytes(read(4)?.try_into().unwrap()) as usize;
            let key = read(klen)?;
            let vlen = u32::from_le_bytes(read(4)?.try_into().unwrap()) as usize;
            let val = read(vlen)?;
            self.position += (8 + klen + vlen) as u64;
            Ok((Bytes::from(key), bincode::deserialize(&val)?))
        })();
        Some(result)
    }
}
