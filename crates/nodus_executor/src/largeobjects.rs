//! Large objects: the `lo_*`, `loread`, and `lowrite` family, over the
//! engine's keys so its writes roll back with the transaction, and the
//! descriptors a session opens on them.
//!
//! An object's bytes live in pages of [`PAGE_SIZE`] (as
//! `pg_largeobject`'s rows), its size and owner in a metadata key (as
//! `pg_largeobject_metadata`'s row).

use crate::MemExecutor;
use anyhow::Result;
use bytes::Bytes;
use std::collections::BTreeMap;

use crate::Value;

/// The page size `pg_largeobject` divides objects into.
pub(crate) const PAGE_SIZE: i64 = 2048;

/// `INV_WRITE` of `lo_open`.
pub(crate) const INV_WRITE: i64 = 0x20000;
/// `INV_READ` of `lo_open`.
pub(crate) const INV_READ: i64 = 0x40000;

/// `MAX_LARGE_OBJECT_SIZE`: what a seek target, a truncation length, and a
/// write's end are bounded by.
const MAX_LARGE_OBJECT_SIZE: i64 = i32::MAX as i64 * PAGE_SIZE;

/// The owner every large object gets: the bootstrap superuser.
const SUPERUSER_OID: i64 = 10;
/// The key the next large object's OID counts up from.
const OID_COUNTER: &str = "lo_oid";

/// One open descriptor: what `lo_open` returned an index for.
pub(crate) struct Descriptor {
    oid: i64,
    flags: i64,
    offset: i64,
    /// The transaction that opened it; its end closes the descriptor.
    txn: Option<nodus_storage_api::TxnId>,
}

/// A session's descriptors, by the descriptor number `lo_open` returns.
pub(crate) type Descriptors = Vec<Option<Descriptor>>;

fn meta_key(oid: i64) -> String {
    format!("lo:{oid}")
}

fn page_key(oid: i64, page: i64) -> String {
    format!("lo:{oid}:{page:07}")
}

/// An object's metadata: its size and owner.
fn meta_text(size: i64) -> String {
    format!("{{\"owner\":{SUPERUSER_OID},\"size\":{size}}}")
}

fn meta_size(meta: &str) -> i64 {
    let Some(at) = meta.find("\"size\":") else {
        return 0;
    };
    meta[at + 7..]
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>()
        .parse()
        .unwrap_or(0)
}

impl MemExecutor {
    /// A key as the session's transaction sees it: its own writes first,
    /// then the snapshot.
    fn lo_read_key(&self, session: &str, key: &str) -> Result<Option<String>> {
        if let Some(txn) = self.active_txns.read().get(session)
            && let Some(pending) = txn.overlay.get(key)
        {
            return Ok(pending.clone());
        }
        let read_ts = self.read_ts(session);
        Ok(self
            .kv
            .get(&Bytes::from(key.to_string()), read_ts)?
            .map(|bytes| String::from_utf8_lossy(&bytes).to_string()))
    }

    fn lo_write_key(&self, session: &str, key: &str, value: String) -> Result<()> {
        self.write_row(session, key.to_string(), value)
    }

    /// The pages of one object, low to high: `(page number, bytes)`.
    pub(crate) fn lo_pages(&self, session: &str, oid: i64) -> Result<Vec<(i64, Vec<u8>)>> {
        let read_ts = self.read_ts(session);
        let mut pages = self.lo_pages_at(read_ts, oid)?;
        let prefix = format!("lo:{oid}:");
        // The transaction's own writes come last.
        if let Some(txn) = self.active_txns.read().get(session) {
            for (key, value) in txn.overlay.range(prefix.clone()..format!("lo:{oid};")) {
                let page: i64 = key[prefix.len()..].parse().unwrap_or(0);
                match value {
                    Some(hex) => {
                        pages.insert(page, decode_hex(hex));
                    }
                    None => {
                        pages.remove(&page);
                    }
                }
            }
        }
        Ok(pages.into_iter().collect())
    }

    /// Every object's OID, in order.
    pub(crate) fn lo_list(&self, session: &str) -> Result<Vec<i64>> {
        let read_ts = self.read_ts(session);
        let mut oids: BTreeMap<i64, ()> = self
            .lo_list_at(read_ts)?
            .into_iter()
            .map(|oid| (oid, ()))
            .collect();
        if let Some(txn) = self.active_txns.read().get(session) {
            for (key, value) in txn.overlay.range("lo:".to_string().."lo;".to_string()) {
                let rest = &key["lo:".len()..];
                if rest.contains(':') {
                    continue;
                }
                match value {
                    Some(_) => {
                        if let Ok(oid) = rest.parse() {
                            oids.insert(oid, ());
                        }
                    }
                    None => {
                        if let Ok(oid) = rest.parse() {
                            oids.remove(&oid);
                        }
                    }
                }
            }
        }
        Ok(oids.into_keys().collect())
    }

    /// The pages of one object at a snapshot, without a session's own writes.
    fn lo_pages_at(
        &self,
        read_ts: nodus_storage_api::Timestamp,
        oid: i64,
    ) -> Result<BTreeMap<i64, Vec<u8>>> {
        let prefix = format!("lo:{oid}:");
        let range = nodus_storage_api::KeyRange {
            start: Bytes::from(prefix.clone()),
            end: Bytes::from(format!("lo:{oid};")),
        };
        let mut pages: BTreeMap<i64, Vec<u8>> = BTreeMap::new();
        for pair in self.kv.scan(range, read_ts)? {
            let pair = pair?;
            let key = String::from_utf8_lossy(&pair.key).to_string();
            let page: i64 = key[prefix.len()..].parse().unwrap_or(0);
            pages.insert(page, decode_hex(&String::from_utf8_lossy(&pair.value)));
        }
        Ok(pages)
    }

    /// Every object's OID at a snapshot, without a session's own writes.
    fn lo_list_at(&self, read_ts: nodus_storage_api::Timestamp) -> Result<Vec<i64>> {
        let range = nodus_storage_api::KeyRange {
            start: Bytes::from("lo:"),
            end: Bytes::from("lo;"),
        };
        let mut oids: BTreeMap<i64, ()> = BTreeMap::new();
        for pair in self.kv.scan(range, read_ts)? {
            let pair = pair?;
            let key = String::from_utf8_lossy(&pair.key).to_string();
            let rest = &key["lo:".len()..];
            if !rest.contains(':')
                && let Ok(oid) = rest.parse()
            {
                oids.insert(oid, ());
            }
        }
        Ok(oids.into_keys().collect())
    }

    /// An object's size, or `None` when it does not exist.
    pub(crate) fn lo_size(&self, session: &str, oid: i64) -> Result<Option<i64>> {
        Ok(self
            .lo_read_key(session, &meta_key(oid))?
            .map(|meta| meta_size(&meta)))
    }

    fn lo_require(&self, session: &str, oid: i64) -> Result<i64> {
        match self.lo_size(session, oid)? {
            Some(size) => Ok(size),
            None => anyhow::bail!(
                crate::error_fields::DbError::new(format!("large object {oid} does not exist"))
                    .code("42704")
                    .into_text()
            ),
        }
    }

    /// A fresh OID, counted up transactionally.
    fn lo_next_oid(&self, session: &str) -> Result<i64> {
        let next = match self.lo_read_key(session, OID_COUNTER)? {
            Some(text) => text.trim().parse::<i64>().unwrap_or(100_000) + 1,
            None => 100_001,
        };
        self.lo_write_key(session, OID_COUNTER, next.to_string())?;
        Ok(next)
    }

    /// `lo_create(oid)` / `lo_creat(0)`: a new object, or the OID taken.
    pub(crate) fn lo_create(&self, session: &str, oid: i64) -> Result<i64> {
        let oid = if oid == 0 {
            self.lo_next_oid(session)?
        } else {
            oid
        };
        if self.lo_size(session, oid)?.is_some() {
            anyhow::bail!(
                crate::error_fields::DbError::new(
                    "duplicate key value violates unique constraint \
                     \"pg_largeobject_metadata_oid_index\""
                )
                .code("23505")
                .detail(format!("Key (oid)=({oid}) already exists."))
                .into_text()
            );
        }
        self.lo_write_key(session, &meta_key(oid), meta_text(0))?;
        Ok(oid)
    }

    /// `lo_get(oid [, offset, nbytes])`: bytes from an object.
    pub(crate) fn lo_get(
        &self,
        session: &str,
        oid: i64,
        offset: i64,
        nbytes: Option<i64>,
    ) -> Result<Vec<u8>> {
        if let Some(nbytes) = nbytes
            && nbytes < 0
        {
            anyhow::bail!(
                crate::error_fields::DbError::new("requested length cannot be negative")
                    .code("22023")
                    .into_text()
            );
        }
        let size = self.lo_require(session, oid)?;
        if offset < 0 || offset > MAX_LARGE_OBJECT_SIZE {
            anyhow::bail!(seek_error(offset));
        }
        let length = match nbytes {
            Some(nbytes) if nbytes <= size - offset => nbytes,
            _ => (size - offset).max(0),
        };
        let mut out = Vec::with_capacity(length as usize);
        for (page, data) in self.lo_pages(session, oid)? {
            let start = page * PAGE_SIZE;
            if start >= offset + length {
                break;
            }
            let from = (offset - start).max(0) as usize;
            let to = ((offset + length - start).min(data.len() as i64)).max(0) as usize;
            if from < to {
                out.extend_from_slice(&data[from..to]);
            }
        }
        Ok(out)
    }

    /// `lo_put(oid, offset, data)`, and the writes of `lowrite`: the bytes
    /// at the offset, zero-filling a gap.
    pub(crate) fn lo_put(&self, session: &str, oid: i64, offset: i64, data: &[u8]) -> Result<()> {
        // PostgreSQL opens the object before it seeks.
        self.lo_require(session, oid)?;
        if offset < 0 || offset > MAX_LARGE_OBJECT_SIZE {
            anyhow::bail!(seek_error(offset));
        }
        self.lo_write_at(session, oid, offset, data)
    }

    fn lo_write_at(&self, session: &str, oid: i64, offset: i64, data: &[u8]) -> Result<()> {
        let size = self.lo_require(session, oid)?;
        if data.is_empty() {
            return Ok(());
        }
        if offset + data.len() as i64 > MAX_LARGE_OBJECT_SIZE {
            anyhow::bail!(
                crate::error_fields::DbError::new(format!(
                    "invalid large object write request size: {}",
                    data.len()
                ))
                .code("22023")
                .into_text()
            );
        }
        let end = offset + data.len() as i64;
        let first = offset / PAGE_SIZE;
        let last = (end - 1).div_euclid(PAGE_SIZE);
        for page in first..=last.max(first) {
            let start = page * PAGE_SIZE;
            let mut bytes = self
                .lo_read_key(session, &page_key(oid, page))?
                .map(|hex| decode_hex(&hex))
                .unwrap_or_default();
            // The part of `data` landing on this page, at its offset in
            // the page; anything before it is zeros.
            let data_from = (start - offset).max(0) as usize;
            let data_to = ((start + PAGE_SIZE - offset).min(data.len() as i64))
                .max(data_from as i64) as usize;
            let at = (offset - start).max(0) as usize;
            if data_from < data_to {
                let needed = at + (data_to - data_from);
                if bytes.len() < needed {
                    bytes.resize(needed, 0);
                }
                bytes[at..needed].copy_from_slice(&data[data_from..data_to]);
                self.lo_write_key(session, &page_key(oid, page), encode_hex(&bytes))?;
            }
        }
        if end > size {
            self.lo_write_key(session, &meta_key(oid), meta_text(end))?;
        }
        Ok(())
    }

    /// `lo_truncate`: shrink or (zero-filling) grow an object.
    pub(crate) fn lo_truncate(&self, session: &str, oid: i64, length: i64) -> Result<()> {
        let size = self.lo_require(session, oid)?;
        if length < size {
            let last = if length == 0 {
                -1
            } else {
                (length - 1) / PAGE_SIZE
            };
            for (page, _) in self.lo_pages(session, oid)? {
                if page > last {
                    self.delete_row(session, page_key(oid, page));
                }
            }
            if length > 0
                && let Some((page, mut data)) = self
                    .lo_pages(session, oid)?
                    .into_iter()
                    .find(|(page, _)| *page == last)
            {
                data.truncate((length - page * PAGE_SIZE) as usize);
                self.lo_write_key(session, &page_key(oid, page), encode_hex(&data))?;
            }
            self.lo_write_key(session, &meta_key(oid), meta_text(length))?;
        } else if length > size {
            self.lo_write_at(session, oid, size, &vec![0u8; (length - size) as usize])?;
        }
        Ok(())
    }

    /// `lo_unlink(oid)`: the object and its pages, while no descriptor
    /// stays open on it.
    pub(crate) fn lo_unlink(&self, session: &str, oid: i64) -> Result<i64> {
        self.lo_require(session, oid)?;
        for (page, _) in self.lo_pages(session, oid)? {
            self.delete_row(session, page_key(oid, page));
        }
        self.delete_row(session, meta_key(oid));
        let mut all = self.large_objects.lock();
        if let Some(descriptors) = all.get_mut(session) {
            for descriptor in descriptors.iter_mut() {
                if descriptor.as_ref().is_some_and(|d| d.oid == oid) {
                    *descriptor = None;
                }
            }
        }
        Ok(1)
    }

    /// `lo_open(oid, flags)`: a descriptor number for the session.
    pub(crate) fn lo_open(
        &self,
        session: &str,
        oid: i64,
        flags: i64,
        txn: Option<nodus_storage_api::TxnId>,
    ) -> Result<i64> {
        // PostgreSQL refuses a mode with neither `INV_READ` nor `INV_WRITE`
        // set, before it looks at the object.
        if flags & (INV_READ | INV_WRITE) == 0 {
            anyhow::bail!(
                crate::error_fields::DbError::new(format!(
                    "invalid flags for opening a large object: {flags}"
                ))
                .code("22023")
                .into_text()
            );
        }
        self.lo_require(session, oid)?;
        let mut all = self.large_objects.lock();
        let descriptors = all.entry(session.to_string()).or_default();
        let at = descriptors
            .iter()
            .position(Option::is_none)
            .unwrap_or(descriptors.len());
        if at == descriptors.len() {
            descriptors.push(None);
        }
        descriptors[at] = Some(Descriptor {
            oid,
            flags,
            offset: 0,
            txn,
        });
        Ok(at as i64)
    }

    fn lo_with_descriptor<R>(
        &self,
        session: &str,
        fd: i64,
        use_it: impl FnOnce(&mut Descriptor) -> Result<R>,
    ) -> Result<R> {
        let mut all = self.large_objects.lock();
        let found = all
            .get_mut(session)
            .and_then(|descriptors| descriptors.get_mut(fd.max(0) as usize))
            .and_then(Option::as_mut);
        let Some(descriptor) = found else {
            anyhow::bail!(
                crate::error_fields::DbError::new(format!("invalid large-object descriptor: {fd}"))
                    .code("42704")
                    .into_text()
            );
        };
        use_it(descriptor)
    }

    /// `lo_close(fd)`.
    pub(crate) fn lo_close(&self, session: &str, fd: i64) -> Result<()> {
        let mut all = self.large_objects.lock();
        let found = all
            .get_mut(session)
            .and_then(|descriptors| descriptors.get_mut(fd.max(0) as usize))
            .and_then(Option::take);
        if found.is_none() {
            anyhow::bail!(
                crate::error_fields::DbError::new(format!("invalid large-object descriptor: {fd}"))
                    .code("42704")
                    .into_text()
            );
        }
        Ok(())
    }

    /// `lo_lseek(fd, offset, whence)`, returning the new position.
    pub(crate) fn lo_lseek(&self, session: &str, fd: i64, offset: i64, whence: i64) -> Result<i64> {
        self.lo_with_descriptor(session, fd, |descriptor| {
            let size = self.lo_require(session, descriptor.oid)?;
            let target = match whence {
                0 => offset,
                1 => descriptor.offset + offset,
                2 => size + offset,
                other => {
                    anyhow::bail!(
                        crate::error_fields::DbError::new(format!(
                            "invalid whence setting: {other}"
                        ))
                        .code("22023")
                        .into_text()
                    );
                }
            };
            if target < 0 || target > MAX_LARGE_OBJECT_SIZE {
                anyhow::bail!(seek_error(target));
            }
            descriptor.offset = target;
            Ok(target)
        })
    }

    /// `lo_tell(fd)`.
    pub(crate) fn lo_tell(&self, session: &str, fd: i64) -> Result<i64> {
        self.lo_with_descriptor(session, fd, |descriptor| Ok(descriptor.offset))
    }

    /// `loread(fd, len)`: bytes from the descriptor's position.
    pub(crate) fn lo_read(&self, session: &str, fd: i64, len: i64) -> Result<Vec<u8>> {
        self.lo_with_descriptor(session, fd, |descriptor| {
            // A descriptor opened for writing can be read from, as
            // PostgreSQL grants `INV_WRITE` the read lock too.
            if descriptor.flags & (INV_READ | INV_WRITE) == 0 {
                anyhow::bail!(
                    crate::error_fields::DbError::new(format!(
                        "large object descriptor {fd} was not opened for reading"
                    ))
                    .code("55000")
                    .into_text()
                );
            }
            let offset = descriptor.offset;
            let len = len.max(0);
            let data = self.lo_get(session, descriptor.oid, offset, Some(len))?;
            descriptor.offset += data.len() as i64;
            Ok(data)
        })
    }

    /// `lowrite(fd, data)`: bytes at the descriptor's position.
    pub(crate) fn lo_write(&self, session: &str, fd: i64, data: &[u8]) -> Result<i64> {
        self.lo_with_descriptor(session, fd, |descriptor| {
            if descriptor.flags & INV_WRITE == 0 {
                anyhow::bail!(
                    crate::error_fields::DbError::new(format!(
                        "large object descriptor {fd} was not opened for writing"
                    ))
                    .code("55000")
                    .into_text()
                );
            }
            let offset = descriptor.offset;
            self.lo_put(session, descriptor.oid, offset, data)?;
            descriptor.offset = offset + data.len() as i64;
            Ok(data.len() as i64)
        })
    }

    /// `lo_truncate(fd, len)`.
    pub(crate) fn lo_truncate_fd(&self, session: &str, fd: i64, length: i64) -> Result<()> {
        self.lo_with_descriptor(session, fd, |descriptor| {
            if descriptor.flags & INV_WRITE == 0 {
                anyhow::bail!(
                    crate::error_fields::DbError::new(format!(
                        "large object descriptor {fd} was not opened for writing"
                    ))
                    .code("55000")
                    .into_text()
                );
            }
            if length < 0 || length > MAX_LARGE_OBJECT_SIZE {
                anyhow::bail!(
                    crate::error_fields::DbError::new(format!(
                        "invalid large object truncation target: {length}"
                    ))
                    .code("22023")
                    .into_text()
                );
            }
            let oid = descriptor.oid;
            self.lo_truncate(session, oid, length)
        })
    }

    /// A transaction's end: its descriptors close, as PostgreSQL's cookies
    /// do.
    pub(crate) fn end_transaction_large_objects(&self, session: &str) {
        self.large_objects.lock().remove(session);
    }
}

fn seek_error(offset: i64) -> String {
    crate::error_fields::DbError::new(format!("invalid large object seek target: {offset}"))
        .code("22023")
        .into_text()
}

/// Bytes as the hex text a page is stored as.
pub(crate) fn encode_hex(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len() * 2);
    for byte in data {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// The bytes a page's hex text holds.
pub(crate) fn decode_hex(text: &str) -> Vec<u8> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks(2) {
        if pair.len() == 2
            && let Ok(byte) = u8::from_str_radix(&String::from_utf8_lossy(pair), 16)
        {
            out.push(byte);
        }
    }
    out
}
