//! Full-text search: the `tsvector` and `tsquery` types with PostgreSQL's
//! literals, canonical forms, comparisons, and operators.
//!
//! Values are kept as canonical text, like the other structured families; the
//! declared type makes the text a lexeme vector or a query. The model follows
//! PostgreSQL's storage and algorithms closely, because its comparisons (and
//! so `ORDER BY`, `DISTINCT`, and `GROUP BY`) depend on the stored byte sizes
//! and on the query tree's shape.

use std::cmp::Ordering;

// ---------------------------------------------------------------- limits

/// The longest lexeme (`WordEntry.len` is 11 bits).
const MAX_STRLEN: usize = (1 << 11) - 1;
/// The largest lexeme storage (`WordEntry.pos` is 20 bits).
const MAX_STRPOS: usize = (1 << 20) - 1;
/// The most positions one lexeme keeps.
const MAX_NUM_POS: usize = 256;
/// One past the largest storable position (`WordEntryPos.pos` is 14 bits).
const MAX_ENTRY_POS: i64 = 1 << 14;

fn limit_pos(position: i64) -> i64 {
    if position >= MAX_ENTRY_POS {
        MAX_ENTRY_POS - 1
    } else {
        position
    }
}

/// Whether a declared type names `tsvector`.
pub(crate) fn is_tsvector_type(data_type: &str) -> bool {
    let text = data_type.trim();
    let text = text
        .strip_prefix("pg_catalog.")
        .or_else(|| text.strip_prefix("PG_CATALOG."))
        .unwrap_or(text);
    text.eq_ignore_ascii_case("tsvector")
}

/// Whether a declared type names `tsquery`.
pub(crate) fn is_tsquery_type(data_type: &str) -> bool {
    let text = data_type.trim();
    let text = text
        .strip_prefix("pg_catalog.")
        .or_else(|| text.strip_prefix("PG_CATALOG."))
        .unwrap_or(text);
    text.eq_ignore_ascii_case("tsquery")
}

fn db(message: impl Into<String>, code: &str) -> String {
    crate::error_fields::DbError::new(message)
        .code(code)
        .into_text()
}

/// PostgreSQL's `tsCompareString`: bytewise, so that `prefix` reports
/// whether the left string is a prefix of the right.
pub(crate) fn ts_compare_string(a: &[u8], b: &[u8], prefix: bool) -> Ordering {
    let n = a.len().min(b.len());
    let cmp = a[..n].cmp(&b[..n]);
    if prefix {
        if cmp == Ordering::Equal && a.len() > b.len() {
            Ordering::Greater
        } else {
            cmp
        }
    } else if cmp == Ordering::Equal {
        a.len().cmp(&b.len())
    } else {
        cmp
    }
}

// ---------------------------------------------------------------- tsvector

/// A lexeme position and weight (`A` is 3, `D` is 0, as PostgreSQL stores it).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Pos {
    pub value: u16,
    pub weight: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Entry {
    pub lexeme: String,
    pub positions: Vec<Pos>,
}

/// A parsed lexeme vector, in PostgreSQL's stored order (by lexeme).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TsVector {
    pub entries: Vec<Entry>,
}

impl TsVector {
    /// The stored size PostgreSQL's comparisons order by
    /// (`CALCDATASIZE` plus the position vectors).
    pub(crate) fn stored_size(&self) -> usize {
        let mut size = 8 + 4 * self.entries.len();
        for entry in &self.entries {
            size += entry.lexeme.len();
            if !entry.positions.is_empty() {
                size = (size + 1) & !1; // SHORTALIGN
                size += 2 + 2 * entry.positions.len();
            }
        }
        size
    }

    pub(crate) fn find_index(&self, lexeme: &str) -> Option<usize> {
        self.entries
            .binary_search_by(|e| ts_compare_string(e.lexeme.as_bytes(), lexeme.as_bytes(), false))
            .ok()
    }

    pub(crate) fn find(&self, lexeme: &str) -> Option<&Entry> {
        self.find_index(lexeme).map(|i| &self.entries[i])
    }

    /// The range of entry indexes a lexeme prefix covers.
    pub(crate) fn prefix_indexes(&self, prefix: &str) -> std::ops::Range<usize> {
        let start = match self
            .entries
            .binary_search_by(|e| ts_compare_string(e.lexeme.as_bytes(), prefix.as_bytes(), false))
        {
            Ok(i) | Err(i) => i,
        };
        let end = self.entries[start..]
            .iter()
            .position(|e| {
                ts_compare_string(prefix.as_bytes(), e.lexeme.as_bytes(), true) != Ordering::Equal
            })
            .map_or(self.entries.len(), |n| start + n);
        start..end
    }

    /// The entries a lexeme prefix matches, in stored order. As PostgreSQL
    /// does, the exact term is found by binary search, and the entries that
    /// have it as a prefix immediately follow it.
    pub(crate) fn find_prefix(&self, prefix: &str) -> &[Entry] {
        let range = self.prefix_indexes(prefix);
        &self.entries[range]
    }
}

/// Parses a `tsvector` literal into its entries, merging duplicates and
/// sorting positions as PostgreSQL does.
pub(crate) fn parse_tsvector(text: &str) -> Result<TsVector, String> {
    let bad = |message: &str| db(format!("{message}: \"{text}\""), "42601");
    let chars: Vec<char> = text.chars().collect();
    let mut entries: Vec<(String, Vec<Pos>)> = Vec::new();
    let mut i = 0usize;
    while i < chars.len() {
        // Skip whitespace between tokens.
        if chars[i].is_whitespace() {
            i += 1;
            continue;
        }
        let mut lexeme = String::new();
        let quoted = chars[i] == '\'';
        if quoted {
            i += 1;
            loop {
                match chars.get(i) {
                    None => return Err(bad("syntax error in tsvector")),
                    Some('\'') => {
                        if chars.get(i + 1) == Some(&'\'') {
                            lexeme.push('\'');
                            i += 2;
                        } else {
                            i += 1;
                            break;
                        }
                    }
                    Some('\\') => match chars.get(i + 1) {
                        Some(&c) => {
                            lexeme.push(c);
                            i += 2;
                        }
                        None => {
                            return Err(db(
                                format!("there is no escaped character: \"{text}\""),
                                "42601",
                            ));
                        }
                    },
                    Some(&c) => {
                        lexeme.push(c);
                        i += 1;
                    }
                }
            }
        } else {
            loop {
                match chars.get(i) {
                    Some('\\') => match chars.get(i + 1) {
                        Some(&c) => {
                            lexeme.push(c);
                            i += 2;
                        }
                        None => {
                            return Err(db(
                                format!("there is no escaped character: \"{text}\""),
                                "42601",
                            ));
                        }
                    },
                    Some(&c) if !c.is_whitespace() && c != ':' => {
                        lexeme.push(c);
                        i += 1;
                    }
                    _ => break,
                }
            }
        }
        if lexeme.is_empty() {
            return Err(bad("syntax error in tsvector"));
        }
        if lexeme.len() > MAX_STRLEN {
            return Err(db(
                format!("word is too long in tsvector: \"{text}\""),
                "54000",
            ));
        }
        // Positions and weights: `:1,2A`, `:3B`, and so on. A weight applies
        // to the position it follows.
        let mut positions: Vec<Pos> = Vec::new();
        if chars.get(i) == Some(&':') {
            i += 1;
            loop {
                match chars.get(i) {
                    Some(c) if c.is_ascii_digit() => {
                        let start = i;
                        while matches!(chars.get(i), Some(c) if c.is_ascii_digit()) {
                            i += 1;
                        }
                        let digits: String = chars[start..i].iter().collect();
                        let value: i64 = digits
                            .parse()
                            .map_err(|_| bad("syntax error in tsvector"))?;
                        let value = limit_pos(value);
                        if value == 0 {
                            return Err(db(
                                format!("wrong position info in tsvector: \"{text}\""),
                                "42601",
                            ));
                        }
                        positions.push(Pos {
                            value: value as u16,
                            weight: 0,
                        });
                    }
                    _ => return Err(bad("syntax error in tsvector")),
                }
                // A weight letter (or `*`, which means `A`) follows.
                loop {
                    match chars.get(i) {
                        Some('a' | 'A' | '*') => {
                            let last = positions.last_mut().expect("position follows digit");
                            if last.weight != 0 {
                                return Err(bad("syntax error in tsvector"));
                            }
                            last.weight = 3;
                            i += 1;
                        }
                        Some('b' | 'B') => {
                            let last = positions.last_mut().expect("position follows digit");
                            if last.weight != 0 {
                                return Err(bad("syntax error in tsvector"));
                            }
                            last.weight = 2;
                            i += 1;
                        }
                        Some('c' | 'C') => {
                            let last = positions.last_mut().expect("position follows digit");
                            if last.weight != 0 {
                                return Err(bad("syntax error in tsvector"));
                            }
                            last.weight = 1;
                            i += 1;
                        }
                        Some('d' | 'D') => {
                            let last = positions.last_mut().expect("position follows digit");
                            if last.weight != 0 {
                                return Err(bad("syntax error in tsvector"));
                            }
                            last.weight = 0;
                            i += 1;
                        }
                        // Stray digits between positions are skipped, as
                        // PostgreSQL's parser does.
                        Some(c) if c.is_ascii_digit() => {
                            i += 1;
                        }
                        _ => break,
                    }
                }
                match chars.get(i) {
                    Some(',') => {
                        i += 1;
                    }
                    Some(&c) if c.is_whitespace() => break,
                    None => break,
                    _ => return Err(bad("syntax error in tsvector")),
                }
            }
        }
        entries.push((lexeme, positions));
    }

    // Sort by lexeme and merge duplicates.
    entries.sort_by(|a, b| ts_compare_string(a.0.as_bytes(), b.0.as_bytes(), false));
    let mut merged: Vec<Entry> = Vec::new();
    for (lexeme, positions) in entries {
        match merged.last_mut() {
            Some(last) if last.lexeme == lexeme => {
                if !positions.is_empty() {
                    last.positions.extend(positions);
                }
            }
            _ => merged.push(Entry { lexeme, positions }),
        }
    }
    // Within a lexeme: positions ascending, one entry per position keeping
    // the heighest weight, at most MAX_NUM_POS of them.
    for entry in &mut merged {
        if entry.positions.len() > 1 {
            entry.positions.sort_by_key(|p| p.value);
            let mut out: Vec<Pos> = Vec::with_capacity(entry.positions.len());
            for pos in &entry.positions {
                match out.last_mut() {
                    Some(last) if last.value == pos.value => {
                        if pos.weight > last.weight {
                            last.weight = pos.weight;
                        }
                    }
                    _ => {
                        if out.len() >= MAX_NUM_POS - 1 || pos.value as i64 == MAX_ENTRY_POS - 1 {
                            break;
                        }
                        out.push(*pos);
                    }
                }
            }
            entry.positions = out;
        }
    }
    let vector = TsVector { entries: merged };
    let storage = vector.entries.iter().map(|e| e.lexeme.len()).sum::<usize>();
    if storage > MAX_STRPOS {
        return Err(db(
            format!("string is too long for tsvector ({storage} bytes, max {MAX_STRPOS} bytes)"),
            "54000",
        ));
    }
    Ok(vector)
}

/// The canonical text of a parsed vector, as PostgreSQL prints it.
pub(crate) fn print_tsvector(vector: &TsVector) -> String {
    let mut out = String::new();
    for (i, entry) in vector.entries.iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        out.push('\'');
        for c in entry.lexeme.chars() {
            if c == '\'' || c == '\\' {
                out.push(c);
            }
            out.push(c);
        }
        out.push('\'');
        if !entry.positions.is_empty() {
            out.push(':');
            for (j, pos) in entry.positions.iter().enumerate() {
                if j > 0 {
                    out.push(',');
                }
                out.push_str(&pos.value.to_string());
                out.push(match pos.weight {
                    3 => 'A',
                    2 => 'B',
                    1 => 'C',
                    _ => continue,
                });
            }
        }
    }
    out
}

/// Parses and canonicalizes a `tsvector` literal.
pub(crate) fn tsvector_canonical(text: &str) -> Result<String, String> {
    parse_tsvector(text).map(|vector| print_tsvector(&vector))
}

/// PostgreSQL's `silly_cmp_tsvector`: stored size, entry count, then each
/// entry's positions (later positions and heavier weights sort first).
pub(crate) fn tsvector_cmp(a: &TsVector, b: &TsVector) -> Ordering {
    match a.stored_size().cmp(&b.stored_size()) {
        Ordering::Equal => {}
        other => return other,
    }
    match a.entries.len().cmp(&b.entries.len()) {
        Ordering::Equal => {}
        other => return other,
    }
    for (ae, be) in a.entries.iter().zip(&b.entries) {
        if ae.positions.is_empty() != be.positions.is_empty() {
            // An entry without positions sorts first.
            return if ae.positions.is_empty() {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        match ts_compare_string(ae.lexeme.as_bytes(), be.lexeme.as_bytes(), false) {
            Ordering::Equal => {}
            other => return other,
        }
        if !ae.positions.is_empty() {
            match ae.positions.len().cmp(&be.positions.len()) {
                Ordering::Equal => {}
                other => return other.reverse(),
            }
            for (ap, bp) in ae.positions.iter().zip(&be.positions) {
                match ap.value.cmp(&bp.value) {
                    Ordering::Equal => {}
                    other => return other.reverse(),
                }
                match ap.weight.cmp(&bp.weight) {
                    Ordering::Equal => {}
                    other => return other.reverse(),
                }
            }
        }
    }
    Ordering::Equal
}

/// `tsvector || tsvector`: the lexemes merge, and the right side's positions
/// shift past the left side's largest.
pub(crate) fn tsvector_concat(a: &TsVector, b: &TsVector) -> TsVector {
    let max_left = a
        .entries
        .iter()
        .flat_map(|e| e.positions.iter())
        .map(|p| p.value as i64)
        .max()
        .unwrap_or(0);
    let mut entries = a.entries.clone();
    for entry in &b.entries {
        let mut shifted = entry.clone();
        if !entry.positions.is_empty() {
            for pos in &mut shifted.positions {
                pos.value = limit_pos(pos.value as i64 + max_left) as u16;
            }
        }
        entries.push(shifted);
    }
    // Merge equal lexemes, as PostgreSQL's tsvector_concat does via its own
    // sorted merge.
    entries.sort_by(|x, y| ts_compare_string(x.lexeme.as_bytes(), y.lexeme.as_bytes(), false));
    let mut merged: Vec<Entry> = Vec::new();
    for entry in entries {
        match merged.last_mut() {
            Some(last) if last.lexeme == entry.lexeme => {
                last.positions.extend(entry.positions);
                last.positions.sort_by_key(|p| p.value);
                last.positions.dedup_by(|later, earlier| {
                    if later.value == earlier.value {
                        // Keep the heavier weight of the two.
                        earlier.weight = earlier.weight.max(later.weight);
                        true
                    } else {
                        false
                    }
                });
            }
            _ => merged.push(entry),
        }
    }
    TsVector { entries: merged }
}

// ---------------------------------------------------------------- tsquery

/// A query node: a lexeme, or an operator over one or two operands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum QNode {
    /// The empty query (`''::tsquery`), which matches nothing.
    Empty,
    Val {
        word: String,
        /// A weight bitmask (bit 3 `A` through bit 0 `D`; zero means any).
        weight: u8,
        prefix: bool,
    },
    Not(Box<QNode>),
    And(Box<QNode>, Box<QNode>),
    Or(Box<QNode>, Box<QNode>),
    Phrase {
        distance: i16,
        left: Box<QNode>,
        right: Box<QNode>,
    },
}

impl QNode {
    /// The number of stored items (PostgreSQL's `size`).
    fn items(&self) -> usize {
        match self {
            QNode::Empty => 0,
            QNode::Val { .. } => 1,
            QNode::Not(inner) => 1 + inner.items(),
            QNode::And(l, r) | QNode::Or(l, r) => 1 + l.items() + r.items(),
            QNode::Phrase { left, right, .. } => 1 + left.items() + right.items(),
        }
    }

    /// The operand text stored after the items (`sumlen`).
    fn operand_bytes(&self) -> usize {
        match self {
            QNode::Empty => 0,
            QNode::Val { word, .. } => word.len() + 1,
            QNode::Not(inner) => inner.operand_bytes(),
            QNode::And(l, r) | QNode::Or(l, r) => l.operand_bytes() + r.operand_bytes(),
            QNode::Phrase { left, right, .. } => left.operand_bytes() + right.operand_bytes(),
        }
    }

    /// PostgreSQL's stored size for the whole query.
    pub(crate) fn stored_size(&self) -> usize {
        8 + 12 * self.items() + self.operand_bytes()
    }
}

/// PostgreSQL's legacy CRC-32 (Sarwate over the normal table), which query
/// comparisons order operands by.
fn legacy_crc32(data: &[u8]) -> u32 {
    static TABLE: [u32; 256] = [
        0x00000000, 0x77073096, 0xEE0E612C, 0x990951BA, 0x076DC419, 0x706AF48F, 0xE963A535,
        0x9E6495A3, 0x0EDB8832, 0x79DCB8A4, 0xE0D5E91E, 0x97D2D988, 0x09B64C2B, 0x7EB17CBD,
        0xE7B82D07, 0x90BF1D91, 0x1DB71064, 0x6AB020F2, 0xF3B97148, 0x84BE41DE, 0x1ADAD47D,
        0x6DDDE4EB, 0xF4D4B551, 0x83D385C7, 0x136C9856, 0x646BA8C0, 0xFD62F97A, 0x8A65C9EC,
        0x14015C4F, 0x63066CD9, 0xFA0F3D63, 0x8D080DF5, 0x3B6E20C8, 0x4C69105E, 0xD56041E4,
        0xA2677172, 0x3C03E4D1, 0x4B04D447, 0xD20D85FD, 0xA50AB56B, 0x35B5A8FA, 0x42B2986C,
        0xDBBBC9D6, 0xACBCF940, 0x32D86CE3, 0x45DF5C75, 0xDCD60DCF, 0xABD13D59, 0x26D930AC,
        0x51DE003A, 0xC8D75180, 0xBFD06116, 0x21B4F4B5, 0x56B3C423, 0xCFBA9599, 0xB8BDA50F,
        0x2802B89E, 0x5F058808, 0xC60CD9B2, 0xB10BE924, 0x2F6F7C87, 0x58684C11, 0xC1611DAB,
        0xB6662D3D, 0x76DC4190, 0x01DB7106, 0x98D220BC, 0xEFD5102A, 0x71B18589, 0x06B6B51F,
        0x9FBFE4A5, 0xE8B8D433, 0x7807C9A2, 0x0F00F934, 0x9609A88E, 0xE10E9818, 0x7F6A0DBB,
        0x086D3D2D, 0x91646C97, 0xE6635C01, 0x6B6B51F4, 0x1C6C6162, 0x856530D8, 0xF262004E,
        0x6C0695ED, 0x1B01A57B, 0x8208F4C1, 0xF50FC457, 0x65B0D9C6, 0x12B7E950, 0x8BBEB8EA,
        0xFCB9887C, 0x62DD1DDF, 0x15DA2D49, 0x8CD37CF3, 0xFBD44C65, 0x4DB26158, 0x3AB551CE,
        0xA3BC0074, 0xD4BB30E2, 0x4ADFA541, 0x3DD895D7, 0xA4D1C46D, 0xD3D6F4FB, 0x4369E96A,
        0x346ED9FC, 0xAD678846, 0xDA60B8D0, 0x44042D73, 0x33031DE5, 0xAA0A4C5F, 0xDD0D7CC9,
        0x5005713C, 0x270241AA, 0xBE0B1010, 0xC90C2086, 0x5768B525, 0x206F85B3, 0xB966D409,
        0xCE61E49F, 0x5EDEF90E, 0x29D9C998, 0xB0D09822, 0xC7D7A8B4, 0x59B33D17, 0x2EB40D81,
        0xB7BD5C3B, 0xC0BA6CAD, 0xEDB88320, 0x9ABFB3B6, 0x03B6E20C, 0x74B1D29A, 0xEAD54739,
        0x9DD277AF, 0x04DB2615, 0x73DC1683, 0xE3630B12, 0x94643B84, 0x0D6D6A3E, 0x7A6A5AA8,
        0xE40ECF0B, 0x9309FF9D, 0x0A00AE27, 0x7D079EB1, 0xF00F9344, 0x8708A3D2, 0x1E01F268,
        0x6906C2FE, 0xF762575D, 0x806567CB, 0x196C3671, 0x6E6B06E7, 0xFED41B76, 0x89D32BE0,
        0x10DA7A5A, 0x67DD4ACC, 0xF9B9DF6F, 0x8EBEEFF9, 0x17B7BE43, 0x60B08ED5, 0xD6D6A3E8,
        0xA1D1937E, 0x38D8C2C4, 0x4FDFF252, 0xD1BB67F1, 0xA6BC5767, 0x3FB506DD, 0x48B2364B,
        0xD80D2BDA, 0xAF0A1B4C, 0x36034AF6, 0x41047A60, 0xDF60EFC3, 0xA867DF55, 0x316E8EEF,
        0x4669BE79, 0xCB61B38C, 0xBC66831A, 0x256FD2A0, 0x5268E236, 0xCC0C7795, 0xBB0B4703,
        0x220216B9, 0x5505262F, 0xC5BA3BBE, 0xB2BD0B28, 0x2BB45A92, 0x5CB36A04, 0xC2D7FFA7,
        0xB5D0CF31, 0x2CD99E8B, 0x5BDEAE1D, 0x9B64C2B0, 0xEC63F226, 0x756AA39C, 0x026D930A,
        0x9C0906A9, 0xEB0E363F, 0x72076785, 0x05005713, 0x95BF4A82, 0xE2B87A14, 0x7BB12BAE,
        0x0CB61B38, 0x92D28E9B, 0xE5D5BE0D, 0x7CDCEFB7, 0x0BDBDF21, 0x86D3D2D4, 0xF1D4E242,
        0x68DDB3F8, 0x1FDA836E, 0x81BE16CD, 0xF6B9265B, 0x6FB077E1, 0x18B74777, 0x88085AE6,
        0xFF0F6A70, 0x66063BCA, 0x11010B5C, 0x8F659EFF, 0xF862AE69, 0x616BFFD3, 0x166CCF45,
        0xA00AE278, 0xD70DD2EE, 0x4E048354, 0x3903B3C2, 0xA7672661, 0xD06016F7, 0x4969474D,
        0x3E6E77DB, 0xAED16A4A, 0xD9D65ADC, 0x40DF0B66, 0x37D83BF0, 0xA9BCAE53, 0xDEBB9EC5,
        0x47B2CF7F, 0x30B5FFE9, 0xBDBDF21C, 0xCABAC28A, 0x53B39330, 0x24B4A3A6, 0xBAD03605,
        0xCDD70693, 0x54DE5729, 0x23D967BF, 0xB3667A2E, 0xC4614AB8, 0x5D681B02, 0x2A6F2B94,
        0xB40BBE37, 0xC30C8EA1, 0x5A05DF1B, 0x2D02EF8D,
    ];
    let mut crc: u32 = 0xFFFF_FFFF;
    for byte in data {
        crc = TABLE[((crc >> 24) as u8 ^ byte) as usize] ^ (crc << 8);
    }
    crc ^ 0xFFFF_FFFF
}

/// PostgreSQL's `QTNodeCompare`: operator kind and count, then operands by
/// CRC and text; larger operators, more children, longer distances, and
/// larger CRCs sort first.
fn qnode_cmp(a: &QNode, b: &QNode) -> Ordering {
    fn op_rank(node: &QNode) -> u8 {
        match node {
            QNode::Empty | QNode::Val { .. } => 0,
            QNode::Not(_) => 1,
            QNode::And(..) => 2,
            QNode::Or(..) => 3,
            QNode::Phrase { .. } => 4,
        }
    }
    fn children(node: &QNode) -> Vec<&QNode> {
        // PostgreSQL stores the query in reverse polish, so a binary node's
        // first child is its *right* operand; the comparison sees them so.
        match node {
            QNode::Empty | QNode::Val { .. } => Vec::new(),
            QNode::Not(inner) => vec![inner],
            QNode::And(l, r) | QNode::Or(l, r) => vec![r, l],
            QNode::Phrase { left, right, .. } => vec![right, left],
        }
    }
    // An operator sorts before a value (PostgreSQL's `QI_OPR` outranks
    // `QI_VAL`, and a larger type sorts first).
    let (a_op, b_op) = (op_rank(a) > 0, op_rank(b) > 0);
    if a_op != b_op {
        return if a_op {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }
    if !a_op {
        let (QNode::Val { word: aw, .. }, QNode::Val { word: bw, .. }) = (a, b) else {
            unreachable!("both values");
        };
        // PostgreSQL stores the CRC as a signed int32 and compares it so.
        let (acrc, bcrc) = (
            legacy_crc32(aw.as_bytes()) as i32,
            legacy_crc32(bw.as_bytes()) as i32,
        );
        match acrc.cmp(&bcrc) {
            Ordering::Equal => {}
            other => return other.reverse(),
        }
        return ts_compare_string(aw.as_bytes(), bw.as_bytes(), false);
    }
    match op_rank(a).cmp(&op_rank(b)) {
        Ordering::Equal => {}
        other => return other.reverse(),
    }
    let (ac, bc) = (children(a), children(b));
    match ac.len().cmp(&bc.len()) {
        Ordering::Equal => {}
        other => return other.reverse(),
    }
    for (x, y) in ac.iter().zip(&bc) {
        match qnode_cmp(x, y) {
            Ordering::Equal => {}
            other => return other,
        }
    }
    if let (QNode::Phrase { distance: ad, .. }, QNode::Phrase { distance: bd, .. }) = (a, b) {
        match ad.cmp(bd) {
            Ordering::Equal => {}
            other => return other.reverse(),
        }
    }
    Ordering::Equal
}

/// PostgreSQL's `CompareTSQ`: item count, stored size, then the trees.
pub(crate) fn tsquery_cmp(a: &QNode, b: &QNode) -> Ordering {
    match a.items().cmp(&b.items()) {
        Ordering::Equal => {}
        other => return other,
    }
    match a.stored_size().cmp(&b.stored_size()) {
        Ordering::Equal => {}
        other => return other,
    }
    if a.items() == 0 {
        return Ordering::Equal;
    }
    qnode_cmp(a, b)
}

impl QNode {
    /// The infix text PostgreSQL prints, with its parenthesization.
    fn print(&self, out: &mut String, parent_priority: i16, right_phrase: bool) {
        match self {
            QNode::Empty => {}
            QNode::Val {
                word,
                weight,
                prefix,
            } => {
                out.push('\'');
                for c in word.chars() {
                    if c == '\'' || c == '\\' {
                        out.push(c);
                    }
                    out.push(c);
                }
                out.push('\'');
                if *weight != 0 || *prefix {
                    out.push(':');
                    if *prefix {
                        out.push('*');
                    }
                    if weight & (1 << 3) != 0 {
                        out.push('A');
                    }
                    if weight & (1 << 2) != 0 {
                        out.push('B');
                    }
                    if weight & (1 << 1) != 0 {
                        out.push('C');
                    }
                    if weight & 1 != 0 {
                        out.push('D');
                    }
                }
            }
            QNode::Not(inner) => {
                let priority = priority_of(QNode::NOT);
                let parens = priority < parent_priority;
                if parens {
                    out.push_str("( ");
                }
                out.push('!');
                inner.print(out, priority, false);
                if parens {
                    out.push_str(" )");
                }
            }
            QNode::And(l, r) => {
                self.print_binary(l, r, QNode::AND, " & ", out, parent_priority, right_phrase)
            }
            QNode::Or(l, r) => {
                self.print_binary(l, r, QNode::OR, " | ", out, parent_priority, right_phrase)
            }
            QNode::Phrase {
                distance: 1,
                left,
                right,
            } => self.print_binary(
                left,
                right,
                QNode::PHRASE,
                " <-> ",
                out,
                parent_priority,
                right_phrase,
            ),
            QNode::Phrase {
                distance,
                left,
                right,
            } => self.print_binary(
                left,
                right,
                QNode::PHRASE,
                &format!(" <{distance}> "),
                out,
                parent_priority,
                right_phrase,
            ),
        }
    }

    fn print_binary(
        &self,
        l: &QNode,
        r: &QNode,
        op: i16,
        symbol: &str,
        out: &mut String,
        parent_priority: i16,
        right_phrase: bool,
    ) {
        let priority = priority_of(op);
        let parens = priority < parent_priority || (op == QNode::PHRASE && right_phrase);
        if parens {
            out.push_str("( ");
        }
        l.print(out, priority, false);
        out.push_str(symbol);
        r.print(out, priority, op == QNode::PHRASE);
        if parens {
            out.push_str(" )");
        }
    }
}

// The operator codes PostgreSQL's priorities index (`OP_NOT` .. `OP_PHRASE`).
impl QNode {
    const NOT: i16 = 1;
    const AND: i16 = 2;
    const OR: i16 = 3;
    const PHRASE: i16 = 4;
}

/// The operator priorities PostgreSQL prints with: NOT 4, PHRASE 3, AND 2,
/// OR 1.
fn priority_of(op: i16) -> i16 {
    match op {
        QNode::NOT => 4,
        QNode::AND => 2,
        QNode::OR => 1,
        _ => 3,
    }
}

/// The canonical text of a query tree.
pub(crate) fn print_tsquery(node: &QNode) -> String {
    let mut out = String::new();
    node.print(&mut out, -1, false);
    out
}

/// Parses a `tsquery` literal: operands, `!`, `&`, `|`, `<->`, `<N>`, and
/// parentheses. Operators bind as PostgreSQL's priorities do — OR loosest,
/// then AND, then phrases, then `!` — and AND/OR/phrase chains are
/// left-associative.
pub(crate) fn parse_tsquery(text: &str) -> Result<QNode, String> {
    if text.trim().is_empty() {
        // An input with no lexemes is the empty query.
        return Ok(QNode::Empty);
    }
    let chars: Vec<char> = text.chars().collect();
    let mut parser = QueryParser {
        chars: &chars,
        pos: 0,
        text,
    };
    let node = parser.parse_or()?;
    if parser.pos != parser.chars.len() {
        return Err(syntax_error(text));
    }
    Ok(node)
}

struct QueryParser<'a> {
    chars: &'a [char],
    pos: usize,
    text: &'a str,
}

impl QueryParser<'_> {
    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn skip_space(&mut self) {
        while matches!(self.peek(), Some(c) if c.is_whitespace()) {
            self.pos += 1;
        }
    }

    /// `or` := `and` (`|` `and`)*
    fn parse_or(&mut self) -> Result<QNode, String> {
        let mut left = self.parse_and()?;
        loop {
            self.skip_space();
            if self.peek() != Some('|') {
                return Ok(left);
            }
            self.pos += 1;
            let right = self.parse_and()?;
            left = QNode::Or(Box::new(left), Box::new(right));
        }
    }

    /// `and` := `phrase` (`&` `phrase`)*
    fn parse_and(&mut self) -> Result<QNode, String> {
        let mut left = self.parse_phrase()?;
        loop {
            self.skip_space();
            if self.peek() != Some('&') {
                return Ok(left);
            }
            self.pos += 1;
            let right = self.parse_phrase()?;
            left = QNode::And(Box::new(left), Box::new(right));
        }
    }

    /// `phrase` := `unary` ((`<->` | `<N>`) `unary`)*
    fn parse_phrase(&mut self) -> Result<QNode, String> {
        let mut left = self.parse_unary()?;
        loop {
            self.skip_space();
            if self.peek() != Some('<') {
                return Ok(left);
            }
            let (distance, next) = parse_phrase_op(self.chars, self.pos, self.text)?;
            self.pos = next;
            let right = self.parse_unary()?;
            left = QNode::Phrase {
                distance,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
    }

    /// `unary` := `!` `unary` | `(` `or` `)` | operand
    fn parse_unary(&mut self) -> Result<QNode, String> {
        self.skip_space();
        match self.peek() {
            None => Err(no_operand_error(self.text)),
            Some('!') => {
                self.pos += 1;
                let inner = self.parse_unary()?;
                Ok(QNode::Not(Box::new(inner)))
            }
            Some('(') => {
                self.pos += 1;
                self.skip_space();
                if self.peek() == Some(')') {
                    return Err(syntax_error(self.text));
                }
                let inner = self.parse_or()?;
                self.skip_space();
                if self.peek() != Some(')') {
                    return Err(no_operand_error(self.text));
                }
                self.pos += 1;
                Ok(inner)
            }
            Some(c) if !matches!(c, ')' | '&' | '|' | '<') => {
                let (word, next) = parse_query_operand(self.chars, self.pos, self.text)?;
                self.pos = next;
                let mut weight = 0u8;
                let mut prefix = false;
                if self.peek() == Some(':') {
                    self.pos += 1;
                    loop {
                        match self.peek() {
                            Some('a' | 'A') => weight |= 1 << 3,
                            Some('b' | 'B') => weight |= 1 << 2,
                            Some('c' | 'C') => weight |= 1 << 1,
                            Some('d' | 'D') => weight |= 1,
                            Some('*') => prefix = true,
                            _ => break,
                        }
                        self.pos += 1;
                    }
                }
                if word.is_empty() {
                    return Err(syntax_error(self.text));
                }
                Ok(QNode::Val {
                    word,
                    weight,
                    prefix,
                })
            }
            Some(_) => Err(syntax_error(self.text)),
        }
    }
}

fn syntax_error(text: &str) -> String {
    db(format!("syntax error in tsquery: \"{text}\""), "42601")
}

fn no_operand_error(text: &str) -> String {
    db(format!("no operand in tsquery: \"{text}\""), "42601")
}

/// One query operand: a quoted lexeme, or a run of characters up to a
/// delimiter (whitespace, `!`, `|`, `&`, `(`, `)`, or `:`).
fn parse_query_operand(
    chars: &[char],
    start: usize,
    text: &str,
) -> Result<(String, usize), String> {
    let mut i = start;
    let mut word = String::new();
    if chars[i] == '\'' {
        i += 1;
        loop {
            match chars.get(i) {
                None => return Err(db(format!("syntax error in tsquery: \"{text}\""), "42601")),
                Some('\'') => {
                    if chars.get(i + 1) == Some(&'\'') {
                        word.push('\'');
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                }
                Some('\\') => match chars.get(i + 1) {
                    Some(&c) => {
                        word.push(c);
                        i += 2;
                    }
                    None => {
                        return Err(db(
                            format!("there is no escaped character: \"{text}\""),
                            "42601",
                        ));
                    }
                },
                Some(&c) => {
                    word.push(c);
                    i += 1;
                }
            }
        }
    } else {
        loop {
            match chars.get(i) {
                Some('\\') => match chars.get(i + 1) {
                    Some(&c) => {
                        word.push(c);
                        i += 2;
                    }
                    None => {
                        return Err(db(
                            format!("there is no escaped character: \"{text}\""),
                            "42601",
                        ));
                    }
                },
                Some(&c)
                    if !c.is_whitespace() && !matches!(c, '!' | '|' | '&' | '(' | ')' | ':') =>
                {
                    word.push(c);
                    i += 1;
                }
                _ => break,
            }
        }
    }
    Ok((word, i))
}

/// A phrase operator: `<->` (distance 1) or `<N>`.
fn parse_phrase_op(chars: &[char], start: usize, text: &str) -> Result<(i16, usize), String> {
    let bad = || db(format!("syntax error in tsquery: \"{text}\""), "42601");
    let mut i = start;
    if chars.get(i) != Some(&'<') {
        return Err(bad());
    }
    i += 1;
    match chars.get(i) {
        Some('-') => {
            i += 1;
            if chars.get(i) != Some(&'>') {
                return Err(bad());
            }
            Ok((1, i + 1))
        }
        Some(c) if c.is_ascii_digit() => {
            let digits_start = i;
            while matches!(chars.get(i), Some(c) if c.is_ascii_digit()) {
                i += 1;
            }
            let digits: String = chars[digits_start..i].iter().collect();
            let distance: i64 = digits.parse().map_err(|_| bad())?;
            if chars.get(i) != Some(&'>') {
                return Err(bad());
            }
            if distance > MAX_ENTRY_POS {
                return Err(db(
                    format!(
                        "distance in phrase operator must be an integer value between zero and {} inclusive",
                        MAX_ENTRY_POS
                    ),
                    "22023",
                ));
            }
            Ok((distance as i16, i + 1))
        }
        _ => Err(bad()),
    }
}

/// Parses and canonicalizes a `tsquery` literal.
pub(crate) fn tsquery_canonical(text: &str) -> Result<String, String> {
    let query = parse_tsquery(text)?;
    if matches!(query, QNode::Empty) {
        // As PostgreSQL's tsquery input does.
        crate::session_env::notice(crate::error_fields::DbError::new(format!(
            "text-search query doesn't contain lexemes: \"{text}\""
        )));
    }
    Ok(print_tsquery(&query))
}

// ---------------------------------------------------------------- matching

/// Where a value node's matches come from: a tsvector, or the positions a
/// cover scan has collected so far (`ts_rank_cd`).
pub(crate) trait MatchSource {
    /// The positions this node matches, or PostgreSQL's "maybe" when the
    /// source cannot tell.
    fn positions(&self, node: &QNode, want_positions: bool) -> MaybePositions;
}

/// A match source over a tsvector.
pub(crate) struct VectorMatches<'a>(pub(crate) &'a TsVector);

impl MatchSource for VectorMatches<'_> {
    fn positions(&self, node: &QNode, want_positions: bool) -> MaybePositions {
        let QNode::Val {
            word,
            weight,
            prefix,
        } = node
        else {
            return MaybePositions::No;
        };
        matched_positions(self.0, word, *prefix, *weight, want_positions)
    }
}

/// The positions a lexeme query node matches, or `None` when the vector has
/// no positions for it (PostgreSQL's "maybe").
fn matched_positions(
    vector: &TsVector,
    word: &str,
    prefix: bool,
    weight: u8,
    want_positions: bool,
) -> MaybePositions {
    // The entries the word matches: one, or every entry a prefix of it
    // covers. A definite match on any of them settles the result.
    let found: Vec<&Entry> = if prefix {
        vector.find_prefix(word).iter().collect()
    } else {
        vector.find(word).into_iter().collect()
    };
    let mut positions: Vec<i32> = Vec::new();
    let mut maybe = false;
    for entry in found {
        if entry.positions.is_empty() {
            // A stripped entry matches regardless of weight.
            if !want_positions {
                return MaybePositions::Yes(Vec::new());
            }
            maybe = true;
            continue;
        }
        let matching: Vec<i32> = entry
            .positions
            .iter()
            .filter(|p| weight == 0 || weight & (1 << p.weight) != 0)
            .map(|p| p.value as i32)
            .collect();
        if matching.is_empty() {
            continue;
        }
        if !want_positions {
            return MaybePositions::Yes(Vec::new());
        }
        positions.extend(matching);
    }
    if !positions.is_empty() {
        MaybePositions::Yes(positions)
    } else if maybe {
        MaybePositions::Maybe
    } else {
        MaybePositions::No
    }
}

pub(crate) enum MaybePositions {
    No,
    Maybe,
    Yes(Vec<i32>),
}

/// Match data for phrase evaluation, mirroring PostgreSQL's `ExecPhraseData`.
#[derive(Default)]
pub(crate) struct PhraseData {
    pub(crate) pos: Vec<i32>,
    pub(crate) width: i32,
    pub(crate) negate: bool,
}

pub(crate) const TSPO_BOTH: u8 = 1;
pub(crate) const TSPO_L_ONLY: u8 = 2;
pub(crate) const TSPO_R_ONLY: u8 = 4;
/// Every position of either side (`TSPO_BOTH | TSPO_L_ONLY | TSPO_R_ONLY`).
pub(crate) const TSPO_ALL: u8 = TSPO_BOTH | TSPO_L_ONLY | TSPO_R_ONLY;

/// The merge-join PostgreSQL's `TS_phrase_output` performs.
pub(crate) fn phrase_output(
    out: Option<&mut PhraseData>,
    left: &PhraseData,
    right: &PhraseData,
    emit: u8,
    l_offset: i32,
    r_offset: i32,
) -> bool {
    let mut li = 0usize;
    let mut ri = 0usize;
    let mut positions: Vec<i32> = Vec::new();
    while li < left.pos.len() || ri < right.pos.len() {
        let l = left.pos.get(li).map(|p| p + l_offset);
        let r = right.pos.get(ri).map(|p| p + r_offset);
        let output = match (l, r) {
            (Some(l), Some(r)) if l < r => {
                li += 1;
                (emit & TSPO_L_ONLY != 0).then_some(l)
            }
            (Some(l), Some(r)) if l == r => {
                li += 1;
                ri += 1;
                (emit & TSPO_BOTH != 0).then_some(l)
            }
            (Some(_), Some(r)) => {
                ri += 1;
                (emit & TSPO_R_ONLY != 0).then_some(r)
            }
            (Some(l), None) => {
                if emit & TSPO_L_ONLY == 0 {
                    break;
                }
                li += 1;
                Some(l)
            }
            (None, Some(r)) => {
                if emit & TSPO_R_ONLY == 0 {
                    break;
                }
                ri += 1;
                Some(r)
            }
            (None, None) => break,
        };
        if let Some(position) = output {
            if out.is_none() {
                return true;
            }
            positions.push(position);
        }
    }
    if let Some(out) = out {
        out.pos = positions;
        return !out.pos.is_empty();
    }
    false
}

/// Evaluates a query tree with position tracking for phrases, as
/// PostgreSQL's `TS_phrase_execute` does.
pub(crate) fn phrase_execute(
    node: &QNode,
    source: &dyn MatchSource,
    mut out: Option<&mut PhraseData>,
) -> Ternary {
    match node {
        QNode::Empty => Ternary::No,
        QNode::Val { .. } => match source.positions(node, out.is_some()) {
            MaybePositions::No => Ternary::No,
            MaybePositions::Maybe => Ternary::Maybe,
            MaybePositions::Yes(positions) => {
                if let Some(data) = out {
                    data.pos = positions;
                }
                Ternary::Yes
            }
        },
        QNode::Not(inner) => match phrase_execute(inner, source, out.as_deref_mut()) {
            Ternary::No => {
                if let Some(data) = out {
                    data.pos.clear();
                    data.negate = true;
                }
                Ternary::Yes
            }
            Ternary::Yes => {
                let Some(data) = out else {
                    return Ternary::Yes;
                };
                if !data.pos.is_empty() {
                    data.negate = !data.negate;
                    Ternary::Yes
                } else if data.negate {
                    data.negate = false;
                    Ternary::No
                } else {
                    Ternary::No
                }
            }
            Ternary::Maybe => Ternary::Maybe,
        },
        QNode::And(l, r) | QNode::Or(l, r) => {
            let is_and = matches!(node, QNode::And(..));
            let mut ldata = PhraseData::default();
            let mut rdata = PhraseData::default();
            let lmatch = phrase_execute(l, source, Some(&mut ldata));
            let rmatch = phrase_execute(r, source, Some(&mut rdata));
            if is_and {
                if lmatch == Ternary::No || rmatch == Ternary::No {
                    return Ternary::No;
                }
                if lmatch == Ternary::Maybe || rmatch == Ternary::Maybe {
                    return Ternary::Maybe;
                }
                let maxwidth = ldata.width.max(rdata.width);
                let (l_offset, r_offset) = (maxwidth - ldata.width, maxwidth - rdata.width);
                let emit = match (ldata.negate, rdata.negate) {
                    (true, true) => TSPO_BOTH | TSPO_L_ONLY | TSPO_R_ONLY,
                    (true, false) => TSPO_R_ONLY,
                    (false, true) => TSPO_L_ONLY,
                    (false, false) => TSPO_BOTH,
                };
                let both_negate = ldata.negate && rdata.negate;
                let matched =
                    phrase_output(out.as_deref_mut(), &ldata, &rdata, emit, l_offset, r_offset);
                if let Some(data) = out {
                    data.width = maxwidth;
                    if both_negate {
                        data.negate = true;
                    }
                }
                if both_negate {
                    return Ternary::Yes;
                }
                if matched { Ternary::Yes } else { Ternary::No }
            } else {
                if lmatch == Ternary::No && rmatch == Ternary::No {
                    return Ternary::No;
                }
                if lmatch == Ternary::Maybe || rmatch == Ternary::Maybe {
                    return Ternary::Maybe;
                }
                if lmatch == Ternary::No {
                    ldata.width = 0;
                }
                if rmatch == Ternary::No {
                    rdata.width = 0;
                }
                let maxwidth = ldata.width.max(rdata.width);
                let (l_offset, r_offset) = (maxwidth - ldata.width, maxwidth - rdata.width);
                let emit = match (ldata.negate, rdata.negate) {
                    (true, true) => TSPO_BOTH,
                    (true, false) => TSPO_L_ONLY,
                    (false, true) => TSPO_R_ONLY,
                    (false, false) => TSPO_BOTH | TSPO_L_ONLY | TSPO_R_ONLY,
                };
                let negated = ldata.negate || rdata.negate;
                let matched =
                    phrase_output(out.as_deref_mut(), &ldata, &rdata, emit, l_offset, r_offset);
                if let Some(data) = out {
                    data.width = maxwidth;
                    if negated {
                        data.negate = true;
                    }
                }
                if negated || matched {
                    Ternary::Yes
                } else {
                    Ternary::No
                }
            }
        }
        QNode::Phrase {
            distance,
            left,
            right,
        } => {
            let mut ldata = PhraseData::default();
            let mut rdata = PhraseData::default();
            let lmatch = phrase_execute(left, source, Some(&mut ldata));
            if lmatch == Ternary::No {
                return Ternary::No;
            }
            let rmatch = phrase_execute(right, source, Some(&mut rdata));
            if rmatch == Ternary::No {
                return Ternary::No;
            }
            if lmatch == Ternary::Maybe || rmatch == Ternary::Maybe {
                return Ternary::Maybe;
            }
            let l_offset = *distance as i32 + rdata.width;
            let r_offset = 0;
            let width = *distance as i32 + ldata.width + rdata.width;
            let emit = match (ldata.negate, rdata.negate) {
                (true, true) => TSPO_BOTH | TSPO_L_ONLY | TSPO_R_ONLY,
                (true, false) => TSPO_R_ONLY,
                (false, true) => TSPO_L_ONLY,
                (false, false) => TSPO_BOTH,
            };
            let both_negate = ldata.negate && rdata.negate;
            let matched =
                phrase_output(out.as_deref_mut(), &ldata, &rdata, emit, l_offset, r_offset);
            if let Some(data) = out {
                data.width = width;
                if both_negate {
                    data.negate = true;
                }
            }
            if both_negate {
                return Ternary::Yes;
            }
            if matched { Ternary::Yes } else { Ternary::No }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Ternary {
    No,
    Yes,
    Maybe,
}

/// `tsvector @@ tsquery`: does the vector satisfy the query.
pub(crate) fn matches(vector: &TsVector, query: &QNode) -> bool {
    // Only a definite yes matches: PostgreSQL treats a top-level "maybe"
    // (position data missing) as no match.
    execute(query, &VectorMatches(vector), false) == Ternary::Yes
}

// ------------------------------------------------------------ ts_rewrite

/// A node of the working tree of `ts_rewrite` (PostgreSQL's `QTNode`): the
/// children are in storage order, the first of a binary node being the one
/// the printer shows last.
#[derive(Clone)]
enum Qtn {
    Val {
        word: String,
        weight: u8,
        prefix: bool,
        crc: i32,
        /// The bitset of lexeme CRCs the subtree holds (`sign`).
        sign: u32,
    },
    Op {
        oper: i16,
        distance: i16,
        children: Vec<Qtn>,
        sign: u32,
        /// `QTN_NOCHANGE`: this node is fresh and is not searched again.
        nochange: bool,
    },
}

fn qtn_sign_crc(crc: i32) -> u32 {
    1u32 << ((crc as u32) % 32)
}

impl Qtn {
    /// `QT2QTN`: the working tree of a parsed query.
    fn of(node: &QNode) -> Qtn {
        match node {
            QNode::Empty => unreachable!("the empty query has no tree"),
            QNode::Val {
                word,
                weight,
                prefix,
            } => {
                let crc = legacy_crc32(word.as_bytes()) as i32;
                Qtn::Val {
                    word: word.clone(),
                    weight: *weight,
                    prefix: *prefix,
                    crc,
                    sign: qtn_sign_crc(crc),
                }
            }
            QNode::Not(inner) => {
                let child = Qtn::of(inner);
                let sign = child.sign();
                Qtn::Op {
                    oper: QNode::NOT,
                    distance: 0,
                    children: vec![child],
                    sign,
                    nochange: false,
                }
            }
            QNode::And(l, r) => Qtn::binary(QNode::AND, 0, r, l),
            QNode::Or(l, r) => Qtn::binary(QNode::OR, 0, r, l),
            QNode::Phrase {
                distance,
                left,
                right,
            } => Qtn::binary(QNode::PHRASE, *distance, right, left),
        }
    }

    /// A binary node, its children in storage order (the right operand
    /// first, as `QT2QTN` reads the array).
    fn binary(oper: i16, distance: i16, first: &QNode, second: &QNode) -> Qtn {
        let children = vec![Qtn::of(first), Qtn::of(second)];
        let sign = children.iter().fold(0, |sign, child| sign | child.sign());
        Qtn::Op {
            oper,
            distance,
            children,
            sign,
            nochange: false,
        }
    }

    fn oper(&self) -> i16 {
        match self {
            Qtn::Val { .. } => 0,
            Qtn::Op { oper, .. } => *oper,
        }
    }

    fn sign(&self) -> u32 {
        match self {
            Qtn::Val { sign, .. } | Qtn::Op { sign, .. } => *sign,
        }
    }

    fn children(&self) -> &[Qtn] {
        match self {
            Qtn::Val { .. } => &[],
            Qtn::Op { children, .. } => children,
        }
    }
}

/// `QTNodeCompare` on the working tree: larger operators, more children,
/// larger CRCs, and longer distances sort first; lexemes compare by text
/// last. Operators sort before values.
fn qtn_cmp(a: &Qtn, b: &Qtn) -> Ordering {
    let (a_op, b_op) = (matches!(a, Qtn::Op { .. }), matches!(b, Qtn::Op { .. }));
    if a_op != b_op {
        return if a_op {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }
    match (a, b) {
        (
            Qtn::Val {
                crc: ac, word: aw, ..
            },
            Qtn::Val {
                crc: bc, word: bw, ..
            },
        ) => {
            match ac.cmp(bc) {
                Ordering::Equal => {}
                other => return other.reverse(),
            }
            ts_compare_string(aw.as_bytes(), bw.as_bytes(), false)
        }
        (
            Qtn::Op {
                oper: ao,
                distance: ad,
                children: ac,
                ..
            },
            Qtn::Op {
                oper: bo,
                distance: bd,
                children: bc,
                ..
            },
        ) => {
            match ao.cmp(bo) {
                Ordering::Equal => {}
                other => return other.reverse(),
            }
            match ac.len().cmp(&bc.len()) {
                Ordering::Equal => {}
                other => return other.reverse(),
            }
            for (x, y) in ac.iter().zip(bc) {
                match qtn_cmp(x, y) {
                    Ordering::Equal => {}
                    other => return other,
                }
            }
            if *ao == QNode::PHRASE {
                match ad.cmp(bd) {
                    Ordering::Equal => {}
                    other => return other.reverse(),
                }
            }
            Ordering::Equal
        }
        _ => unreachable!("the kinds were compared"),
    }
}

/// `QTNEq`: equal signatures and an equal comparison.
fn qtn_eq(a: &Qtn, b: &Qtn) -> bool {
    a.sign() == b.sign() && qtn_cmp(a, b) == Ordering::Equal
}

/// `QTNTernary`: flatten the same operator's nested children into one node.
fn qtn_ternary(node: &mut Qtn) {
    let Qtn::Op { oper, children, .. } = node else {
        return;
    };
    for child in children.iter_mut() {
        qtn_ternary(child);
    }
    if *oper != QNode::AND && *oper != QNode::OR {
        return;
    }
    let mut flat: Vec<Qtn> = Vec::with_capacity(children.len());
    for child in children.drain(..) {
        match child {
            Qtn::Op {
                oper: child_oper,
                children: grandchildren,
                sign,
                ..
            } if child_oper == *oper => {
                let _ = sign;
                flat.extend(grandchildren);
            }
            other => flat.push(other),
        }
    }
    *children = flat;
}

/// `QTNSort`: sort AND/OR children into their canonical order.
fn qtn_sort(node: &mut Qtn) {
    let Qtn::Op { oper, children, .. } = node else {
        return;
    };
    for child in children.iter_mut() {
        qtn_sort(child);
    }
    if children.len() > 1 && *oper != QNode::PHRASE {
        children.sort_by(qtn_cmp);
    }
}

/// `QTNBinary`: rebuild a binary tree by inserting intermediate nodes.
fn qtn_binary(mut node: Qtn) -> Qtn {
    if let Qtn::Op { children, .. } = &mut node {
        for child in children.iter_mut() {
            let taken = std::mem::replace(
                child,
                Qtn::Val {
                    word: String::new(),
                    weight: 0,
                    prefix: false,
                    crc: 0,
                    sign: 0,
                },
            );
            *child = qtn_binary(taken);
        }
        let Qtn::Op { oper, children, .. } = &mut node else {
            unreachable!("an operator node");
        };
        while children.len() > 2 {
            // Pair the first two children, then bring the last one in.
            let first = children.remove(0);
            let second = children.remove(0);
            let sign = first.sign() | second.sign();
            let nn = Qtn::Op {
                oper: *oper,
                distance: 0,
                children: vec![first, second],
                sign,
                nochange: false,
            };
            let last = children.pop().expect("more than two children");
            children.insert(0, nn);
            children.insert(1, last);
        }
    }
    node
}

/// The query an n-ary working tree serializes to (`QTN2QT`), as the binary
/// children in the printer's order.
fn qtn_to_qnode(node: &Qtn) -> QNode {
    match node {
        Qtn::Val {
            word,
            weight,
            prefix,
            ..
        } => QNode::Val {
            word: word.clone(),
            weight: *weight,
            prefix: *prefix,
        },
        Qtn::Op { oper, children, .. } => match (*oper, children.as_slice()) {
            (QNode::NOT, [inner]) => QNode::Not(Box::new(qtn_to_qnode(inner))),
            (QNode::AND, [first, second]) => {
                // `first` is the child the printer shows last.
                QNode::And(
                    Box::new(qtn_to_qnode(second)),
                    Box::new(qtn_to_qnode(first)),
                )
            }
            (QNode::OR, [first, second]) => QNode::Or(
                Box::new(qtn_to_qnode(second)),
                Box::new(qtn_to_qnode(first)),
            ),
            (QNode::PHRASE, [first, second]) => QNode::Phrase {
                distance: node_distance(node),
                left: Box::new(qtn_to_qnode(second)),
                right: Box::new(qtn_to_qnode(first)),
            },
            _ => unreachable!("a binary operator node"),
        },
    }
}

fn node_distance(node: &Qtn) -> i16 {
    match node {
        Qtn::Op { distance, .. } => *distance,
        _ => 0,
    }
}

/// `findeq`: replace a node equal to the target, or a subset of an AND/OR
/// node's children matching the target, with the substitute.
fn qtn_find_eq(node: Qtn, ex: &Qtn, subs: Option<&Qtn>, isfind: &mut bool) -> Option<Qtn> {
    // The signature must cover the target's, and the node kinds must match.
    if (node.sign() & ex.sign()) != ex.sign()
        || matches!(node, Qtn::Op { .. }) != matches!(ex, Qtn::Op { .. })
    {
        return Some(node);
    }
    if matches!(&node, Qtn::Op { nochange: true, .. }) {
        return Some(node);
    }
    match node {
        Qtn::Op {
            oper,
            distance,
            mut children,
            sign,
            nochange: _,
        } => {
            if oper != ex.oper() {
                return Some(Qtn::Op {
                    oper,
                    distance,
                    children,
                    sign,
                    nochange: false,
                });
            }
            if children.len() == ex.children().len() {
                let candidate = Qtn::Op {
                    oper,
                    distance,
                    children,
                    sign,
                    nochange: false,
                };
                if qtn_eq(&candidate, ex) {
                    *isfind = true;
                    return match subs {
                        Some(subs) => Some(mark_nochange(subs.clone())),
                        None => None,
                    };
                }
                // The candidate is consumed; rebuild it.
                let Qtn::Op { children, .. } = candidate else {
                    unreachable!("an operator node");
                };
                return Some(Qtn::Op {
                    oper,
                    distance,
                    children,
                    sign,
                    nochange: false,
                });
            }
            if children.len() > ex.children().len() && !ex.children().is_empty() {
                // AND and OR are commutative and associative: a subset of
                // this node's (sorted) children may match the target.
                let mut matched = vec![false; children.len()];
                let mut nmatched = 0usize;
                let (mut i, mut j) = (0usize, 0usize);
                while i < children.len() && j < ex.children().len() {
                    match qtn_cmp(&children[i], &ex.children()[j]) {
                        Ordering::Equal => {
                            matched[i] = true;
                            nmatched += 1;
                            i += 1;
                            j += 1;
                        }
                        Ordering::Less => i += 1,
                        Ordering::Greater => break,
                    }
                }
                if nmatched == ex.children().len() {
                    let mut kept = Vec::new();
                    for (i, child) in children.drain(..).enumerate() {
                        if !matched[i] {
                            kept.push(child);
                        }
                    }
                    if let Some(subs) = subs {
                        kept.push(mark_nochange(subs.clone()));
                    }
                    let mut node = Qtn::Op {
                        oper,
                        distance,
                        children: kept,
                        sign,
                        nochange: false,
                    };
                    // Re-sort to put the new child in its place.
                    qtn_sort(&mut node);
                    *isfind = true;
                    return Some(node);
                }
                return Some(Qtn::Op {
                    oper,
                    distance,
                    children,
                    sign,
                    nochange: false,
                });
            }
            Some(Qtn::Op {
                oper,
                distance,
                children,
                sign,
                nochange: false,
            })
        }
        Qtn::Val {
            word,
            weight,
            prefix,
            crc,
            sign,
        } => {
            if ex.oper() != 0 || crc != ex_crc(ex) {
                return Some(Qtn::Val {
                    word,
                    weight,
                    prefix,
                    crc,
                    sign,
                });
            }
            let candidate = Qtn::Val {
                word,
                weight,
                prefix,
                crc,
                sign,
            };
            if qtn_eq(&candidate, ex) {
                *isfind = true;
                return match subs {
                    Some(subs) => Some(mark_nochange(subs.clone())),
                    None => None,
                };
            }
            Some(candidate)
        }
    }
}

fn ex_crc(ex: &Qtn) -> i32 {
    match ex {
        Qtn::Val { crc, .. } => *crc,
        _ => 0,
    }
}

fn mark_nochange(node: Qtn) -> Qtn {
    match node {
        Qtn::Op {
            oper,
            distance,
            children,
            sign,
            ..
        } => Qtn::Op {
            oper,
            distance,
            children,
            sign,
            nochange: true,
        },
        val => val,
    }
}

/// `dofindsubquery`: match at the node, then in its children, dropping the
/// subtrees the substitute erased.
fn qtn_find_subquery(
    root: Option<Qtn>,
    ex: &Qtn,
    subs: Option<&Qtn>,
    isfind: &mut bool,
) -> Option<Qtn> {
    let root = match root {
        Some(root) => qtn_find_eq(root, ex, subs, isfind)?,
        None => return None,
    };
    let Qtn::Op {
        oper,
        distance,
        mut children,
        sign,
        nochange,
    } = root
    else {
        return Some(root);
    };
    if nochange {
        return Some(Qtn::Op {
            oper,
            distance,
            children,
            sign,
            nochange,
        });
    }
    let mut kept = Vec::new();
    for child in children.drain(..) {
        if let Some(child) = qtn_find_subquery(Some(child), ex, subs, isfind) {
            kept.push(child);
        }
    }
    // A node left with no children, or one non-NOT child, is simplified out.
    if kept.is_empty() {
        return None;
    }
    if kept.len() == 1 && oper != QNode::NOT {
        return kept.pop();
    }
    Some(Qtn::Op {
        oper,
        distance,
        children: kept,
        sign,
        nochange: false,
    })
}

/// `ts_rewrite(query, target, substitute)`: every occurrence of the target
/// replaced by the substitute, as PostgreSQL's flatten/sort/rewrite/
/// re-binarize pipeline.
pub(crate) fn query_rewrite(query: &QNode, target: &QNode, substitute: &QNode) -> QNode {
    if query.items() == 0 || target.items() == 0 {
        return query.clone();
    }
    let mut tree = Qtn::of(query);
    qtn_ternary(&mut tree);
    qtn_sort(&mut tree);
    let mut ex = Qtn::of(target);
    qtn_ternary(&mut ex);
    qtn_sort(&mut ex);
    let subs = (substitute.items() > 0).then(|| Qtn::of(substitute));
    let mut isfind = false;
    let Some(tree) = qtn_find_subquery(Some(tree), &ex, subs.as_ref(), &mut isfind) else {
        return QNode::Empty;
    };
    qtn_to_qnode(&qtn_binary(tree))
}

pub(crate) fn execute(node: &QNode, source: &dyn MatchSource, skip_not: bool) -> Ternary {
    match node {
        QNode::Empty => Ternary::No,
        QNode::Val { .. } => match source.positions(node, false) {
            MaybePositions::No => Ternary::No,
            MaybePositions::Maybe => Ternary::Maybe,
            MaybePositions::Yes(_) => Ternary::Yes,
        },
        QNode::Not(inner) => {
            if skip_not {
                return Ternary::Yes;
            }
            match execute(inner, source, skip_not) {
                Ternary::No => Ternary::Yes,
                Ternary::Yes => Ternary::No,
                Ternary::Maybe => Ternary::Maybe,
            }
        }
        QNode::And(l, r) => {
            let lmatch = execute(l, source, skip_not);
            if lmatch == Ternary::No {
                return Ternary::No;
            }
            match execute(r, source, skip_not) {
                Ternary::No => Ternary::No,
                Ternary::Yes => lmatch,
                Ternary::Maybe => Ternary::Maybe,
            }
        }
        QNode::Or(l, r) => {
            let lmatch = execute(l, source, skip_not);
            if lmatch == Ternary::Yes {
                return Ternary::Yes;
            }
            match execute(r, source, skip_not) {
                Ternary::No => lmatch,
                Ternary::Yes => Ternary::Yes,
                Ternary::Maybe => Ternary::Maybe,
            }
        }
        QNode::Phrase { .. } => {
            let mut data = PhraseData::default();
            // A phrase needs position data; a "maybe" counts as no match at
            // the top level, as PostgreSQL treats it.
            match phrase_execute(node, source, Some(&mut data)) {
                Ternary::Yes => Ternary::Yes,
                _ => Ternary::No,
            }
        }
    }
}

// ---------------------------------------------------------------- operators

/// `tsquery && tsquery`: AND of the two queries (the empty one is identity).
pub(crate) fn query_and(a: &QNode, b: &QNode) -> QNode {
    match (a, b) {
        (QNode::Empty, other) | (other, QNode::Empty) => other.clone(),
        _ => QNode::And(Box::new(a.clone()), Box::new(b.clone())),
    }
}

/// `tsquery || tsquery`: OR of the two queries.
pub(crate) fn query_or(a: &QNode, b: &QNode) -> QNode {
    match (a, b) {
        (QNode::Empty, other) | (other, QNode::Empty) => other.clone(),
        _ => QNode::Or(Box::new(a.clone()), Box::new(b.clone())),
    }
}

/// `tsquery <-> tsquery` and `tsquery_phrase`: a phrase with a distance.
pub(crate) fn query_phrase(a: &QNode, b: &QNode, distance: i16) -> QNode {
    match (a, b) {
        (QNode::Empty, other) | (other, QNode::Empty) => other.clone(),
        _ => QNode::Phrase {
            distance,
            left: Box::new(a.clone()),
            right: Box::new(b.clone()),
        },
    }
}

/// `!!tsquery`: a negation (the empty query stays empty).
pub(crate) fn query_not(a: &QNode) -> QNode {
    match a {
        QNode::Empty => QNode::Empty,
        _ => QNode::Not(Box::new(a.clone())),
    }
}

/// The lexemes a query mentions, sorted and deduplicated.
fn query_lexemes(node: &QNode, out: &mut Vec<String>) {
    match node {
        QNode::Empty => {}
        QNode::Val { word, .. } => out.push(word.clone()),
        QNode::Not(inner) => query_lexemes(inner, out),
        QNode::And(l, r) | QNode::Or(l, r) => {
            query_lexemes(l, out);
            query_lexemes(r, out);
        }
        QNode::Phrase { left, right, .. } => {
            query_lexemes(left, out);
            query_lexemes(right, out);
        }
    }
}

/// `tsquery @> tsquery`: every lexeme of the right appears in the left.
pub(crate) fn query_contains(a: &QNode, b: &QNode) -> bool {
    let mut left = Vec::new();
    let mut right = Vec::new();
    query_lexemes(a, &mut left);
    query_lexemes(b, &mut right);
    left.sort();
    left.dedup();
    right.sort();
    right.dedup();
    if right.len() > left.len() {
        return false;
    }
    // Both sorted: each right lexeme must appear in left.
    let mut left_index = 0usize;
    for word in &right {
        while left_index < left.len() && &left[left_index] != word {
            left_index += 1;
        }
        if left_index == left.len() {
            return false;
        }
    }
    true
}

/// `tsquery <@ tsquery`.
pub(crate) fn query_contained(a: &QNode, b: &QNode) -> bool {
    query_contains(b, a)
}

// ---------------------------------------------------------------- functions

/// `length(tsvector)`: the number of lexemes.
pub(crate) fn vector_length(vector: &TsVector) -> i64 {
    vector.entries.len() as i64
}

/// `strip(tsvector)`: the vector without positions.
pub(crate) fn vector_strip(vector: &TsVector) -> TsVector {
    TsVector {
        entries: vector
            .entries
            .iter()
            .map(|entry| Entry {
                lexeme: entry.lexeme.clone(),
                positions: Vec::new(),
            })
            .collect(),
    }
}

/// The weight letter `setweight` accepts.
pub(crate) fn weight_letter(text: &str) -> Result<u8, String> {
    // PostgreSQL reads the weight as a `"char"`, whose byte is the first one
    // of the value as written (the empty string reads as a zero byte).
    let byte = text.as_bytes().first().copied().unwrap_or(0);
    match byte {
        b'A' | b'a' => Ok(3),
        b'B' | b'b' => Ok(2),
        b'C' | b'c' => Ok(1),
        b'D' | b'd' => Ok(0),
        other => Err(db(
            format!("unrecognized weight: {other}"),
            // `elog(ERROR)`, without a specific code.
            "XX000",
        )),
    }
}

/// `setweight(tsvector, "char" [, text[]])`: assign a weight to all
/// positions, or only to the listed lexemes.
pub(crate) fn vector_setweight(vector: &TsVector, weight: u8, only: Option<&[String]>) -> TsVector {
    let mut out = vector.clone();
    for entry in &mut out.entries {
        if let Some(only) = only
            && !only.iter().any(|word| *word == entry.lexeme)
        {
            continue;
        }
        for pos in &mut entry.positions {
            pos.weight = weight;
        }
    }
    out
}

/// `numnode(tsquery)`: the number of items (lexemes and operators) stored.
pub(crate) fn query_numnode(query: &QNode) -> i64 {
    query.items() as i64
}

/// `tsvector_to_array`.
pub(crate) fn vector_to_array(vector: &TsVector) -> Vec<String> {
    vector.entries.iter().map(|e| e.lexeme.clone()).collect()
}

/// `array_to_tsvector`: one entry per (non-null, non-empty) word, sorted and
/// deduplicated, with no positions.
pub(crate) fn array_to_vector(words: &[Option<String>]) -> Result<TsVector, String> {
    let mut entries: Vec<Entry> = Vec::new();
    for word in words.iter().flatten() {
        if word.is_empty() {
            return Err(db("lexeme array may not contain empty strings", "2200F"));
        }
        entries.push(Entry {
            lexeme: word.clone(),
            positions: Vec::new(),
        });
    }
    entries.sort_by(|a, b| ts_compare_string(a.lexeme.as_bytes(), b.lexeme.as_bytes(), false));
    entries.dedup_by(|a, b| a.lexeme == b.lexeme);
    Ok(TsVector { entries })
}

/// `ts_delete(tsvector, text | text[])`: drop the named lexemes.
pub(crate) fn vector_delete(vector: &TsVector, words: &[String]) -> TsVector {
    TsVector {
        entries: vector
            .entries
            .iter()
            .filter(|entry| !words.iter().any(|word| *word == entry.lexeme))
            .cloned()
            .collect(),
    }
}

// -------------------------------------------------- the to_tsquery family

/// A query node before stop-word cleanup, with the placeholder a stop word
/// leaves (PostgreSQL's `QI_VALSTOP`).
enum MNode {
    Stop,
    Val {
        word: String,
        weight: u8,
        prefix: bool,
    },
    Not(Box<MNode>),
    And(Box<MNode>, Box<MNode>),
    Or(Box<MNode>, Box<MNode>),
    Phrase {
        distance: i16,
        left: Box<MNode>,
        right: Box<MNode>,
    },
}

/// The operator a morphed operand puts between its words.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum MorphOp {
    And,
    Phrase,
}

/// `pushval_morph`: an operand's dictionary words, one value per position,
/// joined by the morph's operator, with a placeholder for every position a
/// stop word left empty.
fn morph_operand(words: &[(String, usize)], weight: u8, prefix: bool, op: MorphOp) -> MNode {
    let join = |acc: Option<MNode>, node: MNode| match acc {
        None => node,
        Some(left) => match op {
            MorphOp::And => MNode::And(Box::new(left), Box::new(node)),
            MorphOp::Phrase => MNode::Phrase {
                distance: 1,
                left: Box::new(left),
                right: Box::new(node),
            },
        },
    };
    let mut acc: Option<MNode> = None;
    let mut pos = 0usize;
    let mut i = 0usize;
    while i < words.len() {
        let word_pos = words[i].1;
        while pos > 0 && pos + 1 < word_pos {
            acc = Some(join(acc, MNode::Stop));
            pos += 1;
        }
        pos = word_pos;
        let mut group: Option<MNode> = None;
        while i < words.len() && words[i].1 == word_pos {
            let value = MNode::Val {
                word: words[i].0.clone(),
                weight,
                prefix,
            };
            group = Some(match group {
                None => value,
                Some(g) => MNode::And(Box::new(g), Box::new(value)),
            });
            i += 1;
        }
        acc = Some(join(acc, group.expect("a group holds a word")));
    }
    acc.unwrap_or(MNode::Stop)
}

/// `clean_stopword_intree`: removes the placeholders, folding the distances
/// of the phrase nodes they collapse into the surviving parents.
fn clean_stopwords(node: MNode) -> (Option<MNode>, i32, i32) {
    match node {
        MNode::Stop => (None, 0, 0),
        MNode::Val { .. } => (Some(node), 0, 0),
        MNode::Not(inner) => {
            let (child, ladd, radd) = clean_stopwords(*inner);
            match child {
                None => (None, 0, 0),
                Some(child) => (Some(MNode::Not(Box::new(child))), ladd, radd),
            }
        }
        MNode::And(left, right) => {
            let is_and = true;
            let (l, lladd, lradd) = clean_stopwords(*left);
            let (r, rladd, rradd) = clean_stopwords(*right);
            match (l, r) {
                (None, None) => {
                    let add = lladd.max(rladd);
                    (None, add, add)
                }
                (Some(l), None) => (Some(l), lladd, lradd + rradd),
                (None, Some(r)) => (Some(r), lladd + rladd, rradd),
                (Some(l), Some(r)) => (
                    Some(if is_and {
                        MNode::And(Box::new(l), Box::new(r))
                    } else {
                        MNode::Or(Box::new(l), Box::new(r))
                    }),
                    0,
                    0,
                ),
            }
        }
        MNode::Or(left, right) => {
            let is_and = false;
            let (l, lladd, lradd) = clean_stopwords(*left);
            let (r, rladd, rradd) = clean_stopwords(*right);
            match (l, r) {
                (None, None) => {
                    let add = lladd.max(rladd);
                    (None, add, add)
                }
                (Some(l), None) => (Some(l), lladd, lradd + rradd),
                (None, Some(r)) => (Some(r), lladd + rladd, rradd),
                (Some(l), Some(r)) => (
                    Some(if is_and {
                        MNode::And(Box::new(l), Box::new(r))
                    } else {
                        MNode::Or(Box::new(l), Box::new(r))
                    }),
                    0,
                    0,
                ),
            }
        }
        MNode::Phrase {
            distance,
            left,
            right,
        } => {
            let (l, lladd, lradd) = clean_stopwords(*left);
            let (r, rladd, rradd) = clean_stopwords(*right);
            match (l, r) {
                (None, None) => {
                    let add = lladd + distance as i32 + rladd;
                    (None, add, add)
                }
                (Some(l), None) => (Some(l), lladd, lradd + distance as i32 + rradd),
                (None, Some(r)) => (Some(r), lladd + distance as i32 + rladd, rradd),
                (Some(l), Some(r)) => {
                    let distance = distance + (lradd + rladd) as i16;
                    (
                        Some(MNode::Phrase {
                            distance,
                            left: Box::new(l),
                            right: Box::new(r),
                        }),
                        lladd,
                        rradd,
                    )
                }
            }
        }
    }
}

/// The morphed tree as the query, once no placeholder can remain.
fn to_qnode(node: MNode) -> QNode {
    match node {
        MNode::Stop => QNode::Empty,
        MNode::Val {
            word,
            weight,
            prefix,
        } => QNode::Val {
            word,
            weight,
            prefix,
        },
        MNode::Not(inner) => QNode::Not(Box::new(to_qnode(*inner))),
        MNode::And(l, r) => QNode::And(Box::new(to_qnode(*l)), Box::new(to_qnode(*r))),
        MNode::Or(l, r) => QNode::Or(Box::new(to_qnode(*l)), Box::new(to_qnode(*r))),
        MNode::Phrase {
            distance,
            left,
            right,
        } => QNode::Phrase {
            distance,
            left: Box::new(to_qnode(*left)),
            right: Box::new(to_qnode(*right)),
        },
    }
}

/// Replaces every operand of a parsed query with its dictionary words.
fn morph_tree(node: &QNode, lexize: &impl Fn(&str) -> Vec<(String, usize)>, op: MorphOp) -> MNode {
    match node {
        QNode::Empty => MNode::Stop,
        QNode::Val {
            word,
            weight,
            prefix,
        } => morph_operand(&lexize(word), *weight, *prefix, op),
        QNode::Not(inner) => MNode::Not(Box::new(morph_tree(inner, lexize, op))),
        QNode::And(l, r) => MNode::And(
            Box::new(morph_tree(l, lexize, op)),
            Box::new(morph_tree(r, lexize, op)),
        ),
        QNode::Or(l, r) => MNode::Or(
            Box::new(morph_tree(l, lexize, op)),
            Box::new(morph_tree(r, lexize, op)),
        ),
        QNode::Phrase {
            distance,
            left,
            right,
        } => MNode::Phrase {
            distance: *distance,
            left: Box::new(morph_tree(left, lexize, op)),
            right: Box::new(morph_tree(right, lexize, op)),
        },
    }
}

/// The notice an empty query raises as the parser finds no lexemes.
fn no_lexemes_notice(buffer: &str) {
    crate::session_env::notice(crate::error_fields::DbError::new(format!(
        "text-search query doesn't contain lexemes: \"{buffer}\""
    )));
}

/// The notice a query that lost everything to stop words raises.
fn only_stopwords_notice() {
    crate::session_env::notice(crate::error_fields::DbError::new(
        "text-search query contains only stop words or doesn't contain lexemes, ignored",
    ));
}

/// Finishes a morphing query: the cleanup, its notice, and the canonical
/// spelling.
fn finish_query(tree: MNode) -> String {
    match clean_stopwords(tree).0 {
        None => {
            only_stopwords_notice();
            String::new()
        }
        Some(cleaned) => print_tsquery(&to_qnode(cleaned)),
    }
}

/// `to_tsquery(config, text)`: the standard grammar, each operand normalized
/// by the configuration's dictionary.
pub(crate) fn to_tsquery(config: crate::ts_dict::Config, text: &str) -> Result<String, String> {
    let parsed = parse_tsquery(text)?;
    if parsed == QNode::Empty {
        no_lexemes_notice(text);
        return Ok(String::new());
    }
    let lexize = |word: &str| crate::ts_dict::lexize_words(config, word);
    let tree = morph_tree(&parsed, &lexize, MorphOp::Phrase);
    Ok(finish_query(tree))
}

/// `plainto_tsquery(config, text)`: every word of the text, ANDed.
pub(crate) fn plainto_tsquery(
    config: crate::ts_dict::Config,
    text: &str,
) -> Result<String, String> {
    let lexize = |word: &str| crate::ts_dict::lexize_words(config, word);
    // The whole input is one operand.
    let tree = if text.is_empty() {
        no_lexemes_notice(text);
        return Ok(String::new());
    } else {
        morph_operand(&lexize(text), 0, false, MorphOp::And)
    };
    Ok(finish_query(tree))
}

/// `phraseto_tsquery(config, text)`: every word of the text, in a phrase.
pub(crate) fn phraseto_tsquery(
    config: crate::ts_dict::Config,
    text: &str,
) -> Result<String, String> {
    let lexize = |word: &str| crate::ts_dict::lexize_words(config, word);
    let tree = if text.is_empty() {
        no_lexemes_notice(text);
        return Ok(String::new());
    } else {
        morph_operand(&lexize(text), 0, false, MorphOp::Phrase)
    };
    Ok(finish_query(tree))
}

/// One token of a web search query.
enum WebToken {
    Value(String),
    And,
    Or,
    Not,
}

/// `gettoken_query_websearch`: `"quoted phrases"`, `-negation`, `or`, and
/// implicit AND between words, as PostgreSQL's tokenizer reads them. The
/// second value says whether the tokenizer ended waiting for an operand
/// (which then takes a stop-word placeholder).
fn web_tokens(text: &str) -> (Vec<WebToken>, bool) {
    use WebState::*;
    #[derive(Clone, Copy, PartialEq)]
    enum WebState {
        FirstOperand,
        Operand,
        Operator,
    }
    let chars: Vec<char> = text.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0usize;
    let mut state = FirstOperand;
    let is_operator = |c: char| matches!(c, '!' | '&' | '|' | '(' | ')' | '<');
    loop {
        match state {
            FirstOperand | Operand => {
                if i >= chars.len() {
                    // PostgreSQL tries one more token, finds none, and (past
                    // the first operand) leaves a stop-word placeholder.
                    return (tokens, state == Operand);
                }
                let c = chars[i];
                if c == '-' {
                    tokens.push(WebToken::Not);
                    i += 1;
                    state = Operand;
                    continue;
                }
                if c == '"' {
                    i += 1;
                    let start = i;
                    while i < chars.len() && chars[i] != '"' {
                        i += 1;
                    }
                    tokens.push(WebToken::Value(chars[start..i].iter().collect()));
                    if i < chars.len() {
                        i += 1;
                    }
                    state = Operator;
                    continue;
                }
                if is_operator(c) {
                    i += 1;
                    continue;
                }
                if c.is_whitespace() {
                    i += 1;
                    continue;
                }
                // A word: `:` ends it (and is consumed) unless the word
                // starts with one.
                let start = i;
                if chars[i] == ':' {
                    i += 1;
                }
                while i < chars.len()
                    && !chars[i].is_whitespace()
                    && !is_operator(chars[i])
                    && chars[i] != '"'
                    && chars[i] != ':'
                {
                    i += 1;
                }
                if i < chars.len() && chars[i] == ':' {
                    i += 1;
                }
                tokens.push(WebToken::Value(
                    chars[start..i.min(chars.len())]
                        .iter()
                        .collect::<String>()
                        .trim_end_matches(':')
                        .to_string(),
                ));
                state = Operator;
            }
            Operator => {
                if i >= chars.len() {
                    return (tokens, false);
                }
                let c = chars[i];
                if is_or_operator(&chars, i) {
                    tokens.push(WebToken::Or);
                    i += 2;
                    state = Operand;
                    continue;
                }
                if is_operator(c) {
                    i += 1;
                    continue;
                }
                if !c.is_whitespace() {
                    tokens.push(WebToken::And);
                    state = Operand;
                    continue;
                }
                i += 1;
            }
        }
    }
}

/// `parse_or_operator`: whether an `or` at this position is the operator:
/// a word of its own with some operand after it.
fn is_or_operator(chars: &[char], at: usize) -> bool {
    if !matches!(chars.get(at), Some('o' | 'O')) || !matches!(chars.get(at + 1), Some('r' | 'R')) {
        return false;
    }
    // It must not be part of a word.
    match chars.get(at + 2) {
        None => return false,
        Some('-') => return false,
        Some('_') => return false,
        Some(c) if c.is_alphanumeric() => return false,
        _ => {}
    }
    // And some operand must follow.
    let mut i = at + 2;
    loop {
        i += 1;
        match chars.get(i) {
            None => return false,
            Some(c) if !c.is_whitespace() => return true,
            _ => {}
        }
    }
}

/// `websearch_to_tsquery(config, text)`.
pub(crate) fn websearch_to_tsquery(
    config: crate::ts_dict::Config,
    text: &str,
) -> Result<String, String> {
    let lexize = |word: &str| crate::ts_dict::lexize_words(config, word);
    let (tokens, expects_operand) = web_tokens(text);
    if tokens.is_empty() {
        no_lexemes_notice(text);
        return Ok(String::new());
    }

    // A shunting-yard over the tokens, with PostgreSQL's priorities
    // (NOT 4, AND 2, OR 1).
    let rank = |t: &WebToken| match t {
        WebToken::Not => 4,
        WebToken::And => 2,
        WebToken::Or => 1,
        WebToken::Value(_) => 0,
    };
    let mut output: Vec<MNode> = Vec::new();
    let mut ops: Vec<WebToken> = Vec::new();
    // A missing operand is a stop word placeholder, as `pushStop` leaves.
    let apply = |output: &mut Vec<MNode>, op: &WebToken| match op {
        WebToken::Not => {
            let inner = output.pop().unwrap_or(MNode::Stop);
            output.push(MNode::Not(Box::new(inner)));
        }
        WebToken::And | WebToken::Or => {
            let right = output.pop().unwrap_or(MNode::Stop);
            let left = output.pop().unwrap_or(MNode::Stop);
            output.push(if matches!(op, WebToken::And) {
                MNode::And(Box::new(left), Box::new(right))
            } else {
                MNode::Or(Box::new(left), Box::new(right))
            });
        }
        WebToken::Value(_) => {}
    };
    for token in tokens {
        match token {
            WebToken::Value(value) => {
                let lowered = value;
                output.push(morph_operand(&lexize(&lowered), 0, false, MorphOp::Phrase));
            }
            op => {
                // `cleanOpStack`: stop when the incoming operator binds
                // tighter than the stack top (NOT is right associative).
                while let Some(top) = ops.last() {
                    let stop = if matches!(op, WebToken::Not) {
                        rank(&op) >= rank(top)
                    } else {
                        rank(&op) > rank(top)
                    };
                    if stop {
                        break;
                    }
                    let top = ops.pop().expect("checked");
                    apply(&mut output, &top);
                }
                ops.push(op);
            }
        }
    }
    // An operator left waiting for an operand takes a stop-word
    // placeholder, as `pushStop` gives it.
    if expects_operand {
        output.push(MNode::Stop);
    }
    while let Some(op) = ops.pop() {
        apply(&mut output, &op);
    }
    let tree = output.pop().unwrap_or(MNode::Stop);
    Ok(finish_query(tree))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vector(text: &str) -> TsVector {
        parse_tsvector(text).expect("valid vector")
    }

    fn query(text: &str) -> QNode {
        parse_tsquery(text).expect("valid query")
    }

    fn canon_vector(text: &str) -> String {
        print_tsvector(&vector(text))
    }

    fn canon_query(text: &str) -> String {
        print_tsquery(&query(text))
    }

    #[test]
    fn crc_values_match_postgresql() {
        // The legacy CRC-32 modern PostgreSQL computes for query operands.
        assert_eq!(legacy_crc32(b"a"), 0x17B7_BEBC, "crc(a)");
        assert_eq!(legacy_crc32(b"b"), 0x8EBE_EF06, "crc(b)");
    }

    #[test]
    fn tsvector_literals_print_canonically() {
        assert_eq!(canon_vector("fat:2 cat:1"), "'cat':1 'fat':2");
        assert_eq!(canon_vector("fat cat"), "'cat' 'fat'");
        assert_eq!(canon_vector(""), "");
        assert_eq!(canon_vector("a:1A"), "'a':1A");
        assert_eq!(
            canon_vector("fat:2,4 cat:3 rat:5A"),
            "'cat':3 'fat':2,4 'rat':5A"
        );
        assert_eq!(canon_vector("the"), "'the'");
        assert_eq!(canon_vector("fat:1A2"), "'fat':1A");
        assert_eq!(canon_vector("fat:2A fat:1B"), "'fat':1B,2A");
        assert!(parse_tsvector("fat:0").is_err());
        assert!(parse_tsvector("fat:1x").is_err());
    }

    #[test]
    fn tsvector_sizes_match_postgresql() {
        assert_eq!(vector("").stored_size(), 8);
        assert_eq!(vector("a").stored_size(), 13);
        assert_eq!(vector("a:1").stored_size(), 18);
        assert_eq!(vector("a:1A").stored_size(), 18);
        assert_eq!(vector("a b").stored_size(), 18);
        assert_eq!(vector("a:1,2").stored_size(), 20);
    }

    #[test]
    fn tsvector_order_matches_postgresql() {
        let less = |a: &str, b: &str| tsvector_cmp(&vector(a), &vector(b)) == Ordering::Less;
        assert!(less("a", "b"));
        assert!(!less("b", "a"));
        assert!(less("a", "aa"));
        assert!(!less("a:1", "a:2"));
        assert!(less("a:1A", "a:1B"));
        assert!(less("a", "a:1"));
        assert!(!less("a:1", "a"));
        assert!(less("a:1", "a:1,2"));
        assert!(!less("a:1,2", "a:1"));
    }

    #[test]
    fn tsquery_literals_print_canonically() {
        assert_eq!(canon_query("fat & rat"), "'fat' & 'rat'");
        assert_eq!(canon_query("fat & !rat"), "'fat' & !'rat'");
        assert_eq!(canon_query("fat <-> rat"), "'fat' <-> 'rat'");
        assert_eq!(canon_query("fat <2> rat"), "'fat' <2> 'rat'");
        assert_eq!(canon_query("(fat & rat) | cat"), "'fat' & 'rat' | 'cat'");
        assert_eq!(canon_query("fat:*"), "'fat':*");
        assert_eq!(canon_query("fat:*A"), "'fat':*A");
        assert_eq!(canon_query("((a))"), "'a'");
        assert_eq!(canon_query("b & a"), "'b' & 'a'");
        assert_eq!(canon_query("a & a"), "'a' & 'a'");
        assert_eq!(canon_query("!(a & b)"), "!( 'a' & 'b' )");
        assert_eq!(canon_query("!a & b"), "!'a' & 'b'");
        assert_eq!(canon_query("a & b <-> c"), "'a' & 'b' <-> 'c'");
        assert_eq!(canon_query("a <2> b & c"), "'a' <2> 'b' & 'c'");
        assert_eq!(canon_query("a & b:AB"), "'a' & 'b':AB");
        assert_eq!(
            canon_query("(a | b) & (c | d)"),
            "( 'a' | 'b' ) & ( 'c' | 'd' )"
        );
        assert_eq!(canon_query("c <-> b <-> a"), "'c' <-> 'b' <-> 'a'");
        assert_eq!(canon_query("a <-> (b & c)"), "'a' <-> ( 'b' & 'c' )");
        assert_eq!(canon_query(""), "");
        assert!(parse_tsquery("a b").is_err());
        assert!(parse_tsquery("a &").is_err());
    }

    #[test]
    fn tsquery_ordering_matches_postgresql() {
        let less = |a: &str, b: &str| tsquery_cmp(&query(a), &query(b)) == Ordering::Less;
        assert!(less("a", "b"));
        assert!(!less("b", "a"));
        assert!(!less("a & b", "a"));
        assert!(less("a | b", "a & b"));
        assert!(!less("!a", "a"));
        // A binary node's operands compare right side first (the query is
        // stored in reverse polish), so the operands' CRCs decide.
        assert!(less("b | a", "a | b"));
        assert!(less("b & a", "a & b"));
        assert!(less("b <-> a", "a <-> b"));
        assert!(query("a") == query("a"));
        assert!(query("a & b") != query("b & a"));
    }

    #[test]
    fn tsquery_sizes_match_postgresql() {
        assert_eq!(query("").stored_size(), 8);
        assert_eq!(query("a").stored_size(), 8 + 12 + 2);
        assert_eq!(query("a & b").stored_size(), 48);
        assert_eq!(query("!(a & b) | c").stored_size(), 8 + 12 * 6 + 2 * 3);
    }

    #[test]
    fn prefix_matching_scans_every_entry() {
        let m = |v: &str, q: &str| matches(&vector(v), &query(q));
        assert!(m("a:1A b:2", "a:*"));
        assert!(m("a b:2", "a:*"));
        assert!(m("ab ac b", "a:*"));
        assert!(m("a:1A", "a:*"));
        assert!(!m("a:1A b:2", "a:D"));
        assert!(m("ab:1A ac:1", "a:*A"));
        assert!(!m("ab:1A ac:1", "a:A"));
        assert!(m("ab:1A ac:1B", "a:*A"));
        // A phrase operand never matches by prefix, as in PostgreSQL.
        assert!(!m("ab:1 ac:2", "a:* <-> c:*"));
        assert!(m("ab:1 ac:2", "ab <-> ac"));
        assert!(!m("ab:1 ac:2", "ab <-> c:*"));
    }

    #[test]
    fn weight_letters_match_postgresql() {
        assert_eq!(weight_letter("A"), Ok(3));
        assert_eq!(weight_letter("d"), Ok(0));
        // The byte of the value, as `"char"` reads it; an empty string is 0.
        assert!(
            weight_letter("E")
                .unwrap_err()
                .contains("unrecognized weight: 69")
        );
        assert!(
            weight_letter("")
                .unwrap_err()
                .contains("unrecognized weight: 0")
        );
        assert!(
            weight_letter("é")
                .unwrap_err()
                .contains("unrecognized weight: 195")
        );
    }

    #[test]
    fn empty_query_literal_is_canonical() {
        // An empty query keeps PostgreSQL's canonical spelling (and stages
        // its NOTICE where a session is listening).
        assert_eq!(tsquery_canonical("").expect("parses"), "");
        assert_eq!(tsquery_canonical(" ").expect("parses"), "");
    }

    #[test]
    fn matching_follows_tsquery_semantics() {
        let m = |v: &str, q: &str| matches(&vector(v), &query(q));
        // A position-less vector cannot satisfy a phrase.
        assert!(!m("fat cat sat", "fat <-> cat"));
        assert!(m("fat:1 cat:2 sat:3", "fat <-> cat"));
        assert!(!m("fat:1 cat:2 sat:3", "cat <-> fat"));
        assert!(!m("fat:1 cat:2 sat:3", "fat <-> sat"));
        assert!(m("fat:1 cat:2 sat:3", "fat <-> cat <-> sat"));
        assert!(!m("fat:1 cat:3 sat:4", "fat <2> sat"));
        assert!(m("fat:1 cat:3 sat:4", "fat <3> sat"));
        assert!(m("fat:1 cat:2", "fat <-> cat"));
        assert!(!m("fat:1 cat:3", "fat <-> cat"));
        assert!(m("fat:1A cat:2", "fat:A <-> cat"));
        assert!(m("fat:1A sat:2", "fat:B <-> sat") == false);
        assert!(m("fat cat", "fa:*"));
        assert!(!m("fat cat", "fa"));
        assert!(m("fat cat sat", "!dog"));
        assert!(m("fat cat sat", "fat & !dog"));
        assert!(!m("fat cat sat", "fat & !cat"));
        assert!(m("fat:1 cat:2 sat:3", "(fat <-> cat) & sat"));
        assert!(!m("fat cat", "fat <-> !cat"));
        assert!(!m("fat", ""));
    }

    #[test]
    fn the_query_family_matches_postgresql() {
        use crate::ts_dict::Config;
        let tq = |text: &str| to_tsquery(Config::English, text).expect("parses");
        assert_eq!(tq("fat & the & cat"), "'fat' & 'cat'");
        assert_eq!(tq("fat & !the"), "'fat'");
        assert_eq!(tq("the"), "");
        assert_eq!(tq("cat:*"), "'cat':*");
        assert_eq!(tq("cat:A"), "'cat':A");
        assert_eq!(tq("cats & runs"), "'cat' & 'run'");
        assert_eq!(tq("fat <-> the <-> cat"), "'fat' <2> 'cat'");
        assert_eq!(tq("(fat | the) & cat"), "'fat' & 'cat'");
        assert_eq!(tq(""), "");
        assert_eq!(
            plainto_tsquery(Config::English, "The Fat Cats").expect("parses"),
            "'fat' & 'cat'"
        );
        assert_eq!(
            plainto_tsquery(Config::English, "a-café-x 42").expect("parses"),
            "'a-café-x' & 'café' & 'x' & '42'"
        );
        assert_eq!(
            phraseto_tsquery(Config::English, "The Fat Cats").expect("parses"),
            "'fat' <-> 'cat'"
        );
        assert_eq!(
            phraseto_tsquery(Config::English, "fat the cat").expect("parses"),
            "'fat' <2> 'cat'"
        );
        let wq = |text: &str| websearch_to_tsquery(Config::English, text).expect("parses");
        assert_eq!(wq("fat cat or -dog"), "'fat' & 'cat' | !'dog'");
        assert_eq!(wq("\"fat cat\" dog"), "'fat' <-> 'cat' & 'dog'");
        assert_eq!(wq("a & b"), "'b'");
        assert_eq!(wq("foo -"), "'foo'");
        assert_eq!(wq("foo &"), "'foo'");
        assert_eq!(wq("a:b:c"), "'b' & 'c'");
        assert_eq!(wq("foo | bar"), "'foo' & 'bar'");
        assert_eq!(wq("the cat"), "'cat'");
        assert_eq!(wq(""), "");
    }

    #[test]
    fn operators_and_functions() {
        assert_eq!(canon_query(""), "");
        assert_eq!(
            print_tsquery(&query_and(&query("a"), &query("b"))),
            "'a' & 'b'"
        );
        assert_eq!(print_tsquery(&query_and(&query("a"), &query(""))), "'a'");
        assert_eq!(
            print_tsquery(&query_phrase(&query("a"), &query("b"), 3)),
            "'a' <3> 'b'"
        );
        assert_eq!(print_tsquery(&query_not(&query("a"))), "!'a'");
        assert!(query_contains(&query("a & b"), &query("a | b")));
        assert!(!query_contains(&query("a"), &query("a & b")));
        assert_eq!(
            print_tsvector(&tsvector_concat(&vector("fat:2A"), &vector("fat:1B"))),
            "'fat':2A,3B"
        );
        assert_eq!(print_tsvector(&vector_strip(&vector("fat:2A"))), "'fat'");
        assert_eq!(
            print_tsvector(&vector_setweight(&vector("fat:1 rat:2"), 3, None)),
            "'fat':1A 'rat':2A"
        );
        assert_eq!(vector_length(&vector("")), 0);
        assert_eq!(query_numnode(&query("a & b")), 3);
        assert_eq!(vector_to_array(&vector("fat")), ["fat".to_string()]);
        assert_eq!(
            print_tsvector(
                &array_to_vector(&[Some("fat".into()), Some("fat".into()), Some("rat".into())])
                    .expect("ok")
            ),
            "'fat' 'rat'"
        );
        assert!(array_to_vector(&[Some("fat".into()), Some(String::new())]).is_err());
        assert_eq!(
            print_tsvector(&vector_delete(&vector("fat:1 rat:2"), &["cat".into()])),
            "'fat':1 'rat':2"
        );
    }
}

#[cfg(test)]
mod rewrite_tests {
    use super::*;

    fn rewrite(query: &str, target: &str, substitute: &str) -> String {
        let query = parse_tsquery(query).expect("a query");
        let target = parse_tsquery(target).expect("a target");
        let substitute = parse_tsquery(substitute).expect("a substitute");
        print_tsquery(&query_rewrite(&query, &target, &substitute))
    }

    #[test]
    fn rewrites_match_postgresql() {
        // The values are PostgreSQL's own outputs.
        assert_eq!(rewrite("a & b", "a", "c"), "'b' & 'c'");
        assert_eq!(rewrite("a & b", "x", "c"), "'b' & 'a'");
        assert_eq!(rewrite("a | b", "a", "c"), "'b' | 'c'");
        assert_eq!(rewrite("a & b", "a & b", "c"), "'c'");
        assert_eq!(rewrite("a", "a", "b | c"), "'b' | 'c'");
        assert_eq!(rewrite("a & b & c", "b", "x & y"), "'x' & 'y' & 'c' & 'a'");
        assert_eq!(rewrite("a | (b & c)", "b", "x"), "'a' | 'x' & 'c'");
        assert_eq!(rewrite("!a & b", "b", "c"), "'c' & !'a'");
        assert_eq!(rewrite("a:* & b", "a:*", "c"), "'b' & 'c'");
        assert_eq!(rewrite("a <-> b", "a", "c"), "'c' <-> 'b'");
        assert_eq!(rewrite("a", "b", "c"), "'a'");
        assert_eq!(rewrite("a <2> b", "a <-> b", "x"), "'a' <2> 'b'");
        assert_eq!(rewrite("a & a", "a", "b"), "'b' & 'b'");
        assert_eq!(rewrite("a & (a | c)", "a", "b"), "'b' & ( 'c' | 'b' )");
        assert_eq!(rewrite("((a & b) & c)", "a & b", "x"), "'c' & 'x'");
        assert_eq!(rewrite("a", "a", "a & b"), "'a' & 'b'");
        // A phrase target matches only its own distance.
        assert_eq!(rewrite("a <-> b <-> c", "a <-> b", "x"), "'x' <-> 'c'");
        // The empty substitute erases the target.
        assert_eq!(rewrite("a & b", "a", ""), "'b'");
        assert_eq!(rewrite("a & b", "a & b", ""), "");
    }
}
