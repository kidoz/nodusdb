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
fn ts_compare_string(a: &[u8], b: &[u8], prefix: bool) -> Ordering {
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

    fn find(&self, lexeme: &str) -> Option<&Entry> {
        self.entries
            .binary_search_by(|e| ts_compare_string(e.lexeme.as_bytes(), lexeme.as_bytes(), false))
            .ok()
            .map(|i| &self.entries[i])
    }

    /// The entries a lexeme prefix matches, in stored order. As PostgreSQL
    /// does, the exact term is found by binary search, and the entries that
    /// have it as a prefix immediately follow it.
    fn find_prefix(&self, prefix: &str) -> &[Entry] {
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
        &self.entries[start..end]
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
    // A value sorts before an operator.
    let (a_op, b_op) = (op_rank(a) > 0, op_rank(b) > 0);
    if a_op != b_op {
        return if a_op {
            Ordering::Greater
        } else {
            Ordering::Less
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
    fn empty_query_literal_is_canonical() {
        // An empty query keeps PostgreSQL's canonical spelling (and stages
        // its NOTICE where a session is listening).
        assert_eq!(tsquery_canonical("").expect("parses"), "");
        assert_eq!(tsquery_canonical(" ").expect("parses"), "");
    }
}
