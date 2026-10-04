//! PostgreSQL's ranking of a tsvector against a tsquery: `ts_rank` (the
//! occurrence-and-weight ranking of `tsrank.c`) and, later here,
//! `ts_rank_cd` (the cover-density ranking).

use crate::textsearch::{Pos, QNode, TsVector};

/// The default weights, in PostgreSQL's order: D, C, B, A.
pub(crate) const DEFAULT_WEIGHTS: [f32; 4] = [0.1, 0.2, 0.4, 1.0];

/// A method's bits.
pub(crate) const RANK_NO_NORM: i32 = 0x00;
pub(crate) const RANK_NORM_LOGLENGTH: i32 = 0x01;
pub(crate) const RANK_NORM_LENGTH: i32 = 0x02;
pub(crate) const RANK_NORM_EXTDIST: i32 = 0x04;
pub(crate) const RANK_NORM_UNIQ: i32 = 0x08;
pub(crate) const RANK_NORM_LOGUNIQ: i32 = 0x10;
pub(crate) const RANK_NORM_RDIVRPLUS1: i32 = 0x20;

/// `word_distance`: a weight for a collocation `w` positions apart.
fn word_distance(w: i32) -> f32 {
    if w > 100 {
        return 1e-30;
    }
    (1.0 / (1.005 + 0.05 * ((f64::from(w)) / 1.5 - 2.0).exp())) as f32
}

/// `cnt_length`: the number of positions, one per position-less lexeme.
fn cnt_length(vector: &TsVector) -> i32 {
    let mut len = 0;
    for entry in &vector.entries {
        if entry.positions.is_empty() {
            len += 1;
        } else {
            len += entry.positions.len() as i32;
        }
    }
    len
}

/// The query's operands, sorted by word and de-duplicated, as
/// `SortAndUniqItems` leaves them.
fn query_operands(query: &QNode) -> Vec<(String, bool)> {
    fn collect(node: &QNode, out: &mut Vec<(String, bool)>) {
        match node {
            QNode::Empty => {}
            QNode::Val { word, prefix, .. } => out.push((word.clone(), *prefix)),
            QNode::Not(inner) => collect(inner, out),
            QNode::And(l, r) | QNode::Or(l, r) => {
                collect(l, out);
                collect(r, out);
            }
            QNode::Phrase { left, right, .. } => {
                collect(left, out);
                collect(right, out);
            }
        }
    }
    let mut items = Vec::new();
    collect(query, &mut items);
    items.sort_by(|a, b| a.0.cmp(&b.0));
    items.dedup_by(|a, b| a.0 == b.0);
    items
}

/// The entries an operand matches: the one exact entry, or every entry its
/// prefix covers.
fn matching_entries<'a>(
    vector: &'a TsVector,
    word: &str,
    prefix: bool,
) -> Vec<(&'a str, &'a [Pos])> {
    if prefix {
        vector
            .find_prefix(word)
            .iter()
            .map(|e| (e.lexeme.as_str(), e.positions.as_slice()))
            .collect()
    } else {
        vector
            .find(word)
            .map(|e| vec![(e.lexeme.as_str(), e.positions.as_slice())])
            .unwrap_or_default()
    }
}

/// A position-less entry reads as one position of weight D at the largest
/// position (PostgreSQL's `POSNULL`).
const POSNULL: Pos = Pos {
    value: (16384 - 1) as u16,
    weight: 0,
};

/// `calc_rank_or`.
fn calc_rank_or(w: &[f32; 4], vector: &TsVector, query: &QNode) -> f32 {
    let items = query_operands(query);
    let size = items.len();
    let mut res: f32 = 0.0;
    for (word, prefix) in &items {
        for (_, positions) in matching_entries(vector, word, *prefix) {
            let post: &[Pos] = if positions.is_empty() {
                std::slice::from_ref(&POSNULL)
            } else {
                positions
            };
            let mut resj: f32 = 0.0;
            let mut wjm: f32 = -1.0;
            let mut jm = 0usize;
            for (j, pos) in post.iter().enumerate() {
                let weight = w[pos.weight as usize];
                resj += weight / ((j + 1) * (j + 1)) as f32;
                if weight > wjm {
                    wjm = weight;
                    jm = j;
                }
            }
            // The inner sum is float arithmetic, the division by
            // pi^2/6 is double, as in PostgreSQL.
            let inner = wjm + resj - wjm / ((jm + 1) * (jm + 1)) as f32;
            res = (f64::from(res) + f64::from(inner) / 1.64493406685) as f32;
        }
    }
    if size > 0 {
        res /= size as f32;
    }
    res
}

/// `calc_rank_and`.
fn calc_rank_and(w: &[f32; 4], vector: &TsVector, query: &QNode) -> f32 {
    let items = query_operands(query);
    if items.len() < 2 {
        return calc_rank_or(w, vector, query);
    }
    let mut res: f32 = -1.0;
    // The current position vector of each item, kept across the items that
    // follow, as PostgreSQL's `pos[]` array is.
    let mut pos: Vec<Option<Vec<Pos>>> = vec![None; items.len()];
    let mut posnull: Vec<bool> = vec![false; items.len()];
    for i in 0..items.len() {
        let (word, prefix) = &items[i];
        for (_, positions) in matching_entries(vector, word, *prefix) {
            let null = positions.is_empty();
            let current: Vec<Pos> = if null {
                vec![POSNULL]
            } else {
                positions.to_vec()
            };
            for k in 0..i {
                let Some(other) = &pos[k] else {
                    continue;
                };
                let other_null = posnull[k];
                for post in &current {
                    for ct in other {
                        let mut dist = (i32::from(post.value) - i32::from(ct.value)).abs();
                        if dist == 0 && !(null || other_null) {
                            // Two real positions never collide.
                            continue;
                        }
                        if dist == 0 {
                            dist = 16384;
                        }
                        let product =
                            w[post.weight as usize] * w[ct.weight as usize] * word_distance(dist);
                        let curw = (f64::from(product)).sqrt() as f32;
                        res = if res < 0.0 {
                            curw
                        } else {
                            (1.0 - (1.0 - f64::from(res)) * (1.0 - f64::from(curw))) as f32
                        };
                    }
                }
            }
            pos[i] = Some(current);
            posnull[i] = null;
        }
    }
    res
}

/// `calc_rank`: the ranking with its normalizations.
pub(crate) fn calc_rank(w: &[f32; 4], vector: &TsVector, query: &QNode, method: i32) -> f32 {
    let mut res: f32 = 0.0;
    if vector.entries.is_empty() || query_is_empty(query) {
        return 0.0;
    }
    let top = top_operator(query);
    res = match top {
        Some(TopOp::And | TopOp::Phrase) => calc_rank_and(w, vector, query),
        _ => calc_rank_or(w, vector, query),
    };
    if res < 0.0 {
        res = 1e-20;
    }
    if method & RANK_NORM_LOGLENGTH != 0 {
        res = (f64::from(res) / (((cnt_length(vector) + 1) as f64).log2())) as f32;
    }
    if method & RANK_NORM_LENGTH != 0 {
        let len = cnt_length(vector);
        if len > 0 {
            res /= len as f32;
        }
    }
    if method & RANK_NORM_UNIQ != 0 {
        res /= vector.entries.len() as f32;
    }
    if method & RANK_NORM_LOGUNIQ != 0 {
        res = (f64::from(res) / (((vector.entries.len() + 1) as f64).log2())) as f32;
    }
    if method & RANK_NORM_RDIVRPLUS1 != 0 {
        res /= res + 1.0;
    }
    res
}

/// The query's top-level operator, when it has one.
enum TopOp {
    And,
    Phrase,
    Other,
}

fn top_operator(query: &QNode) -> Option<TopOp> {
    match query {
        QNode::And(..) => Some(TopOp::And),
        QNode::Phrase { .. } => Some(TopOp::Phrase),
        QNode::Or(..) | QNode::Not(..) => Some(TopOp::Other),
        _ => None,
    }
}

fn query_is_empty(query: &QNode) -> bool {
    matches!(query, QNode::Empty)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::textsearch::{parse_tsquery, parse_tsvector};

    fn rank(vector: &str, query: &str) -> f32 {
        calc_rank(
            &DEFAULT_WEIGHTS,
            &parse_tsvector(vector).expect("vector"),
            &parse_tsquery(query).expect("query"),
            RANK_NO_NORM,
        )
    }

    #[test]
    fn cover_density_ranks_match_postgresql() {
        let cd = |vector: &str, query: &str, method: i32| {
            calc_rank_cd(
                &DEFAULT_WEIGHTS,
                &parse_tsvector(vector).expect("vector"),
                &parse_tsquery(query).expect("query"),
                method,
            )
        };
        // Ground truth from PostgreSQL 18.4.
        assert_eq!(cd("a:1 b:2", "a & b", 0), 0.1);
        assert_eq!(cd("fat:1 cat:2 sat:3", "fat <-> cat", 0), 0.1);
        assert_eq!(cd("fat:1A cat:2", "fat & cat", 0), 0.18181819);
        assert_eq!(cd("fat:1 cat:2", "fat", 32), 0.09090909);
        assert_eq!(cd("fat:1 cat:2 sat:3 dog:4", "fat & dog", 0), 0.033333335);
        assert_eq!(cd("fat:1 cat:2 sat:3 dog:4", "fat & cat", 0), 0.1);
        assert_eq!(cd("fat:1 cat:2 sat:3 dog:4", "fat | dog", 0), 0.2);
        assert_eq!(cd("a:1 b:2 c:3 a:4 b:5 c:6", "a & b", 0), 0.25);
        assert_eq!(cd("a:1 b:2 c:3", "a <2> c", 0), 0.05);
        assert_eq!(cd("fat:1 fat:2 fat:3 fat:4 fat:5", "fat", 0), 0.5);
        assert_eq!(cd("a:1 b:2", "!z", 0), 0.0);
        assert_eq!(cd("", "fat", 0), 0.0);
        assert_eq!(cd("a:1 b:2", "", 0), 0.0);
    }

    #[test]
    fn ranks_match_postgresql() {
        // Ground truth from PostgreSQL 18.4.
        assert_eq!(rank("a:1 b:2", "a"), 0.06079271);
        assert_eq!(rank("a:1 b:2", "a & b"), 0.09910322);
        assert_eq!(rank("a:1A b:2", "a"), 0.6079271);
        assert_eq!(rank("a:1 b:2 c:3", "a & b & c"), 0.26832977);
        assert_eq!(rank("fat:1 cat:2 sat:3", "fat <-> cat"), 0.09910322);
        assert_eq!(rank("fat:1 cat:2 sat:3", "fat | sat"), 0.06079271);
        assert_eq!(rank("fat:1 cat:2", "cat & !fat"), 0.09910322);
        assert_eq!(rank("fat:1 cat:2", ""), 0.0);
        assert_eq!(rank("", "fat"), 0.0);
        assert_eq!(rank("fat:1 cat:2", "dog"), 0.0);
        assert_eq!(rank("fat:1 cat:2", "fat"), 0.06079271);
        assert_eq!(rank("ab:1 ac:2", "a:*"), 0.12158542);
        assert_eq!(rank("ab:1 ac:2 ad:3", "a:*"), 0.18237813);
        assert_eq!(rank("a:1,2,3 b:4", "a"), 0.082745634);
        assert_eq!(rank("fat:1 ab:2 ac:3", "fa:*"), 0.06079271);
    }
}

// ------------------------------------------------------- cover density

/// One documented position: where it is, the entry it came from, and the
/// query operands that matched there.
struct DocEntry {
    pos: Pos,
    entry: usize,
    items: Vec<usize>,
}

/// The positions a cover scan has collected for one query operand.
#[derive(Default)]
struct QrOperand {
    exists: bool,
    pos: Vec<u16>,
}

/// A cover of the document: the span `begin..=end` of documented positions
/// that satisfies the query, and its position bounds `p..=q`.
#[derive(Default)]
struct CoverExt {
    pos: usize,
    p: i32,
    q: i32,
    begin: usize,
    end: usize,
}

/// The match source of a cover scan: the operands' collected positions.
struct QueryRepresentation<'a> {
    operands: Vec<QrOperand>,
    nodes: &'a [&'a QNode],
}

impl crate::textsearch::MatchSource for QueryRepresentation<'_> {
    fn positions(&self, node: &QNode, want_positions: bool) -> crate::textsearch::MaybePositions {
        use crate::textsearch::MaybePositions;
        let Some(i) = self.nodes.iter().position(|n| std::ptr::eq(*n, node)) else {
            return MaybePositions::No;
        };
        let data = &self.operands[i];
        if !data.exists {
            return MaybePositions::No;
        }
        if want_positions {
            MaybePositions::Yes(data.pos.iter().map(|p| i32::from(*p)).collect())
        } else {
            MaybePositions::Yes(Vec::new())
        }
    }
}

/// The query's value nodes, in traversal order; each occurrence is its own
/// operand.
fn value_nodes(query: &QNode) -> Vec<&QNode> {
    fn collect<'a>(node: &'a QNode, out: &mut Vec<&'a QNode>) {
        match node {
            QNode::Empty => {}
            QNode::Val { .. } => out.push(node),
            QNode::Not(inner) => collect(inner, out),
            QNode::And(l, r) | QNode::Or(l, r) => {
                collect(l, out);
                collect(r, out);
            }
            QNode::Phrase { left, right, .. } => {
                collect(left, out);
                collect(right, out);
            }
        }
    }
    let mut nodes = Vec::new();
    collect(query, &mut nodes);
    nodes
}

/// The entries a node matches, by index, with their positions.
fn matching_entry_indexes<'a>(vector: &'a TsVector, node: &QNode) -> Vec<(usize, &'a [Pos])> {
    let QNode::Val { word, prefix, .. } = node else {
        return Vec::new();
    };
    if *prefix {
        let range = vector.prefix_indexes(word);
        range
            .map(|i| (i, vector.entries[i].positions.as_slice()))
            .collect()
    } else {
        vector
            .find_index(word)
            .map(|i| vec![(i, vector.entries[i].positions.as_slice())])
            .unwrap_or_default()
    }
}

/// `get_docrep`: the documented positions of the query's operands, sorted
/// and merged per entry and position. `None` when nothing matched.
fn get_docrep(vector: &TsVector, query: &QNode, nodes: &[&QNode]) -> Option<Vec<DocEntry>> {
    let mut doc: Vec<DocEntry> = Vec::new();
    for (i, node) in nodes.iter().enumerate() {
        let QNode::Val { weight, .. } = node else {
            continue;
        };
        for (entry, positions) in matching_entry_indexes(vector, node) {
            for pos in positions {
                if *weight == 0 || weight & (1 << pos.weight) != 0 {
                    doc.push(DocEntry {
                        pos: *pos,
                        entry,
                        items: vec![i],
                    });
                }
            }
        }
    }
    if doc.is_empty() {
        return None;
    }
    doc.sort_by(|a, b| {
        a.pos
            .value
            .cmp(&b.pos.value)
            .then(a.pos.weight.cmp(&b.pos.weight))
            .then(a.entry.cmp(&b.entry))
    });
    // Join the operands that matched at one entry and position.
    let mut merged: Vec<DocEntry> = Vec::with_capacity(doc.len());
    for entry in doc {
        match merged.last_mut() {
            Some(last) if last.pos == entry.pos && last.entry == entry.entry => {
                last.items.extend(entry.items);
            }
            _ => merged.push(entry),
        }
    }
    Some(merged)
}

/// `fillQueryRepresentationData`.
fn fill_operands(qr: &mut QueryRepresentation, doc: &DocEntry, reverse: bool) {
    for item in &doc.items {
        let data = &mut qr.operands[*item];
        data.exists = true;
        if data.pos.last() != Some(&doc.pos.value) {
            if reverse {
                data.pos.insert(0, doc.pos.value);
            } else {
                data.pos.push(doc.pos.value);
            }
        }
    }
}

/// `Cover`: the next cover at or after `ext.pos`, or `false` when none is
/// left.
fn cover(
    query: &QNode,
    doc: &[DocEntry],
    qr: &mut QueryRepresentation,
    ext: &mut CoverExt,
) -> bool {
    let mut lastpos = ext.pos;
    let mut found = false;
    for operands in qr.operands.iter_mut() {
        operands.exists = false;
        operands.pos.clear();
    }
    ext.p = i32::MAX;
    ext.q = 0;
    let mut ptr = ext.pos;
    // The upper bound: the first position from which the query matches.
    while ptr < doc.len() {
        fill_operands(qr, &doc[ptr], false);
        if crate::textsearch::execute(query, qr, false) != crate::textsearch::Ternary::No {
            if i32::from(doc[ptr].pos.value) > ext.q {
                ext.q = i32::from(doc[ptr].pos.value);
                ext.end = ptr;
                lastpos = ptr;
                found = true;
            }
            break;
        }
        ptr += 1;
    }
    if !found {
        return false;
    }
    for operands in qr.operands.iter_mut() {
        operands.exists = false;
        operands.pos.clear();
    }
    // The lower bound: scan back from the upper bound.
    let mut ptr = lastpos;
    loop {
        fill_operands(qr, &doc[ptr], true);
        if crate::textsearch::execute(query, qr, false) != crate::textsearch::Ternary::No {
            if i32::from(doc[ptr].pos.value) < ext.p {
                ext.begin = ptr;
                ext.p = i32::from(doc[ptr].pos.value);
            }
            break;
        }
        if ptr == ext.pos {
            break;
        }
        ptr -= 1;
    }
    if ext.p <= ext.q {
        ext.pos = ptr + 1;
        return true;
    }
    ext.pos += 1;
    cover(query, doc, qr, ext)
}

/// `calc_rank_cd`: the cover-density ranking.
pub(crate) fn calc_rank_cd(
    weights: &[f32; 4],
    vector: &TsVector,
    query: &QNode,
    method: i32,
) -> f32 {
    let mut invws = [0.0f64; 4];
    for i in 0..4 {
        let weight = if weights[i] >= 0.0 {
            f64::from(weights[i])
        } else {
            f64::from(DEFAULT_WEIGHTS[i])
        };
        if weight > 1.0 {
            crate::eval_error::raise(
                crate::error_fields::DbError::new("weight out of range")
                    .code("22023")
                    .into_text(),
            );
            return f32::NAN;
        }
        invws[i] = 1.0 / weight;
    }
    let nodes = value_nodes(query);
    let Some(doc) = get_docrep(vector, query, &nodes) else {
        return 0.0;
    };
    let mut qr = QueryRepresentation {
        operands: (0..nodes.len()).map(|_| QrOperand::default()).collect(),
        nodes: &nodes,
    };
    let mut ext = CoverExt::default();
    let mut wdoc = 0.0f64;
    let mut sum_dist = 0.0f64;
    let mut prev_ext_pos = 0.0f64;
    let mut nextent = 0usize;
    while cover(query, &doc, &mut qr, &mut ext) {
        let mut inv_sum = 0.0f64;
        for entry in &doc[ext.begin..=ext.end] {
            inv_sum += invws[entry.pos.weight as usize];
        }
        let cpos = (ext.end - ext.begin + 1) as f64 / inv_sum;
        let mut nnoise = (ext.q - ext.p) - (ext.end - ext.begin) as i32;
        if nnoise < 0 {
            nnoise = (ext.end - ext.begin) as i32 / 2;
        }
        wdoc += cpos / (1.0 + f64::from(nnoise));
        let cur_ext_pos = f64::from(ext.q + ext.p) / 2.0;
        if nextent > 0 && cur_ext_pos > prev_ext_pos {
            sum_dist += 1.0 / (cur_ext_pos - prev_ext_pos);
        }
        prev_ext_pos = cur_ext_pos;
        nextent += 1;
    }
    if method & RANK_NORM_LOGLENGTH != 0 && !vector.entries.is_empty() {
        wdoc /= f64::from(cnt_length(vector) + 1).ln();
    }
    if method & RANK_NORM_LENGTH != 0 {
        let len = cnt_length(vector);
        if len > 0 {
            wdoc /= f64::from(len);
        }
    }
    if method & RANK_NORM_EXTDIST != 0 && nextent > 0 && sum_dist > 0.0 {
        wdoc /= f64::from(nextent as u32) / sum_dist;
    }
    if method & RANK_NORM_UNIQ != 0 && !vector.entries.is_empty() {
        wdoc /= vector.entries.len() as f64;
    }
    if method & RANK_NORM_LOGUNIQ != 0 && !vector.entries.is_empty() {
        wdoc /= f64::from(vector.entries.len() as u32 + 1).log2();
    }
    if method & RANK_NORM_RDIVRPLUS1 != 0 {
        wdoc /= wdoc + 1.0;
    }
    wdoc as f32
}
