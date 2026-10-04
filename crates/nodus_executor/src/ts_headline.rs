//! PostgreSQL's `ts_headline`: the document split into the parser's tokens,
//! the query's matches marked, and a fragment chosen and highlighted as
//! `wparser_def.c` and `ts_parse.c` do it.

use crate::textsearch::{MatchSource, QNode, Ternary, ts_compare_string};
use crate::ts_parse;
use std::cmp::Ordering;

/// The options a headline is generated with, as PostgreSQL defaults them.
pub(crate) struct Options {
    pub max_words: i32,
    pub min_words: i32,
    pub shortword: i32,
    pub max_fragments: i32,
    pub highlight_all: bool,
    pub start_sel: String,
    pub stop_sel: String,
    pub frag_delim: String,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            max_words: 35,
            min_words: 15,
            shortword: 3,
            max_fragments: 0,
            highlight_all: false,
            start_sel: "<b>".to_string(),
            stop_sel: "</b>".to_string(),
            frag_delim: " ... ".to_string(),
        }
    }
}

/// One token of the document, with the query operand it matched.
#[derive(Clone, Default)]
struct Word {
    text: String,
    ty: u8,
    pos: u16,
    /// The query operand this word matched, when any.
    item: Option<usize>,
    repeated: bool,
    selected: bool,
    replace: bool,
    skip: bool,
    inside: bool,
}

/// The document's words, as `HeadlineParsedText`.
struct Parsed {
    words: Vec<Word>,
    /// The lexeme position the next word starts at.
    vector_pos: usize,
}

/// The query's operands, by identity, as the C code's `QueryItem` pointers.
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
    let mut operands = Vec::new();
    collect(query, &mut operands);
    operands
}

/// `LIMITPOS`.
fn limitpos(pos: usize) -> u16 {
    pos.min(16383) as u16
}

/// `hladdword`.
fn add_word(parsed: &mut Parsed, text: &str, ty: u8) {
    parsed.words.push(Word {
        text: text.to_string(),
        ty,
        ..Word::default()
    });
}

/// `hlfinditem`: record the query operands a lexeme of the last-added word
/// matches, duplicating the word when several do.
fn find_item(parsed: &mut Parsed, operands: &[(String, bool)], pos: usize, lexeme: &str) {
    let Some(last) = parsed.words.len().checked_sub(1) else {
        return;
    };
    parsed.words[last].pos = limitpos(pos);
    for (i, (word, prefix)) in operands.iter().enumerate() {
        if ts_compare_string(word.as_bytes(), lexeme.as_bytes(), *prefix) != Ordering::Equal {
            continue;
        }
        if parsed.words[last].item.is_some() {
            let mut duplicate = parsed.words[last].clone();
            duplicate.item = Some(i);
            duplicate.repeated = true;
            parsed.words.push(duplicate);
        } else {
            parsed.words[last].item = Some(i);
        }
    }
}

/// `hlparsetext`: the document's tokens, with the query's matches marked.
fn parse_document(
    config: crate::ts_dict::Config,
    document: &str,
    operands: &[(String, bool)],
) -> Parsed {
    let mut parsed = Parsed {
        words: Vec::new(),
        vector_pos: 0,
    };
    for token in ts_parse::parse(document) {
        if token.text.len() >= 2047 {
            // `IGNORE_LONGLEXEME`: the token is skipped with a notice.
            crate::session_env::notice(
                crate::error_fields::DbError::new("word is too long to be indexed")
                    .detail("Words longer than 2047 characters are ignored.")
                    .code("54000"),
            );
            continue;
        }
        // Every token is a word of the headline, whatever its type.
        add_word(&mut parsed, &token.text, token.ty);
        // A recognized token takes a position, and its lexemes are matched
        // against the query.
        if let Some(lexemes) = crate::ts_dict::token_lexemes(config, token.ty, &token.text) {
            parsed.vector_pos += 1;
            let pos = parsed.vector_pos;
            for lexeme in &lexemes {
                find_item(&mut parsed, operands, pos, lexeme);
            }
        }
    }
    parsed
}

// ------------------------------------------------------------- locations

/// The headlines' match data for a query node: the lexeme positions it
/// covers (`ExecPhraseData`, as `TS_execute_locations` builds it).
#[derive(Clone, Default)]
pub(crate) struct Location {
    pub pos: Vec<i32>,
    pub width: i32,
    pub negate: bool,
}

/// `checkcondition_HL`: the words whose item is this operand, by their
/// lexeme positions.
fn check_condition(
    words: &[Word],
    operand: usize,
    want_positions: bool,
) -> crate::textsearch::MaybePositions {
    use crate::textsearch::MaybePositions;
    let mut positions: Vec<i32> = Vec::new();
    for word in words {
        if word.item != Some(operand) {
            continue;
        }
        if !want_positions {
            return MaybePositions::Yes(Vec::new());
        }
        if positions.last() != Some(&i32::from(word.pos)) {
            positions.push(i32::from(word.pos));
        }
    }
    if positions.is_empty() {
        MaybePositions::No
    } else {
        MaybePositions::Yes(positions)
    }
}

/// The match source of a locations scan.
struct HlMatches<'a> {
    words: &'a [Word],
    nodes: &'a [&'a QNode],
}

impl crate::textsearch::MatchSource for HlMatches<'_> {
    fn positions(&self, node: &QNode, want_positions: bool) -> crate::textsearch::MaybePositions {
        let Some(i) = self.nodes.iter().position(|n| std::ptr::eq(*n, node)) else {
            return crate::textsearch::MaybePositions::No;
        };
        check_condition(self.words, i, want_positions)
    }
}

/// The query's value nodes, in traversal order.
fn nodes_of(query: &QNode) -> Vec<&QNode> {
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

/// `TS_execute_locations`: the locations of a successful match, or `None`
/// when the query does not match. An empty list is a match with no
/// locations (a NOT, or a phrase that matched negatively).
fn execute_locations(source: &HlMatches, node: &QNode) -> Option<Vec<Location>> {
    match node {
        QNode::Empty => None,
        QNode::Val { .. } => {
            let crate::textsearch::MaybePositions::Yes(positions) = source.positions(node, true)
            else {
                return None;
            };
            Some(vec![Location {
                pos: positions,
                ..Location::default()
            }])
        }
        QNode::Not(inner) => match execute_locations(source, inner) {
            // The inner expression failed, so the NOT matches (with no
            // locations of its own).
            None => Some(Vec::new()),
            Some(_) => None,
        },
        QNode::And(l, r) => {
            let mut left = execute_locations(source, l)?;
            let right = execute_locations(source, r)?;
            left.extend(right);
            Some(left)
        }
        QNode::Or(l, r) => {
            let left = execute_locations(source, l);
            let right = execute_locations(source, r);
            if left.is_none() && right.is_none() {
                return None;
            }
            // An AND'able location from each combination of sub-matches
            // follows the disjunctive law; a side that matched without
            // locations (a NOT) contributes nothing.
            let left = left.unwrap_or_default();
            let right = right.unwrap_or_default();
            if left.is_empty() {
                return Some(right);
            }
            if right.is_empty() {
                return Some(left);
            }
            let mut combined = Vec::new();
            for ldata in &left {
                for rdata in &right {
                    let mut data = crate::textsearch::PhraseData::default();
                    crate::textsearch::phrase_output(
                        Some(&mut data),
                        &crate::textsearch::PhraseData {
                            pos: ldata.pos.clone(),
                            width: ldata.width,
                            negate: ldata.negate,
                        },
                        &crate::textsearch::PhraseData {
                            pos: rdata.pos.clone(),
                            width: rdata.width,
                            negate: rdata.negate,
                        },
                        crate::textsearch::TSPO_ALL,
                        0,
                        0,
                    );
                    // Report the larger width, as PostgreSQL notes.
                    data.width = ldata.width.max(rdata.width);
                    combined.push(Location {
                        pos: data.pos,
                        width: data.width,
                        negate: data.negate,
                    });
                }
            }
            Some(combined)
        }
        QNode::Phrase { .. } => {
            let mut data = crate::textsearch::PhraseData::default();
            // A phrase needs position data, and its own match decides.
            match crate::textsearch::phrase_execute(node, source, Some(&mut data)) {
                Ternary::Yes if !data.negate => Some(vec![Location {
                    pos: data.pos,
                    width: data.width,
                    negate: data.negate,
                }]),
                Ternary::Yes => Some(Vec::new()),
                _ => None,
            }
        }
    }
}

// ------------------------------------------------------------- selection

/// `TS_IDIGNORE`.
fn ts_idignore(ty: u8) -> bool {
    matches!(
        ty,
        ts_parse::TAG_T | ts_parse::PROTOCOL | ts_parse::SPACE | ts_parse::XMLENTITY
    )
}

/// `HLIDREPLACE`.
fn hlidreplace(ty: u8) -> bool {
    ty == ts_parse::TAG_T
}

/// `HLIDSKIP` (the same as `XMLHLIDSKIP` for the default parser).
fn hlidskip(ty: u8) -> bool {
    matches!(
        ty,
        ts_parse::URL_T | ts_parse::NUMHWORD | ts_parse::ASCIIHWORD | ts_parse::HWORD
    )
}

/// `NONWORDTOKEN`.
fn nonword_token(ty: u8) -> bool {
    ty == ts_parse::SPACE || hlidreplace(ty) || hlidskip(ty)
}

/// `NOENDTOKEN`.
fn noend_token(ty: u8) -> bool {
    nonword_token(ty)
        || matches!(
            ty,
            ts_parse::SCIENTIFIC
                | ts_parse::VERSIONNUMBER
                | ts_parse::DECIMAL_T
                | ts_parse::SIGNEDINT
                | ts_parse::UNSIGNEDINT
        )
        || ts_idignore(ty)
}

/// `INTERESTINGWORD`: a non-repeated search term.
fn interesting(words: &[Word], j: usize) -> bool {
    words[j].item.is_some() && !words[j].repeated
}

/// `BADENDPOINT`: not a word, or a short word, unless interesting.
fn bad_endpoint(words: &[Word], j: i32, shortword: i32) -> bool {
    if j < 0 || j as usize >= words.len() {
        return false;
    }
    let word = &words[j as usize];
    (noend_token(word.ty) || word.text.len() as i32 <= shortword) && !interesting(words, j as usize)
}

/// `hlCover`: the first word range at or after the lexeme position
/// `*nextpos` that satisfies the query, extending `*nextpos` past it.
fn hl_cover(
    words: &[Word],
    nodes: &[&QNode],
    query: &QNode,
    locations: &[Location],
    nextpos: &mut i32,
    p: &mut i32,
    q: &mut i32,
) -> bool {
    let mut pos = *nextpos;
    loop {
        // For each AND'ed query term or phrase, its first occurrence at or
        // after `pos`; the cover must start there at the latest.
        let mut pose: i32 = -1;
        for data in locations {
            let first = data.pos.iter().copied().find(|&endp| endp >= pos);
            let Some(first) = first else {
                return false; // no more matches for this term
            };
            if first > pose {
                pose = first;
            }
        }
        if pose < 0 {
            return false; // an empty list of locations
        }
        // ... and its last occurrence at or before `pose`.
        let mut posb: i32 = i32::MAX - 1;
        for data in locations {
            let last = data
                .pos
                .iter()
                .rev()
                .map(|&p| p - data.width)
                .find(|&startp| startp <= pose);
            let Some(last) = last else {
                return false;
            };
            if last < posb {
                posb = last;
            }
        }
        // A phrase match may cross `pos`; try the match starting there
        // anyway, as PostgreSQL notes.
        posb = posb.max(pos);

        if posb <= pose {
            // Convert the lexeme positions to indexes in the word list.
            let mut idxb: i32 = -1;
            let mut idxe: i32 = -1;
            for (i, word) in words.iter().enumerate() {
                if word.item.is_none() {
                    continue;
                }
                if idxb < 0 && i32::from(word.pos) >= posb {
                    idxb = i as i32;
                }
                if i32::from(word.pos) <= pose {
                    idxe = i as i32;
                } else {
                    break;
                }
            }
            if idxb >= 0 && idxe >= idxb {
                // The selected range must still satisfy the query.
                let source = HlMatches {
                    words: &words[idxb as usize..=idxe as usize],
                    nodes,
                };
                if crate::textsearch::execute(query, &source, false) != Ternary::No {
                    *nextpos = posb + 1;
                    *p = idxb;
                    *q = idxe;
                    return true;
                }
            }
        }

        // Any later workable match must start beyond `posb`.
        pos = posb + 1;
    }
}

/// `mark_fragment`: highlight the words from `startpos` to `endpos`.
fn mark_fragment(parsed: &mut Parsed, highlightall: bool, startpos: i32, endpos: i32) {
    for i in startpos..=endpos {
        let word = &mut parsed.words[i as usize];
        if word.item.is_some() {
            word.selected = true;
        }
        if !highlightall {
            if hlidreplace(word.ty) {
                word.replace = true;
            } else if hlidskip(word.ty) {
                word.skip = true;
            }
        } else if hlidskip(word.ty) {
            word.skip = true;
        }
        word.inside = !word.repeated;
    }
}

/// `get_next_fragment`: split a cover into fragments of at most `max_words`,
/// each ending at a query word.
fn get_next_fragment(
    words: &[Word],
    startpos: &mut i32,
    endpos: &mut i32,
    curlen: &mut i32,
    poslen: &mut i32,
    max_words: i32,
) {
    // First move startpos to an item.
    let mut i = *startpos;
    while i <= *endpos {
        *startpos = i;
        if interesting(words, i as usize) {
            break;
        }
        i += 1;
    }
    // Cut endpos to have only max_words.
    *curlen = 0;
    *poslen = 0;
    let mut i = *startpos;
    while i <= *endpos && *curlen < max_words {
        if !nonword_token(words[i as usize].ty) {
            *curlen += 1;
        }
        if interesting(words, i as usize) {
            *poslen += 1;
        }
        i += 1;
    }
    // If the cover was cut, move back endpos to a query item.
    if *endpos > i {
        *endpos = i;
        let mut back = i;
        while back >= *startpos {
            *endpos = back;
            if interesting(words, back as usize) {
                break;
            }
            if !nonword_token(words[back as usize].ty) {
                *curlen -= 1;
            }
            back -= 1;
        }
    }
}

/// One candidate fragment of `mark_hl_fragments` (`CoverPos`).
#[derive(Clone, Copy, Default)]
struct CoverPos {
    startpos: i32,
    endpos: i32,
    poslen: i32,
    curlen: i32,
    chosen: bool,
    excluded: bool,
}

/// The headline selector used when `MaxFragments > 0`.
#[allow(clippy::too_many_arguments)]
fn mark_hl_fragments(
    parsed: &mut Parsed,
    query: &QNode,
    nodes: &[&QNode],
    locations: &[Location],
    highlightall: bool,
    shortword: i32,
    min_words: i32,
    max_words: i32,
    max_fragments: i32,
) {
    // Get all covers.
    let mut covers: Vec<CoverPos> = Vec::new();
    let mut nextpos = 0;
    let (mut p, mut q) = (0, 0);
    let words = &parsed.words;
    while hl_cover(words, nodes, query, locations, &mut nextpos, &mut p, &mut q) {
        let mut startpos = p;
        let mut endpos = q;
        // Break each cover into fragments of at most max_words.
        while startpos <= endpos {
            let mut curlen = 0;
            let mut poslen = 0;
            get_next_fragment(
                words,
                &mut startpos,
                &mut endpos,
                &mut curlen,
                &mut poslen,
                max_words,
            );
            covers.push(CoverPos {
                startpos,
                endpos,
                curlen,
                poslen,
                chosen: false,
                excluded: false,
            });
            startpos = endpos + 1;
            endpos = q;
        }
    }

    let curwords = parsed.words.len() as i32;
    let mut num_f = 0;
    for _ in 0..max_fragments {
        // Choose the cover with the most items, then the fewest words.
        let mut maxitems = 0;
        let mut minwords = i32::MAX;
        let mut mini: i32 = -1;
        for (i, cover) in covers.iter().enumerate() {
            if !cover.chosen
                && !cover.excluded
                && (maxitems < cover.poslen
                    || (maxitems == cover.poslen && minwords > cover.curlen))
            {
                maxitems = cover.poslen;
                minwords = cover.curlen;
                mini = i as i32;
            }
        }
        let Some(mini) = (mini >= 0).then_some(mini as usize) else {
            break; // no selectable covers remain
        };
        covers[mini].chosen = true;
        let mut startpos = covers[mini].startpos;
        let mut endpos = covers[mini].endpos;
        let mut curlen = covers[mini].curlen;
        // Stretch the cover if it is shorter than max_words.
        if curlen < max_words {
            // Divide the stretch on both sides of the cover.
            let maxstretch = (max_words - curlen) / 2;
            let words = &parsed.words;
            let mut stretch = 0;
            let mut posmarker = startpos;
            let mut i = startpos - 1;
            while i >= 0 && stretch < maxstretch && !words[i as usize].inside {
                if !nonword_token(words[i as usize].ty) {
                    curlen += 1;
                    stretch += 1;
                }
                posmarker = i;
                i -= 1;
            }
            // Cut back startpos till we find a good endpoint.
            let mut i = posmarker;
            while i < startpos && bad_endpoint(words, i, shortword) {
                if !nonword_token(words[i as usize].ty) {
                    curlen -= 1;
                }
                i += 1;
            }
            startpos = i;
            // Now stretch the endpos as much as possible.
            let mut posmarker = endpos;
            let mut i = endpos + 1;
            while i < curwords && curlen < max_words && !words[i as usize].inside {
                if !nonword_token(words[i as usize].ty) {
                    curlen += 1;
                }
                posmarker = i;
                i += 1;
            }
            // Cut back endpos till we find a good endpoint.
            let mut i = posmarker;
            while i > endpos && bad_endpoint(words, i, shortword) {
                if !nonword_token(words[i as usize].ty) {
                    curlen -= 1;
                }
                i -= 1;
            }
            endpos = i;
        }
        covers[mini].startpos = startpos;
        covers[mini].endpos = endpos;
        covers[mini].curlen = curlen;
        mark_fragment(parsed, highlightall, startpos, endpos);
        num_f += 1;
        // Exclude covers overlapping this one from future consideration.
        for (i, cover) in covers.iter_mut().enumerate() {
            if i != mini
                && ((cover.startpos >= startpos && cover.startpos <= endpos)
                    || (cover.endpos >= startpos && cover.endpos <= endpos)
                    || (cover.startpos < startpos && cover.endpos > endpos))
            {
                cover.excluded = true;
            }
        }
    }

    // Show the first min_words words if nothing was marked.
    if num_f <= 0 {
        let mut startpos = 0;
        let mut curlen = 0;
        let mut endpos = -1;
        let mut i = 0;
        while i < curwords && curlen < min_words {
            if !nonword_token(parsed.words[i as usize].ty) {
                curlen += 1;
            }
            endpos = i;
            i += 1;
        }
        mark_fragment(parsed, highlightall, startpos, endpos);
        startpos = 0;
    }
}

/// The headline selector used when `MaxFragments == 0`.
#[allow(clippy::too_many_arguments)]
fn mark_hl_words(
    parsed: &mut Parsed,
    query: &QNode,
    nodes: &[&QNode],
    locations: &[Location],
    highlightall: bool,
    shortword: i32,
    min_words: i32,
    max_words: i32,
) {
    let mut bestb: i32 = -1;
    let mut beste: i32 = -1;
    let mut bestlen: i32 = -1;
    let mut bestcover = false;

    if !highlightall {
        // Examine all covers and select a headline using the best one.
        let mut nextpos = 0;
        let (mut p, mut q) = (0, 0);
        let words = &parsed.words;
        let curwords = words.len() as i32;
        while hl_cover(words, nodes, query, locations, &mut nextpos, &mut p, &mut q) {
            // Count words and interesting words within the cover, stopping
            // once we reach max_words.
            let mut curlen = 0;
            let mut poslen = 0;
            let mut posb = p;
            let mut pose = p;
            let mut i = p;
            while i <= q && curlen < max_words {
                if !nonword_token(words[i as usize].ty) {
                    curlen += 1;
                }
                if interesting(words, i as usize) {
                    poslen += 1;
                }
                pose = i;
                i += 1;
            }

            if curlen < max_words {
                // We have room to lengthen the headline.
                let mut i = i - 1;
                while i < curwords && curlen < max_words {
                    if i > q {
                        if !nonword_token(words[i as usize].ty) {
                            curlen += 1;
                        }
                        if interesting(words, i as usize) {
                            poslen += 1;
                        }
                    }
                    pose = i;
                    if !bad_endpoint(words, i, shortword) && curlen >= min_words {
                        break;
                    }
                    i += 1;
                }
                if curlen < min_words {
                    // Reached the end of the text with too few words: try
                    // to extend the headline to the left.
                    let mut i = p - 1;
                    while i >= 0 {
                        if !nonword_token(words[i as usize].ty) {
                            curlen += 1;
                        }
                        if interesting(words, i as usize) {
                            poslen += 1;
                        }
                        if curlen >= max_words {
                            break;
                        }
                        if !bad_endpoint(words, i, shortword) && curlen >= min_words {
                            break;
                        }
                        i -= 1;
                    }
                    posb = if i >= 0 { i } else { 0 };
                }
            } else {
                // Can't make the headline longer: consider shortening it to
                // avoid a bad endpoint.
                let mut i = if i > q { q } else { i };
                while curlen > min_words && i >= 0 {
                    if !bad_endpoint(words, i, shortword) {
                        break;
                    }
                    if !nonword_token(words[i as usize].ty) {
                        curlen -= 1;
                    }
                    if interesting(words, i as usize) {
                        poslen -= 1;
                    }
                    pose = i - 1;
                    i -= 1;
                }
            }

            // Adopt this headline if it is better than the last one.
            let poscover = posb <= p && pose >= q;
            if poscover > bestcover
                || (poscover == bestcover && poslen > bestlen)
                || (poscover == bestcover
                    && poslen == bestlen
                    && !bad_endpoint(words, pose, shortword)
                    && beste >= 0
                    && bad_endpoint(words, beste, shortword))
            {
                bestb = posb;
                beste = pose;
                bestlen = poslen;
                bestcover = poscover;
            }
        }

        // Nothing acceptable: select min_words words from the beginning.
        if bestlen < 0 {
            let mut curlen = 0;
            let mut pose: i32 = -1;
            let mut i = 0;
            while i < curwords && curlen < min_words {
                if !nonword_token(words[i as usize].ty) {
                    curlen += 1;
                }
                pose = i;
                i += 1;
            }
            bestb = 0;
            beste = pose;
        }
    } else {
        // Highlight-all mode: the headline is the whole document.
        bestb = 0;
        beste = parsed.words.len() as i32 - 1;
    }

    mark_fragment(parsed, highlightall, bestb, beste);
}

/// `generateHeadline`: the selected fragments as text.
fn generate_headline(parsed: &Parsed, options: &Options) -> String {
    let mut out = String::new();
    let mut numfragments = 0;
    let mut infrag = false;
    for word in &parsed.words {
        if word.inside && !word.repeated {
            if !infrag {
                infrag = true;
                numfragments += 1;
                // A fragment delimiter before every fragment but the first.
                if numfragments > 1 {
                    out.push_str(&options.frag_delim);
                }
            }
            if word.replace {
                out.push(' ');
            } else if !word.skip {
                if word.selected {
                    out.push_str(&options.start_sel);
                }
                out.push_str(&word.text);
                if word.selected {
                    out.push_str(&options.stop_sel);
                }
            }
        } else if !word.repeated && infrag {
            infrag = false;
        }
    }
    out
}

// ------------------------------------------------------------- options

/// A `DbError` with its SQLSTATE, as the caller's error protocol carries it.
fn db_error(message: &str, code: &str) -> String {
    crate::error_fields::DbError::new(message)
        .code(code)
        .into_text()
}

/// PostgreSQL's `isspace` for the C locale.
fn is_space(c: u8) -> bool {
    c == b' ' || (0x09..=0x0d).contains(&c)
}

/// `strtoint(val, ..., 10)` with a full-string match: `None` when the value
/// is not an integer, an error when it is out of range.
fn strtoint_10(value: &str) -> Result<Option<i32>, String> {
    let digits = value.strip_prefix(['+', '-']).unwrap_or(value);
    if digits.is_empty() || !digits.bytes().all(|c| c.is_ascii_digit()) {
        return Ok(None);
    }
    match value.parse::<i64>() {
        Ok(v) if i32::try_from(v).is_ok() => Ok(Some(v as i32)),
        _ => Err(db_error(
            &format!("value \"{value}\" is out of range for type integer"),
            "22003",
        )),
    }
}

/// `pg_strtoint32`: the option values that must be integers.
fn str_to_int32(value: &str) -> Result<i32, String> {
    let digits = value.strip_prefix(['+', '-']).unwrap_or(value);
    if digits.is_empty() || !digits.bytes().all(|c| c.is_ascii_digit()) {
        return Err(db_error(
            &format!("invalid input syntax for type integer: \"{value}\""),
            "22P02",
        ));
    }
    match value.parse::<i64>() {
        Ok(v) if i32::try_from(v).is_ok() => Ok(v as i32),
        _ => Err(db_error(
            &format!("value \"{value}\" is out of range for type integer"),
            "22003",
        )),
    }
}

/// `buildDefItem` + `defGetString`: an unquoted integer normalizes to its
/// decimal text; every other value keeps its spelling.
fn def_value_text(value: &str, was_quoted: bool) -> Result<String, String> {
    if !was_quoted && !value.is_empty() {
        if let Some(v) = strtoint_10(value)? {
            return Ok(v.to_string());
        }
    }
    Ok(value.to_string())
}

/// `deserialize_deflist`: the `name=value` pairs of an options string.
fn deserialize_deflist(text: &str) -> Result<Vec<(String, String)>, String> {
    #[derive(PartialEq)]
    enum State {
        WaitKey,
        InKey,
        InQKey,
        WaitEq,
        WaitValue,
        InSqValue,
        InDqValue,
        InWValue,
    }

    let bytes = text.as_bytes();
    let mut workspace: Vec<u8> = Vec::with_capacity(bytes.len() + 1);
    let mut key_start = 0usize;
    let mut key_end = 0usize;
    let mut value_start = 0usize;
    let mut result = Vec::new();
    let mut state = State::WaitKey;
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        match state {
            State::WaitKey => {
                if is_space(c) || c == b',' {
                    i += 1;
                } else if c == b'"' {
                    key_start = workspace.len();
                    state = State::InQKey;
                    i += 1;
                } else {
                    key_start = workspace.len();
                    workspace.push(c);
                    state = State::InKey;
                    i += 1;
                }
            }
            State::InKey => {
                if is_space(c) {
                    key_end = workspace.len();
                    workspace.push(0);
                    state = State::WaitEq;
                } else if c == b'=' {
                    key_end = workspace.len();
                    workspace.push(0);
                    state = State::WaitValue;
                } else {
                    workspace.push(c);
                }
                i += 1;
            }
            State::InQKey => {
                if c == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        // Copy only one of the two quotes.
                        workspace.push(c);
                        i += 2;
                        continue;
                    }
                    key_end = workspace.len();
                    workspace.push(0);
                    state = State::WaitEq;
                    i += 1;
                } else {
                    workspace.push(c);
                    i += 1;
                }
            }
            State::WaitEq => {
                if c == b'=' {
                    state = State::WaitValue;
                } else if !is_space(c) {
                    return Err(db_error(
                        &format!("invalid parameter list format: \"{text}\""),
                        "42601",
                    ));
                }
                i += 1;
            }
            State::WaitValue => {
                if c == b'\'' {
                    value_start = workspace.len();
                    state = State::InSqValue;
                } else if c == b'E' && i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 1;
                    value_start = workspace.len();
                    state = State::InSqValue;
                } else if c == b'"' {
                    value_start = workspace.len();
                    state = State::InDqValue;
                } else if !is_space(c) {
                    value_start = workspace.len();
                    workspace.push(c);
                    state = State::InWValue;
                }
                i += 1;
            }
            State::InSqValue => {
                if c == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        // Copy only one of the two quotes.
                        workspace.push(c);
                        i += 2;
                        continue;
                    }
                    let value_end = workspace.len();
                    workspace.push(0);
                    result.push(finish_item(
                        &workspace,
                        key_start,
                        key_end,
                        value_start,
                        value_end,
                        true,
                    )?);
                    state = State::WaitKey;
                    i += 1;
                } else {
                    // A doubled backslash collapses to one.
                    if c == b'\\' && i + 1 < bytes.len() && bytes[i + 1] == b'\\' {
                        i += 1;
                    }
                    workspace.push(c);
                    i += 1;
                }
            }
            State::InDqValue => {
                if c == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        // Copy only one of the two quotes.
                        workspace.push(c);
                        i += 2;
                        continue;
                    }
                    let value_end = workspace.len();
                    workspace.push(0);
                    result.push(finish_item(
                        &workspace,
                        key_start,
                        key_end,
                        value_start,
                        value_end,
                        true,
                    )?);
                    state = State::WaitKey;
                    i += 1;
                } else {
                    workspace.push(c);
                    i += 1;
                }
            }
            State::InWValue => {
                if c == b',' || is_space(c) {
                    let value_end = workspace.len();
                    workspace.push(0);
                    result.push(finish_item(
                        &workspace,
                        key_start,
                        key_end,
                        value_start,
                        value_end,
                        false,
                    )?);
                    state = State::WaitKey;
                } else {
                    workspace.push(c);
                }
                i += 1;
            }
        }
    }

    if state == State::InWValue {
        let value_end = workspace.len();
        workspace.push(0);
        result.push(finish_item(
            &workspace,
            key_start,
            key_end,
            value_start,
            value_end,
            false,
        )?);
    } else if state != State::WaitKey {
        return Err(db_error(
            &format!("invalid parameter list format: \"{text}\""),
            "42601",
        ));
    }

    Ok(result)
}

/// The `(name, defGetString(value))` pair of one parsed deflist item.
fn finish_item(
    workspace: &[u8],
    key_start: usize,
    key_end: usize,
    value_start: usize,
    value_end: usize,
    was_quoted: bool,
) -> Result<(String, String), String> {
    let name = String::from_utf8_lossy(&workspace[key_start..key_end]).into_owned();
    let value = String::from_utf8_lossy(&workspace[value_start..value_end]).into_owned();
    Ok((name, def_value_text(&value, was_quoted)?))
}

/// `prsd_headline`'s option extraction, with its validation.
fn parse_options(text: Option<&str>) -> Result<Options, String> {
    let mut options = Options::default();
    if let Some(text) = text {
        for (name, value) in deserialize_deflist(text)? {
            if name.eq_ignore_ascii_case("MaxWords") {
                options.max_words = str_to_int32(&value)?;
            } else if name.eq_ignore_ascii_case("MinWords") {
                options.min_words = str_to_int32(&value)?;
            } else if name.eq_ignore_ascii_case("ShortWord") {
                options.shortword = str_to_int32(&value)?;
            } else if name.eq_ignore_ascii_case("MaxFragments") {
                options.max_fragments = str_to_int32(&value)?;
            } else if name.eq_ignore_ascii_case("StartSel") {
                options.start_sel = value;
            } else if name.eq_ignore_ascii_case("StopSel") {
                options.stop_sel = value;
            } else if name.eq_ignore_ascii_case("FragmentDelimiter") {
                options.frag_delim = value;
            } else if name.eq_ignore_ascii_case("HighlightAll") {
                options.highlight_all = ["1", "on", "true", "t", "y", "yes"]
                    .iter()
                    .any(|v| value.eq_ignore_ascii_case(v));
            } else {
                return Err(db_error(
                    &format!("unrecognized headline parameter: \"{name}\""),
                    "22023",
                ));
            }
        }
    }

    // In HighlightAll mode these parameters are ignored.
    if !options.highlight_all {
        if options.min_words >= options.max_words {
            return Err(db_error("MinWords must be less than MaxWords", "22023"));
        }
        if options.min_words <= 0 {
            return Err(db_error("MinWords must be positive", "22023"));
        }
        if options.shortword < 0 {
            return Err(db_error("ShortWord must be >= 0", "22023"));
        }
        if options.max_fragments < 0 {
            return Err(db_error("MaxFragments must be >= 0", "22023"));
        }
    }
    if options.start_sel.len() > i16::MAX as usize {
        return Err(db_error("value for \"StartSel\" is too long", "22023"));
    }
    if options.stop_sel.len() > i16::MAX as usize {
        return Err(db_error("value for \"StopSel\" is too long", "22023"));
    }
    if options.frag_delim.len() > i16::MAX as usize {
        return Err(db_error(
            "value for \"FragmentDelimiter\" is too long",
            "22023",
        ));
    }
    Ok(options)
}

// ----------------------------------------------------------------- entry

/// `ts_headline`: the document's most relevant fragment, with the query's
/// matches highlighted.
pub(crate) fn ts_headline(
    config: crate::ts_dict::Config,
    document: &str,
    query: &QNode,
    options_text: Option<&str>,
) -> Result<String, String> {
    let operands = query_operands(query);
    let mut parsed = parse_document(config, document, &operands);
    let options = parse_options(options_text)?;

    // Locate the words and phrases matching the query.
    let locations = if matches!(query, QNode::Empty) {
        Vec::new() // an empty query matches nothing
    } else {
        let nodes = nodes_of(query);
        let source = HlMatches {
            words: &parsed.words,
            nodes: &nodes,
        };
        execute_locations(&source, query).unwrap_or_default()
    };

    // Apply the appropriate headline selector.
    let nodes = nodes_of(query);
    if options.max_fragments == 0 {
        mark_hl_words(
            &mut parsed,
            query,
            &nodes,
            &locations,
            options.highlight_all,
            options.shortword,
            options.min_words,
            options.max_words,
        );
    } else {
        mark_hl_fragments(
            &mut parsed,
            query,
            &nodes,
            &locations,
            options.highlight_all,
            options.shortword,
            options.min_words,
            options.max_words,
            options.max_fragments,
        );
    }

    Ok(generate_headline(&parsed, &options))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ts_dict::Config;

    fn headline(config: Config, document: &str, query: &str, options: Option<&str>) -> String {
        let query = crate::textsearch::parse_tsquery(query).expect("a query");
        ts_headline(config, document, &query, options).expect("a headline")
    }

    fn english(document: &str, query: &str, options: Option<&str>) -> String {
        headline(Config::English, document, query, options)
    }

    #[test]
    fn headlines_match_postgresql() {
        let doc = "The fat cats ate rats";
        assert_eq!(english(doc, "fat", None), "The <b>fat</b> cats ate rats");
        assert_eq!(english(doc, "cat", None), "The fat <b>cats</b> ate rats");
        assert_eq!(
            english(doc, "fat & rat", None),
            "The <b>fat</b> cats ate <b>rats</b>"
        );
        assert_eq!(
            english(doc, "fat | dog", None),
            "The <b>fat</b> cats ate rats"
        );
        assert_eq!(english(doc, "!rat", None), "The fat cats ate <b>rats</b>");
        assert_eq!(
            english(doc, "fat <-> cats", None),
            "The <b>fat</b> cats ate rats"
        );
        assert_eq!(
            english(doc, "fat <2> rats", None),
            "The <b>fat</b> cats ate rats"
        );
        assert_eq!(english(doc, "fat:*", None), "The <b>fat</b> cats ate rats");
        assert_eq!(
            english("fat cat sat mat", "fat | !cat", None),
            "<b>fat</b> <b>cat</b> sat mat"
        );
        assert_eq!(
            english(
                "fat cat sat mat",
                "fat <-> sat",
                Some("MinWords=1, MaxWords=4")
            ),
            "<b>fat</b>"
        );
        // The simple configuration does not stem.
        assert_eq!(
            headline(Config::Simple, "The Fat Cats Ate Rats", "fat", None),
            "The <b>Fat</b> Cats Ate Rats"
        );
    }

    #[test]
    fn headline_options_match_postgresql() {
        let doc = "The fat cats ate rats";
        assert_eq!(
            english(doc, "fat", Some("StartSel=[, StopSel=]")),
            "The [fat] cats ate rats"
        );
        assert_eq!(
            english(doc, "fat", Some("MaxWords=3, MinWords=1")),
            "<b>fat</b>"
        );
        assert_eq!(
            english(doc, "fat", Some("HighlightAll=1")),
            "The <b>fat</b> cats ate rats"
        );
        assert_eq!(
            english(doc, "rats", Some("ShortWord=9, MinWords=1, MaxWords=4")),
            "The"
        );
        assert_eq!(
            english(doc, "rat", Some("MaxFragments=1, MaxWords=2, MinWords=1")),
            "<b>rats</b>"
        );
        // A longer document splits into fragments the delimiter joins.
        let long = "fat cats and fat dogs and fat rats and fat cows and fat pigs and fat hens";
        assert_eq!(
            english(long, "fat", Some("MaxWords=6, MinWords=2, MaxFragments=3")),
            "<b>fat</b> cats and <b>fat</b> dogs ... <b>fat</b> rats and <b>fat</b> cows ... \
             <b>fat</b> pigs and <b>fat</b> hens"
        );
        assert_eq!(
            english(
                long,
                "fat",
                Some("MaxWords=6, MinWords=2, MaxFragments=3, FragmentDelimiter=|")
            ),
            "<b>fat</b> cats and <b>fat</b> dogs|<b>fat</b> rats and <b>fat</b> cows|\
             <b>fat</b> pigs and <b>fat</b> hens"
        );
        // A repeated word is marked once per occurrence.
        assert_eq!(
            english("the fat fat fat cat", "fat", None),
            "the <b>fat</b> <b>fat</b> <b>fat</b> cat"
        );
        assert_eq!(
            english(
                "the fat fat fat cat",
                "fat",
                Some("MaxFragments=1, MaxWords=3, MinWords=1")
            ),
            "<b>fat</b> <b>fat</b> <b>fat</b>"
        );
    }

    #[test]
    fn headline_marks_tags_and_uris_like_postgresql() {
        // A tag becomes a space outside highlight-all mode.
        assert_eq!(english("<b>fat</b> cats", "fat", None), " <b>fat</b>  cats");
        // A document with no match shows its first words untouched.
        assert_eq!(
            english("foo http://example.com/fat bar", "fat", None),
            "foo http://example.com/fat bar"
        );
        assert_eq!(
            english(
                "a b c d e f g h i j k l m n o p q r s t u v w x y z",
                "m",
                None
            ),
            "a b c d e f g h i j k l <b>m</b> n o p q r s t u v w x y z"
        );
    }

    #[test]
    fn headline_option_errors_match_postgresql() {
        let doc = "The fat cats ate rats";
        let query = crate::textsearch::parse_tsquery("fat").expect("a query");
        let err = |options: &str| {
            ts_headline(Config::English, doc, &query, Some(options)).expect_err("an error")
        };
        assert!(err("MinWords=5, MaxWords=3").contains("MinWords must be less than MaxWords"));
        assert!(err("MinWords=0, MaxWords=3").contains("MinWords must be positive"));
        assert!(err("ShortWord=-1, MinWords=1, MaxWords=3").contains("ShortWord must be >= 0"));
        assert!(
            err("MaxFragments=-1, MinWords=1, MaxWords=3").contains("MaxFragments must be >= 0")
        );
        assert!(err("Nope=3").contains("unrecognized headline parameter: \"Nope\""));
        assert!(err("MaxWords").contains("invalid parameter list format"));
        assert!(err("MaxWords=abc").contains("invalid input syntax for type integer: \"abc\""));
        // HighlightAll skips the numeric validation.
        assert_eq!(
            english(doc, "fat", Some("HighlightAll=1, MinWords=0, MaxWords=0")),
            "The <b>fat</b> cats ate rats"
        );
        // An empty options string is the defaults.
        assert_eq!(
            english(doc, "fat", Some("")),
            "The <b>fat</b> cats ate rats"
        );
    }
}
