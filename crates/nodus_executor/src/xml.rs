//! PostgreSQL's XML support: the `xml` type (OID 142), whose values are kept
//! as their canonical text like the other structured types — here the text as
//! written, because PostgreSQL stores an XML value verbatim and only checks it.
//!
//! Validation is PostgreSQL's `xml_parse`: a `content` value is parsed after
//! its XML declaration as a balanced chunk, a DOCTYPE (or the `document`
//! option) forces the document parse, and a failure is "invalid XML content"
//! (2200N) or "invalid XML document" (2200M) with the SQLSTATE PostgreSQL
//! raises. The parser is a well-formedness checker in the spirit of the one
//! libxml2 performs for PostgreSQL, down to its "ignorable whitespace"
//! heuristic under `XMLSERIALIZE(... INDENT)`; libxml2's error details are not
//! reproduced, and DTD content models are honored only for that heuristic.

use std::collections::HashMap;

use crate::error_fields::DbError;

const INVALID_DOCUMENT: &str = "2200M";
const INVALID_CONTENT: &str = "2200N";
const NOT_AN_XML_DOCUMENT: &str = "2200L";
const INVALID_XML_COMMENT: &str = "2200S";
const INVALID_XML_PI: &str = "2200T";

/// How an XML value is parsed: `document` demands a whole document, while
/// `content` accepts balanced content and lets a DOCTYPE force the document
/// parse (SQL/XML:2006's definition, as PostgreSQL implements it).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Mode {
    Document,
    Content,
}

/// Whether a declared type names the `xml` type.
pub(crate) fn is_type(data_type: &str) -> bool {
    let text = data_type.trim();
    let text = text
        .strip_prefix("pg_catalog.")
        .or_else(|| text.strip_prefix("PG_CATALOG."))
        .unwrap_or(text);
    text.eq_ignore_ascii_case("xml")
}

/// A rejected XML value: the SQLSTATE, the message, and libxml2's detail
/// where PostgreSQL's own code (the XML declaration check) produces one.
#[derive(Debug)]
pub(crate) struct XmlError {
    code: &'static str,
    message: String,
    detail: Option<String>,
}

impl XmlError {
    fn new(code: &'static str, message: impl Into<String>) -> XmlError {
        XmlError {
            code,
            message: message.into(),
            detail: None,
        }
    }

    fn detail(mut self, detail: impl Into<String>) -> XmlError {
        self.detail = Some(detail.into());
        self
    }
}

/// The error text for the executor's error channel.
pub(crate) fn error_text(error: XmlError) -> String {
    let XmlError {
        code,
        message,
        detail,
    } = error;
    let error = DbError::new(message).code(code);
    match detail {
        Some(detail) => error.detail(detail).into_text(),
        None => error.into_text(),
    }
}

fn fail(error: XmlError) -> String {
    error_text(error)
}

// ---------------------------------------------------------------------------
// The XML declaration
// ---------------------------------------------------------------------------

/// The XML declaration at the start of a value, as PostgreSQL's
/// `parse_xml_decl` reads it: the length consumed (zero when there is no
/// declaration), and the version and standalone values it named.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Decl {
    pub(crate) len: usize,
    pub(crate) version: Option<String>,
    pub(crate) standalone: i32,
}

fn is_blank(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\r')
}

/// PostgreSQL's approximation of libxml2's `xmlIsNameChar` (the legacy
/// tables): enough to tell `<?xml ...?>` from a PI like `<?xml-stylesheet`.
fn is_name_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '_' | ':' | '.' | '-') || (c as u32) >= 0x80
}

/// PostgreSQL's `parse_xml_decl`: `Err` carries the detail of the malformed
/// declaration for `errdetail_for_xml_code`.
pub(crate) fn parse_decl(text: &str) -> Result<Decl, &'static str> {
    const MISSING_VERSION: &str = "Malformed declaration: missing version.";
    const MISSING_ENCODING: &str = "Missing encoding in text declaration.";
    const STANDALONE_VALUE: &str = "standalone accepts only 'yes' or 'no'.";
    const NOT_FINISHED: &str = "Parsing XML declaration: '?>' expected.";
    const INVALID_CHAR: &str = "Invalid character value.";

    let chars: Vec<char> = text.chars().collect();
    let mut pos = 0;
    let mut version = None;
    let mut standalone = -1;
    let mut consumed = 0;
    'found: {
        if !chars.starts_with(&['<', '?', 'x', 'm', 'l']) {
            break 'found;
        }
        // A name character after `<?xml` makes it a PI, not a declaration.
        if chars.get(5).is_some_and(|c| is_name_char(*c)) {
            break 'found;
        }
        pos = 5;
        let blank = |c: char| matches!(c, ' ' | '\t' | '\n' | '\r');
        if !chars.get(pos).is_some_and(|c| blank(*c)) {
            return Err("Space required.");
        }
        while chars.get(pos).is_some_and(|c| blank(*c)) {
            pos += 1;
        }
        if !chars[pos..].starts_with(&['v', 'e', 'r', 's', 'i', 'o', 'n']) {
            return Err(MISSING_VERSION);
        }
        pos += 7;
        while chars.get(pos).is_some_and(|c| blank(*c)) {
            pos += 1;
        }
        if chars.get(pos) != Some(&'=') {
            return Err(MISSING_VERSION);
        }
        pos += 1;
        while chars.get(pos).is_some_and(|c| blank(*c)) {
            pos += 1;
        }
        let quote = match chars.get(pos) {
            Some(c @ ('\'' | '"')) => *c,
            _ => return Err(MISSING_VERSION),
        };
        let end = chars[pos + 1..]
            .iter()
            .position(|c| *c == quote)
            .ok_or(MISSING_VERSION)?
            + pos
            + 1;
        version = Some(chars[pos + 1..end].iter().collect::<String>());
        pos = end + 1;
        // The encoding, if named, must follow the version.
        let before = pos;
        while chars.get(pos).is_some_and(|c| blank(*c)) {
            pos += 1;
        }
        if chars[pos..].starts_with(&['e', 'n', 'c', 'o', 'd', 'i', 'n', 'g']) {
            if !chars.get(before).is_some_and(|c| blank(*c)) {
                return Err("Space required.");
            }
            pos += 8;
            while chars.get(pos).is_some_and(|c| blank(*c)) {
                pos += 1;
            }
            if chars.get(pos) != Some(&'=') {
                return Err(MISSING_ENCODING);
            }
            pos += 1;
            while chars.get(pos).is_some_and(|c| blank(*c)) {
                pos += 1;
            }
            let quote = match chars.get(pos) {
                Some(c @ ('\'' | '"')) => *c,
                _ => return Err(MISSING_ENCODING),
            };
            pos = chars[pos + 1..]
                .iter()
                .position(|c| *c == quote)
                .ok_or(MISSING_ENCODING)?
                + pos
                + 2;
        } else {
            pos = before;
        }
        // So must the standalone flag.
        let before = pos;
        while chars.get(pos).is_some_and(|c| blank(*c)) {
            pos += 1;
        }
        if chars[pos..].starts_with(&['s', 't', 'a', 'n', 'd', 'a', 'l', 'o', 'n', 'e']) {
            if !chars.get(before).is_some_and(|c| blank(*c)) {
                return Err("Space required.");
            }
            pos += 10;
            while chars.get(pos).is_some_and(|c| blank(*c)) {
                pos += 1;
            }
            if chars.get(pos) != Some(&'=') {
                return Err(STANDALONE_VALUE);
            }
            pos += 1;
            while chars.get(pos).is_some_and(|c| blank(*c)) {
                pos += 1;
            }
            let rest: String = chars[pos..].iter().take(5).collect();
            if rest.starts_with("'yes'") || rest.starts_with("\"yes\"") {
                standalone = 1;
                pos += 5;
            } else if rest.starts_with("'no'") || rest.starts_with("\"no\"") {
                standalone = 0;
                pos += 4;
            } else {
                return Err(STANDALONE_VALUE);
            }
        } else {
            pos = before;
        }
        while chars.get(pos).is_some_and(|c| blank(*c)) {
            pos += 1;
        }
        if !chars[pos..].starts_with(&['?', '>']) {
            return Err(NOT_FINISHED);
        }
        pos += 2;
        consumed = pos;
    }
    // A declaration is ASCII; anything else is invalid, as PostgreSQL decides.
    if chars[..consumed].iter().any(|c| (*c as u32) > 127) {
        return Err(INVALID_CHAR);
    }
    Ok(Decl {
        len: consumed,
        version,
        standalone,
    })
}

/// Whether content (after any declaration) holds a DOCTYPE, which forces the
/// document parse — PostgreSQL's `xml_doctype_in_content`.
fn doctype_in_content(text: &str) -> bool {
    let chars: Vec<char> = text.chars().collect();
    let mut pos = 0;
    loop {
        while chars.get(pos).is_some_and(|c| is_blank(*c)) {
            pos += 1;
        }
        if chars.get(pos) != Some(&'<') {
            return false;
        }
        pos += 1;
        if chars.get(pos) == Some(&'!') {
            pos += 1;
            if chars[pos..].starts_with(&['D', 'O', 'C', 'T', 'Y', 'P', 'E']) {
                return true;
            }
            if !chars[pos..].starts_with(&['-', '-']) {
                return false;
            }
            let mut at = pos + 2;
            loop {
                if at + 1 >= chars.len() {
                    return false;
                }
                if chars[at] == '-' && chars[at + 1] == '-' {
                    break;
                }
                at += 1;
            }
            if chars.get(at + 2) != Some(&'>') {
                return false;
            }
            pos = at + 3;
            continue;
        }
        if chars.get(pos) != Some(&'?') {
            return false;
        }
        pos += 1;
        let end =
            (pos..chars.len().saturating_sub(1)).find(|i| chars[*i] == '?' && chars[i + 1] == '>');
        match end {
            Some(end) => pos = end + 2,
            None => return false,
        }
    }
}

/// PostgreSQL's `print_xml_decl` without an encoding: a declaration is written
/// only when the version is not the default or a standalone value is named.
fn print_decl(out: &mut String, version: Option<&str>, standalone: i32) -> bool {
    if !version.is_some_and(|v| v != "1.0") && standalone == -1 {
        return false;
    }
    out.push_str("<?xml");
    out.push_str(&format!(" version=\"{}\"", version.unwrap_or("1.0")));
    match standalone {
        1 => out.push_str(" standalone=\"yes\""),
        0 => out.push_str(" standalone=\"no\""),
        _ => {}
    }
    out.push_str("?>");
    true
}

// ---------------------------------------------------------------------------
// The parsed value
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) enum Node {
    Element(Element),
    Text(String),
    Cdata(String),
    Comment(String),
    Pi {
        name: String,
        content: Option<String>,
    },
    /// A DOCTYPE, kept in the spelling PostgreSQL's XML output would use.
    Doctype(String),
}

#[derive(Clone, Debug)]
pub(crate) struct Element {
    pub(crate) name: String,
    pub(crate) attrs: Vec<(String, String)>,
    pub(crate) children: Vec<Node>,
}

/// A parsed XML value: the nodes, and what the parse decided about the input.
#[derive(Clone, Debug)]
pub(crate) struct Document {
    pub(crate) children: Vec<Node>,
    /// Whether the document parse ran (a DOCTYPE in content forces it).
    parsed_as_document: bool,
    /// Whether the input began with an XML declaration.
    had_decl: bool,
    version: Option<String>,
    standalone: i32,
}

impl Document {
    fn new() -> Document {
        Document {
            children: Vec::new(),
            parsed_as_document: false,
            had_decl: false,
            version: None,
            standalone: -1,
        }
    }
}

/// Whether the document parse would accept this value (libxml2's well-formed
/// document), which `IS DOCUMENT` and `xml_is_well_formed_document` ask.
pub(crate) fn is_document(value: &str) -> bool {
    parse(value, Mode::Document, true).is_ok()
}

/// Whether the value is well-formed under an option, as
/// `xml_is_well_formed[_document|_content]`.
pub(crate) fn is_well_formed(value: &str, mode: Mode) -> bool {
    parse(value, mode, true).is_ok()
}

/// PostgreSQL's `xml_parse`: the value is checked and, for the INDENT output,
/// returned as a tree. `preserve_whitespace` is false only under
/// `XMLSERIALIZE(... INDENT)`, where libxml2's parser drops the whitespace its
/// heuristic finds ignorable.
pub(crate) fn parse(
    value: &str,
    mode: Mode,
    preserve_whitespace: bool,
) -> Result<Document, XmlError> {
    match mode {
        Mode::Document => {
            let mut parser = Parser::new(value, preserve_whitespace);
            let mut doc = parser
                .parse_document()
                .map_err(|_| XmlError::new(INVALID_DOCUMENT, "invalid XML document"))?;
            doc.parsed_as_document = true;
            Ok(doc)
        }
        Mode::Content => {
            let decl = match parse_decl(value) {
                Ok(decl) => decl,
                Err(detail) => {
                    return Err(XmlError::new(
                        INVALID_CONTENT,
                        "invalid XML content: invalid XML declaration",
                    )
                    .detail(detail));
                }
            };
            if doctype_in_content(&value[decl.len..]) {
                let mut parser = Parser::new(value, preserve_whitespace);
                let mut doc = parser
                    .parse_document()
                    .map_err(|_| XmlError::new(INVALID_CONTENT, "invalid XML content"))?;
                doc.parsed_as_document = true;
                Ok(doc)
            } else {
                let mut parser = Parser::new(value, preserve_whitespace);
                parser.pos = decl.len;
                let mut doc = parser
                    .parse_content()
                    .map_err(|_| XmlError::new(INVALID_CONTENT, "invalid XML content"))?;
                doc.had_decl = decl.len > 0;
                doc.version = decl.version;
                doc.standalone = decl.standalone;
                Ok(doc)
            }
        }
    }
}

/// The check alone, for the cast to `xml` and `xmlparse`.
pub(crate) fn validate(value: &str, mode: Mode) -> Result<(), XmlError> {
    parse(value, mode, true).map(|_| ())
}

/// The error text for a failed check, for the executor's error channel.
pub(crate) fn validate_text(value: &str, mode: Mode) -> Result<(), String> {
    validate(value, mode).map_err(fail)
}

// ---------------------------------------------------------------------------
// The parser
// ---------------------------------------------------------------------------

fn is_xml_char(c: char) -> bool {
    matches!(c, '\t' | '\n' | '\r')
        || (' '..='\u{D7FF}').contains(&c)
        || ('\u{E000}'..='\u{FFFD}').contains(&c)
        || ('\u{10000}'..='\u{10FFFF}').contains(&c)
}

/// A failure of the well-formedness check; the message is the caller's.
#[derive(Debug)]
struct Invalid;

struct Parser {
    chars: Vec<char>,
    pos: usize,
    /// The internal subset's entity values, by name.
    entities: HashMap<String, String>,
    /// Entity expansions underway: the name and the buffer position its
    /// replacement text ends at, to catch reference loops.
    active: Vec<(String, usize)>,
    /// `<!ELEMENT name ...>` declarations: whether the content model makes
    /// whitespace inside the element ignorable, as libxml2's `areBlanks`.
    models: HashMap<String, bool>,
    preserve_whitespace: bool,
    /// The size the value may grow to through entity expansion.
    limit: usize,
}

type R = Result<(), Invalid>;

impl Parser {
    fn new(value: &str, preserve_whitespace: bool) -> Parser {
        let chars: Vec<char> = value.chars().collect();
        let limit = chars.len().saturating_mul(16).saturating_add(1 << 20);
        Parser {
            chars,
            pos: 0,
            entities: HashMap::new(),
            active: Vec::new(),
            models: HashMap::new(),
            preserve_whitespace,
            limit,
        }
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn peek_at(&self, offset: usize) -> Option<char> {
        self.chars.get(self.pos + offset).copied()
    }

    fn at_end(&self) -> bool {
        self.pos >= self.chars.len()
    }

    fn starts_with(&self, text: &str) -> bool {
        let mut i = self.pos;
        for c in text.chars() {
            if self.chars.get(i) != Some(&c) {
                return false;
            }
            i += 1;
        }
        true
    }

    /// Advances, retiring the entity expansions the position has left.
    fn bump(&mut self) {
        self.pos += 1;
        while let Some((_, end)) = self.active.last() {
            if self.pos >= *end {
                self.active.pop();
            } else {
                break;
            }
        }
    }

    fn bump_by(&mut self, n: usize) {
        for _ in 0..n {
            self.bump();
        }
    }

    fn skip_ws(&mut self) {
        while self.peek().is_some_and(is_blank) {
            self.bump();
        }
    }

    fn expect(&mut self, c: char) -> R {
        if self.peek() == Some(c) {
            self.bump();
            Ok(())
        } else {
            Err(Invalid)
        }
    }

    fn is_name_start(&self) -> bool {
        self.peek()
            .is_some_and(|c| c == ':' || c == '_' || c.is_ascii_alphabetic() || (c as u32) >= 0x80)
    }

    fn parse_name(&mut self) -> Result<String, Invalid> {
        if !self.is_name_start() {
            return Err(Invalid);
        }
        let start = self.pos;
        self.bump();
        while self.peek().is_some_and(is_name_char) {
            self.bump();
        }
        Ok(self.chars[start..self.pos].iter().collect())
    }

    /// A character reference or an entity reference, appended to `out`. A
    /// declared entity's replacement text is spliced into the input so that
    /// content sees the markup it holds, as libxml2's substitution does.
    fn reference(&mut self, out: &mut String) -> R {
        self.bump(); // '&'
        if self.peek() == Some('#') {
            self.bump();
            let hex = self.peek() == Some('x');
            if hex {
                self.bump();
            }
            let mut value: u32 = 0;
            let mut digits = 0;
            while let Some(c) = self.peek() {
                let digit = match (hex, c) {
                    (true, c) if c.is_ascii_hexdigit() => c.to_digit(16).unwrap(),
                    (false, c) if c.is_ascii_digit() => c.to_digit(10).unwrap(),
                    _ => break,
                };
                value = value.saturating_mul(if hex { 16 } else { 10 });
                value += digit;
                digits += 1;
                self.bump();
            }
            if digits == 0 || self.peek() != Some(';') {
                return Err(Invalid);
            }
            self.bump();
            let c = char::from_u32(value).ok_or(Invalid)?;
            if !is_xml_char(c) {
                return Err(Invalid);
            }
            out.push(c);
            return Ok(());
        }
        let name = self.parse_name()?;
        if self.peek() != Some(';') {
            return Err(Invalid);
        }
        self.bump();
        let c = match name.as_str() {
            "lt" => Some('<'),
            "gt" => Some('>'),
            "amp" => Some('&'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            _ => None,
        };
        if let Some(c) = c {
            out.push(c);
            return Ok(());
        }
        if self.active.iter().any(|(n, _)| n == &name) {
            return Err(Invalid);
        }
        let value = self.entities.get(&name).ok_or(Invalid)?.clone();
        let inserted: Vec<char> = value.chars().collect();
        if self.active.len() >= 40 || self.chars.len() + inserted.len() > self.limit {
            return Err(Invalid);
        }
        let end = self.pos + inserted.len();
        self.chars.splice(self.pos..self.pos, inserted);
        self.active.push((name, end));
        Ok(())
    }

    /// A run of character data, ending at markup or the end of input, with
    /// its references expanded. Whitespace-only runs are dropped where
    /// libxml2's heuristic finds them ignorable, under INDENT. Returns
    /// whether the run held nothing but whitespace.
    fn text_run(
        &mut self,
        children: &mut Vec<Node>,
        preserve: bool,
        model: Option<bool>,
    ) -> Result<bool, Invalid> {
        let mut text = String::new();
        while let Some(c) = self.peek() {
            match c {
                '<' => break,
                '&' => self.reference(&mut text)?,
                ']' if self.peek_at(1) == Some(']') && self.peek_at(2) == Some('>') => {
                    return Err(Invalid);
                }
                '\r' => {
                    self.bump();
                    if self.peek() == Some('\n') {
                        self.bump();
                    }
                    text.push('\n');
                }
                '>' => {
                    self.bump();
                    text.push('>');
                }
                c => {
                    if !is_xml_char(c) {
                        return Err(Invalid);
                    }
                    self.bump();
                    text.push(c);
                }
            }
        }
        if text.is_empty() {
            return Ok(true);
        }
        let blank = text.chars().all(is_blank);
        if blank && self.blanks_ignorable(children, preserve, model) {
            return Ok(true);
        }
        match children.last_mut() {
            Some(Node::Text(last)) => last.push_str(&text),
            _ => children.push(Node::Text(text)),
        }
        Ok(blank)
    }

    /// libxml2's `areBlanks`: whether a whitespace-only run is ignorable. The
    /// run must be followed by markup, must not be the only content before an
    /// end tag, and must not sit next to a text node; a `<!ELEMENT ...>`
    /// declaration overrides the heuristic.
    fn blanks_ignorable(&self, children: &[Node], preserve: bool, model: Option<bool>) -> bool {
        if preserve {
            return false;
        }
        if let Some(ignorable) = model {
            return ignorable;
        }
        if self.peek() != Some('<') {
            // At the end of the input the run is kept, as libxml2 keeps it.
            return false;
        }
        if children.is_empty() && self.starts_with("</") {
            return false;
        }
        if matches!(children.last(), Some(Node::Text(_))) {
            return false;
        }
        if matches!(children.first(), Some(Node::Text(_))) {
            return false;
        }
        true
    }

    fn parse_comment(&mut self, children: &mut Vec<Node>) -> R {
        self.bump_by(4); // "<!--"
        let mut text = String::new();
        loop {
            match self.peek() {
                None => return Err(Invalid),
                Some('-') if self.peek_at(1) == Some('-') => {
                    if self.peek_at(2) != Some('>') {
                        return Err(Invalid);
                    }
                    self.bump_by(3);
                    children.push(Node::Comment(text));
                    return Ok(());
                }
                Some('-') => return Err(Invalid), // "--" may not end the comment
                Some('\r') => {
                    self.bump();
                    if self.peek() == Some('\n') {
                        self.bump();
                    }
                    text.push('\n');
                }
                Some(c) => {
                    if !is_xml_char(c) {
                        return Err(Invalid);
                    }
                    self.bump();
                    text.push(c);
                }
            }
        }
    }

    fn parse_cdata(&mut self, children: &mut Vec<Node>) -> R {
        self.bump_by(9); // "<![CDATA["
        let mut text = String::new();
        loop {
            if self.at_end() {
                return Err(Invalid);
            }
            if self.starts_with("]]>") {
                self.bump_by(3);
                children.push(Node::Cdata(text));
                return Ok(());
            }
            let c = self.peek().unwrap();
            if c == '\r' {
                self.bump();
                if self.peek() == Some('\n') {
                    self.bump();
                }
                text.push('\n');
                continue;
            }
            if !is_xml_char(c) {
                return Err(Invalid);
            }
            self.bump();
            text.push(c);
        }
    }

    fn parse_pi(&mut self, children: &mut Vec<Node>) -> R {
        self.bump_by(2); // "<?"
        let name = self.parse_name()?;
        if name.eq_ignore_ascii_case("xml") {
            return Err(Invalid);
        }
        let mut content = None;
        if !self.starts_with("?>") {
            self.skip_ws();
            let mut text = String::new();
            loop {
                if self.at_end() {
                    return Err(Invalid);
                }
                if self.starts_with("?>") {
                    break;
                }
                let c = self.peek().unwrap();
                if !is_xml_char(c) {
                    return Err(Invalid);
                }
                self.bump();
                text.push(c);
            }
            content = Some(text);
        }
        self.bump_by(2); // "?>"
        children.push(Node::Pi { name, content });
        Ok(())
    }

    /// An element, its start tag, content, and end tag.
    fn parse_element(&mut self, inherited_preserve: bool) -> Result<Node, Invalid> {
        self.expect('<')?;
        let name = self.parse_name()?;
        let mut attrs: Vec<(String, String)> = Vec::new();
        // Whitespace separates the name and each attribute.
        let mut spaced = false;
        loop {
            let before = self.pos;
            self.skip_ws();
            spaced |= self.pos != before;
            match self.peek() {
                Some('/') if self.peek_at(1) == Some('>') => {
                    self.bump_by(2);
                    return Ok(Node::Element(Element {
                        name,
                        attrs,
                        children: Vec::new(),
                    }));
                }
                Some('>') => {
                    self.bump();
                    break;
                }
                Some(_) if !spaced => return Err(Invalid),
                Some(c)
                    if c == ':' || c == '_' || c.is_ascii_alphabetic() || (c as u32) >= 0x80 =>
                {
                    let attr = self.parse_name()?;
                    if attrs.iter().any(|(n, _)| n == &attr) {
                        return Err(Invalid);
                    }
                    self.skip_ws();
                    self.expect('=')?;
                    self.skip_ws();
                    let value = self.parse_attribute_value()?;
                    attrs.push((attr, value));
                    spaced = false;
                }
                _ => return Err(Invalid),
            }
        }
        let preserve = self.preserve_ws(&attrs, inherited_preserve);
        let model = self.models.get(&name).copied();
        let mut children = Vec::new();
        loop {
            if self.at_end() {
                return Err(Invalid);
            }
            if self.starts_with("</") {
                self.bump_by(2);
                let close = self.parse_name()?;
                self.skip_ws();
                self.expect('>')?;
                if close != name {
                    return Err(Invalid);
                }
                break;
            }
            if self.starts_with("<!--") {
                self.parse_comment(&mut children)?;
            } else if self.starts_with("<![CDATA[") {
                self.parse_cdata(&mut children)?;
            } else if self.starts_with("<?") {
                self.parse_pi(&mut children)?;
            } else if self.starts_with("<!") {
                return Err(Invalid);
            } else if self.peek() == Some('<') {
                let child = self.parse_element(preserve)?;
                children.push(child);
            } else {
                self.text_run(&mut children, preserve, model)?;
            }
        }
        Ok(Node::Element(Element {
            name,
            attrs,
            children,
        }))
    }

    /// Whether `xml:space` on this element preserves whitespace, inherited
    /// from the enclosing elements unless the attribute says otherwise.
    fn preserve_ws(&self, attrs: &[(String, String)], inherited: bool) -> bool {
        match attrs.iter().find(|(n, _)| n == "xml:space") {
            Some((_, v)) if v == "preserve" => true,
            Some((_, v)) if v == "default" => false,
            _ => inherited,
        }
    }

    fn parse_attribute_value(&mut self) -> Result<String, Invalid> {
        let quote = match self.peek() {
            Some(c @ ('\'' | '"')) => c,
            _ => return Err(Invalid),
        };
        self.bump();
        let mut value = String::new();
        loop {
            match self.peek() {
                None => return Err(Invalid),
                Some(c) if c == quote => {
                    self.bump();
                    return Ok(value);
                }
                Some('<') => return Err(Invalid),
                Some('&') => self.reference(&mut value)?,
                // Literal whitespace is normalized to a space; whitespace in
                // an entity's replacement text is not.
                Some('\t' | '\n' | '\r') if !self.in_replacement() => {
                    let c = self.peek().unwrap();
                    self.bump();
                    if c == '\r' && self.peek() == Some('\n') {
                        self.bump();
                    }
                    value.push(' ');
                }
                Some(c) => {
                    if !is_xml_char(c) {
                        return Err(Invalid);
                    }
                    self.bump();
                    value.push(c);
                }
            }
        }
    }

    /// Whether the position is inside an entity's replacement text, whose
    /// characters are appended as they are rather than normalized.
    fn in_replacement(&self) -> bool {
        !self.active.is_empty()
    }

    /// A whole document: an optional declaration, miscellany, an optional
    /// DOCTYPE, one root element, and miscellany to the end.
    fn parse_document(&mut self) -> Result<Document, Invalid> {
        let mut doc = Document::new();
        if self.starts_with("<?xml") {
            let rest: String = self.chars[self.pos..].iter().collect();
            match parse_decl(&rest) {
                Ok(decl) if decl.len > 0 => {
                    // libxml2 knows versions 1.0 and 1.1.
                    if !matches!(decl.version.as_deref(), Some("1.0") | Some("1.1")) {
                        return Err(Invalid);
                    }
                    doc.had_decl = true;
                    doc.version = decl.version.clone();
                    doc.standalone = decl.standalone;
                    self.bump_by(decl.len);
                }
                Ok(_) => {}
                Err(_) => return Err(Invalid),
            }
        }
        let mut root = false;
        let mut doctype = false;
        loop {
            if self.at_end() {
                if !root {
                    return Err(Invalid);
                }
                return Ok(doc);
            }
            if self.starts_with("<!--") {
                self.parse_comment(&mut doc.children)?;
            } else if self.starts_with("<?") {
                self.parse_pi(&mut doc.children)?;
            } else if self.starts_with("<!DOCTYPE") {
                if root || doctype {
                    return Err(Invalid);
                }
                doctype = true;
                let text = self.parse_doctype()?;
                doc.children.push(Node::Doctype(text));
            } else if self.starts_with("<!") {
                return Err(Invalid);
            } else if self.peek() == Some('<') {
                if root {
                    return Err(Invalid);
                }
                let element = self.parse_element(false)?;
                doc.children.push(element);
                root = true;
            } else {
                // Whitespace between the markup may remain a text node; any
                // other character data is "extra content".
                if !self.text_run(&mut doc.children, false, None)? {
                    return Err(Invalid);
                }
            }
        }
    }

    /// Balanced content: elements, text, comments, PIs, and CDATA, with no
    /// DOCTYPE or XML declaration of its own.
    fn parse_content(&mut self) -> Result<Document, Invalid> {
        let mut doc = Document::new();
        loop {
            if self.at_end() {
                return Ok(doc);
            }
            if self.starts_with("</") {
                return Err(Invalid);
            }
            if self.starts_with("<!--") {
                self.parse_comment(&mut doc.children)?;
            } else if self.starts_with("<![CDATA[") {
                self.parse_cdata(&mut doc.children)?;
            } else if self.starts_with("<?") {
                self.parse_pi(&mut doc.children)?;
            } else if self.starts_with("<!") {
                return Err(Invalid);
            } else if self.peek() == Some('<') {
                let element = self.parse_element(false)?;
                doc.children.push(element);
            } else {
                self.text_run(&mut doc.children, false, None)?;
            }
        }
    }

    /// A DOCTYPE declaration: the external identifiers, and the internal
    /// subset, whose entity declarations the parser needs and whose
    /// serialized form `XMLSERIALIZE` would print.
    fn parse_doctype(&mut self) -> Result<String, Invalid> {
        self.bump_by(9); // "<!DOCTYPE"
        self.skip_ws();
        let name = self.parse_name()?;
        let mut out = format!("<!DOCTYPE {name}");
        self.skip_ws();
        if self.starts_with("SYSTEM") || self.starts_with("PUBLIC") {
            let public = self.starts_with("PUBLIC");
            self.bump_by(6); // "SYSTEM" or "PUBLIC"
            self.skip_ws();
            let first = self.parse_quoted()?;
            out.push_str(&format!(
                " {} {first}",
                if public { "PUBLIC" } else { "SYSTEM" }
            ));
            if public {
                self.skip_ws();
                let system = self.parse_quoted()?;
                out.push_str(&format!(" {system}"));
            }
        }
        self.skip_ws();
        if self.peek() == Some('[') {
            self.bump();
            let mut declarations = Vec::new();
            loop {
                self.skip_ws();
                match self.peek() {
                    None => return Err(Invalid),
                    Some(']') => {
                        self.bump();
                        break;
                    }
                    Some('%') => {
                        self.bump();
                        self.parse_name()?;
                        if self.peek() == Some(';') {
                            self.bump();
                        }
                    }
                    Some('<') if self.starts_with("<!--") => {
                        let mut ignored = Vec::new();
                        self.parse_comment(&mut ignored)?;
                    }
                    Some('<') if self.starts_with("<?") => {
                        let mut ignored = Vec::new();
                        self.parse_pi(&mut ignored)?;
                    }
                    Some('<') if self.starts_with("<!ENTITY") => {
                        declarations.push(self.parse_entity_decl()?);
                    }
                    Some('<') if self.starts_with("<!ELEMENT") => {
                        declarations.push(self.parse_element_decl()?);
                    }
                    Some('<') if self.starts_with("<!") => {
                        declarations.push(self.parse_other_decl()?);
                    }
                    _ => return Err(Invalid),
                }
            }
            self.skip_ws();
            if declarations.is_empty() {
                out.push_str(" []");
            } else {
                out.push_str(" [\n");
                out.push_str(&declarations.join("\n"));
                out.push_str("\n]");
            }
        }
        self.skip_ws();
        self.expect('>')?;
        out.push('>');
        Ok(out)
    }

    /// A quoted literal, written back in double quotes as libxml2 would.
    fn parse_quoted(&mut self) -> Result<String, Invalid> {
        let quote = match self.peek() {
            Some(c @ ('\'' | '"')) => c,
            _ => return Err(Invalid),
        };
        self.bump();
        let start = self.pos;
        while !self.at_end() && self.peek() != Some(quote) {
            self.bump();
        }
        if self.at_end() {
            return Err(Invalid);
        }
        let text: String = self.chars[start..self.pos].iter().collect();
        self.bump();
        Ok(format!("\"{text}\""))
    }

    /// `<!ENTITY name "value">`: an internal entity is recorded (its
    /// references left for expansion when it is used), a parameter or
    /// external one is only skipped.
    fn parse_entity_decl(&mut self) -> Result<String, Invalid> {
        self.bump_by(8); // "<!ENTITY"
        self.skip_ws();
        let parameter = self.peek() == Some('%');
        if parameter {
            self.bump();
            self.skip_ws();
        }
        let name = self.parse_name()?;
        self.skip_ws();
        if self.peek().is_some_and(|c| c == '"' || c == '\'') {
            let quote = self.peek().unwrap();
            self.bump();
            let start = self.pos;
            while !self.at_end() && self.peek() != Some(quote) {
                self.bump();
            }
            if self.at_end() {
                return Err(Invalid);
            }
            let value: String = self.chars[start..self.pos].iter().collect();
            self.bump();
            self.skip_ws();
            self.expect('>')?;
            if !parameter {
                self.entities.insert(name.clone(), value.clone());
            }
            return Ok(format!("<!ENTITY {name} \"{value}\">"));
        }
        // An external entity; its identifiers are kept, the entity is not.
        if self.starts_with("SYSTEM") || self.starts_with("PUBLIC") {
            let public = self.starts_with("PUBLIC");
            self.bump_by(6); // "SYSTEM" or "PUBLIC"
            self.skip_ws();
            let first = self.parse_quoted()?;
            let mut out = format!(
                "<!ENTITY {name} {} {first}",
                if public { "PUBLIC" } else { "SYSTEM" }
            );
            if public {
                self.skip_ws();
                out.push_str(&format!(" {}", self.parse_quoted()?));
            }
            self.skip_ws();
            self.expect('>')?;
            return Ok(format!("{out}>"));
        }
        Err(Invalid)
    }

    /// `<!ELEMENT name model>`: the model is remembered only to decide
    /// whether whitespace inside the element is ignorable. A children model
    /// (a parenthesized list, as opposed to `ANY` or `EMPTY` or mixed) makes
    /// it so, as libxml2's `areBlanks` decides.
    fn parse_element_decl(&mut self) -> Result<String, Invalid> {
        self.bump_by(9); // "<!ELEMENT"
        self.skip_ws();
        let name = self.parse_name()?;
        self.skip_ws();
        let start = self.pos;
        if self.starts_with("ANY") || self.starts_with("EMPTY") {
            self.models.insert(name.clone(), false);
        } else if self.starts_with("(#PCDATA") {
            self.models.insert(name.clone(), false);
        } else if self.peek() == Some('(') {
            self.models.insert(name.clone(), true);
        } else {
            return Err(Invalid);
        }
        self.skip_declaration_body()?;
        let model: String = self.chars[start..self.pos - 1]
            .iter()
            .collect::<String>()
            .trim()
            .to_string();
        Ok(format!("<!ELEMENT {name} {model}>"))
    }

    /// Any other declaration, kept verbatim.
    fn parse_other_decl(&mut self) -> Result<String, Invalid> {
        let start = self.pos;
        self.skip_declaration_body()?;
        Ok(self.chars[start..self.pos].iter().collect())
    }

    /// Skips to the `>` that closes a declaration, respecting quoted strings.
    fn skip_declaration_body(&mut self) -> R {
        let mut quote = None;
        loop {
            match self.peek() {
                None => return Err(Invalid),
                Some(c) if quote.is_some() => {
                    self.bump();
                    if Some(c) == quote {
                        quote = None;
                    }
                }
                Some(c @ ('\'' | '"')) => {
                    quote = Some(c);
                    self.bump();
                }
                Some('>') => {
                    self.bump();
                    return Ok(());
                }
                Some(_) => self.bump(),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The functions on values
// ---------------------------------------------------------------------------

/// `xmlconcat`: the arguments' XML declarations are merged into one, and the
/// remaining text is concatenated — PostgreSQL's `xmlconcat`.
pub(crate) fn concat(parts: &[&str]) -> String {
    let mut global_standalone = 1i32;
    let mut version: Option<String> = None;
    let mut no_version = false;
    let mut out = String::new();
    for part in parts {
        let decl = match parse_decl(part) {
            Ok(decl) => decl,
            // An unreadable declaration is skipped whole, as in PostgreSQL.
            Err(_) => Decl {
                len: part.len(),
                version: None,
                standalone: -1,
            },
        };
        if decl.standalone == 0 && global_standalone == 1 {
            global_standalone = 0;
        }
        if decl.standalone < 0 {
            global_standalone = -1;
        }
        match &decl.version {
            None => no_version = true,
            Some(v) => match &version {
                None => version = Some(v.clone()),
                Some(global) if global != v => no_version = true,
                _ => {}
            },
        }
        out.push_str(&part[decl.len..]);
    }
    if !no_version || global_standalone >= 0 {
        let mut buf = String::new();
        let version = if no_version { None } else { version.as_deref() };
        if print_decl(&mut buf, version, global_standalone) {
            buf.push_str(&out);
            return buf;
        }
    }
    out
}

/// `xmlcomment`: the argument wrapped in `<!-- -->`, refused when it could
/// not be a comment.
pub(crate) fn comment(arg: &str) -> Result<String, XmlError> {
    let chars: Vec<char> = arg.chars().collect();
    let mut invalid = chars.windows(2).any(|w| w[0] == '-' && w[1] == '-');
    if chars.last() == Some(&'-') {
        invalid = true;
    }
    if invalid {
        return Err(XmlError::new(INVALID_XML_COMMENT, "invalid XML comment"));
    }
    Ok(format!("<!--{arg}-->"))
}

/// `xmltext`: the text with the characters libxml2's `xmlEncodeSpecialChars`
/// escapes, as XML — the result is of type `xml` in PostgreSQL.
pub(crate) fn escape_special(text: &str) -> String {
    let mut out = String::new();
    for c in text.chars() {
        match c {
            '"' => out.push_str("&quot;"),
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '\r' => out.push_str("&#13;"),
            c => out.push(c),
        }
    }
    out
}

/// `xml_out`: the value as stored, except that an XML declaration is written
/// back without its encoding — as PostgreSQL's `xml_out_internal` does.
pub(crate) fn output(text: &str) -> String {
    let Ok(decl) = parse_decl(text) else {
        // A stored value always parses; PostgreSQL only warns here.
        return text.to_string();
    };
    let mut out = String::new();
    let rest = &text[decl.len..];
    if print_decl(&mut out, decl.version.as_deref(), decl.standalone) {
        out.push_str(rest);
    } else {
        // Without a declaration, a single leading newline is eaten.
        out.push_str(rest.strip_prefix('\n').unwrap_or(rest));
    }
    out
}

/// `xmltotext_with_options`: `XMLSERIALIZE`. Without INDENT and for content,
/// the value is its own text; otherwise it is re-serialized, with libxml2's
/// formatting under INDENT.
pub(crate) fn serialize(value: &str, mode: Mode, indent: bool) -> Result<String, XmlError> {
    if mode != Mode::Document && !indent {
        return Ok(value.to_string());
    }
    let doc = parse(value, mode, !indent)
        .map_err(|_| XmlError::new(NOT_AN_XML_DOCUMENT, "not an XML document"))?;
    if !indent {
        return Ok(value.to_string());
    }
    let mut out = String::new();
    if doc.parsed_as_document {
        if doc.had_decl {
            out.push_str(&format!(
                "<?xml version=\"{}\" encoding=\"UTF-8\"",
                doc.version.as_deref().unwrap_or("1.0")
            ));
            match doc.standalone {
                1 => out.push_str(" standalone=\"yes\""),
                0 => out.push_str(" standalone=\"no\""),
                _ => {}
            }
            out.push_str("?>\n");
        }
        for child in &doc.children {
            let mut format = true;
            dump(&mut out, child, 0, &mut format, true);
            out.push('\n');
        }
        if mode == Mode::Document {
            // xmlDocContentDumpOutput adds a trailing newline; PostgreSQL
            // removes it for the DOCUMENT option.
            out.truncate(out.trim_end_matches(['\n', '\r']).len());
        }
    } else {
        // Content may have several roots; PostgreSQL inserts a newline
        // before each non-text node that follows another.
        let mut first = true;
        for child in &doc.children {
            if !first && !matches!(child, Node::Text(_)) {
                out.push('\n');
            }
            let mut format = true;
            dump(&mut out, child, 0, &mut format, true);
            first = false;
        }
    }
    Ok(out)
}

/// One node as libxml2 serializes it on its own, the form `xpath` returns a
/// result node in.
pub(crate) fn serialize_node(node: &Node) -> String {
    let mut out = String::new();
    let mut format = false;
    dump(&mut out, node, 0, &mut format, true);
    out
}

/// The whole document as libxml2 dumps it (its declaration included, and the
/// trailing newline `xmlDocDump` writes).
pub(crate) fn serialize_document(doc: &Document) -> String {
    // `xmlDocDump` always writes the declaration, with the document's
    // version (1.0 when it had none) and the encoding it saves as.
    let mut out = format!(
        "<?xml version=\"{}\" encoding=\"UTF-8\"",
        doc.version.as_deref().unwrap_or("1.0")
    );
    match doc.standalone {
        1 => out.push_str(" standalone=\"yes\""),
        0 => out.push_str(" standalone=\"no\""),
        _ => {}
    }
    out.push_str("?>\n");
    for child in &doc.children {
        let mut format = false;
        dump(&mut out, child, 0, &mut format, true);
    }
    out.push('\n');
    out
}

fn indent(out: &mut String, level: usize) {
    for _ in 0..level {
        out.push_str("  ");
    }
}

/// One node, formatted as libxml2's `xmlNodeDumpOutputInternal` formats it:
/// children are indented when the element holds no text, and an element that
/// does holds its whole subtree unformatted.
fn dump(out: &mut String, node: &Node, level: usize, format: &mut bool, is_root: bool) {
    match node {
        Node::Element(element) => {
            if !is_root && *format {
                indent(out, level);
            }
            out.push('<');
            out.push_str(&element.name);
            for (name, value) in &element.attrs {
                out.push(' ');
                out.push_str(name);
                out.push_str("=\"");
                out.push_str(&escape_attribute(value));
                out.push('"');
            }
            if element.children.is_empty() {
                out.push_str("/>");
                return;
            }
            let saved = *format;
            let unformatted = *format
                && element
                    .children
                    .iter()
                    .any(|c| matches!(c, Node::Text(_) | Node::Cdata(_)));
            if unformatted {
                *format = false;
            }
            out.push('>');
            if *format {
                out.push('\n');
            }
            for child in &element.children {
                dump(out, child, level + 1, format, false);
                if *format {
                    out.push('\n');
                }
            }
            if *format {
                indent(out, level);
            }
            out.push_str("</");
            out.push_str(&element.name);
            out.push('>');
            if unformatted {
                *format = saved;
            }
        }
        Node::Text(text) => out.push_str(&escape_text(text)),
        Node::Cdata(text) => {
            out.push_str("<![CDATA[");
            out.push_str(text);
            out.push_str("]]>");
        }
        Node::Comment(text) => {
            if !is_root && *format {
                indent(out, level);
            }
            out.push_str("<!--");
            out.push_str(text);
            out.push_str("-->");
        }
        Node::Pi { name, content } => {
            if !is_root && *format {
                indent(out, level);
            }
            out.push_str("<?");
            out.push_str(name);
            if let Some(content) = content {
                out.push(' ');
                out.push_str(content);
            }
            out.push_str("?>");
        }
        Node::Doctype(text) => out.push_str(text),
    }
}

/// `map_sql_identifier_to_xml_name`: an SQL identifier as an XML name
/// (SQL/XML:2008 9.2). With `fully_escaped`, a leading `xml` and colons are
/// escaped too, which the names taken from column references are.
pub(crate) fn identifier_to_xml_name(ident: &str, fully_escaped: bool) -> String {
    let chars: Vec<char> = ident.chars().collect();
    let mut out = String::new();
    for (i, c) in chars.iter().enumerate() {
        let first = i == 0;
        // A leading `xml` is escaped where the name is fully escaped, so an
        // identifier cannot spell a reserved name.
        let head: String = chars.iter().take(3).collect();
        let leading_xml =
            fully_escaped && first && chars.len() >= 3 && head.eq_ignore_ascii_case("xml");
        if *c == ':' && (first || fully_escaped) {
            out.push_str("_x003A_");
        } else if *c == '_' && chars.get(i + 1) == Some(&'x') {
            out.push_str("_x005F_");
        } else if leading_xml {
            out.push_str(if *c == 'x' { "_x0078_" } else { "_x0058_" });
        } else if (first && !is_valid_xml_name_start(*c)) || (!first && !is_valid_xml_name_char(*c))
        {
            out.push_str(&format!("_x{:04X}_", *c as u32));
        } else {
            out.push(*c);
        }
    }
    out
}

/// SQL/XML's name characters: the XML 1.0 `Letter | '_' | ':'` set.
fn is_valid_xml_name_start(c: char) -> bool {
    c == '_'
        || c == ':'
        || ('A'..='Z').contains(&c)
        || ('a'..='z').contains(&c)
        || ('\u{C0}'..='\u{D6}').contains(&c)
        || ('\u{D8}'..='\u{F6}').contains(&c)
        || ('\u{F8}'..='\u{2FF}').contains(&c)
        || ('\u{370}'..='\u{37D}').contains(&c)
        || ('\u{37F}'..='\u{1FFF}').contains(&c)
        || ('\u{200C}'..='\u{200D}').contains(&c)
        || ('\u{2070}'..='\u{218F}').contains(&c)
        || ('\u{2C00}'..='\u{2FEF}').contains(&c)
        || ('\u{3001}'..='\u{D7FF}').contains(&c)
        || ('\u{F900}'..='\u{FDCF}').contains(&c)
        || ('\u{FDF0}'..='\u{FFFD}').contains(&c)
        || c >= '\u{10000}'
}

/// The XML 1.0 `NameChar` set beyond [`is_valid_xml_name_start`].
fn is_valid_xml_name_char(c: char) -> bool {
    is_valid_xml_name_start(c)
        || c == '-'
        || c == '.'
        || ('0'..='9').contains(&c)
        || ('\u{300}'..='\u{345}').contains(&c)
        || ('\u{660}'..='\u{669}').contains(&c)
        || ('\u{6F0}'..='\u{6F9}').contains(&c)
        || ('\u{966}'..='\u{96F}').contains(&c)
        || ('\u{9E6}'..='\u{9EF}').contains(&c)
        || ('\u{203F}'..='\u{2040}').contains(&c)
}

/// `escape_xml`: the characters SQL/XML escapes in a text value becoming XML
/// content — the carriage return is written as a lower-case character
/// reference here, unlike in the serializer.
pub(crate) fn escape_xml(text: &str) -> String {
    let mut out = String::new();
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '\r' => out.push_str("&#x0d;"),
            c => out.push(c),
        }
    }
    out
}

/// An element's content as SQL/XML maps a value of some other type: booleans
/// as XSD's, everything else through its text.
pub(crate) fn value_text(value: &crate::Value) -> String {
    match value {
        crate::Value::Bool(true) => "true".to_string(),
        crate::Value::Bool(false) => "false".to_string(),
        other => crate::render(other),
    }
}

/// `xmlpi`: the processing instruction, with PostgreSQL's checks.
pub(crate) fn pi(target: &str, arg: Option<&str>) -> Result<String, XmlError> {
    if target.eq_ignore_ascii_case("xml") {
        return Err(
            XmlError::new(INVALID_XML_PI, "invalid XML processing instruction").detail(format!(
                "XML processing instruction target name cannot be \"{target}\"."
            )),
        );
    }
    let mut out = format!("<?{target}");
    if let Some(arg) = arg {
        if arg.contains("?>") {
            return Err(
                XmlError::new(INVALID_XML_PI, "invalid XML processing instruction")
                    .detail("XML processing instruction cannot contain \"?>\"."),
            );
        }
        out.push(' ');
        out.push_str(arg.trim_start_matches(' '));
    }
    out.push_str("?>");
    Ok(out)
}

/// `xmlroot`: the value's declaration replaced. `standalone` is SQL/XML's
/// clause: 0 `YES`, 1 `NO`, 2 `NO VALUE`, 3 omitted (keeps the value's own).
pub(crate) fn root(value: &str, version: Option<&str>, standalone: i32) -> String {
    let decl = parse_decl(value).unwrap_or(Decl {
        len: 0,
        version: None,
        standalone: -1,
    });
    let standalone = match standalone {
        0 => 1,
        1 => 0,
        2 => -1,
        _ => decl.standalone,
    };
    let mut out = String::new();
    print_decl(&mut out, version, standalone);
    out.push_str(&value[decl.len..]);
    out
}

/// `xmlelement`: the element, its attributes already rendered.
pub(crate) fn element(name: &str, attrs: Option<&str>, content: &[String]) -> String {
    let mut out = format!("<{name}");
    if let Some(attrs) = attrs {
        out.push_str(attrs);
    }
    if content.is_empty() {
        out.push_str("/>");
        return out;
    }
    out.push('>');
    for part in content {
        out.push_str(part);
    }
    out.push_str("</");
    out.push_str(name);
    out.push('>');
    out
}

/// `xmlattributes`: each non-NULL value as `name="value"`, the values already
/// escaped for an attribute.
pub(crate) fn attributes(items: &[(String, bool, Option<String>)]) -> String {
    let mut out = String::new();
    for (name, fully_escaped, value) in items {
        if let Some(value) = value {
            out.push(' ');
            out.push_str(&identifier_to_xml_name(name, *fully_escaped));
            out.push_str("=\"");
            out.push_str(value);
            out.push('"');
        }
    }
    out
}

/// `xmlforest`: each non-NULL value as an element; all of them NULL is NULL.
pub(crate) fn forest(items: &[(String, bool, Option<String>)]) -> Option<String> {
    let mut out = String::new();
    let mut any = false;
    for (name, fully_escaped, value) in items {
        if let Some(value) = value {
            let name = identifier_to_xml_name(name, *fully_escaped);
            out.push_str(&format!("<{name}>{value}</{name}>"));
            any = true;
        }
    }
    any.then_some(out)
}

/// An element's value as an attribute: the writer's escaping.
pub(crate) fn attribute_value(value: &crate::Value) -> String {
    escape_attribute(&value_text(value))
}

/// libxml2's `xmlEscapeEntities`: the three markup characters and a carriage
/// return; everything else is written as it is.
fn escape_text(text: &str) -> String {
    let mut out = String::new();
    for c in text.chars() {
        match c {
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '&' => out.push_str("&amp;"),
            '\r' => out.push_str("&#xD;"),
            c => out.push(c),
        }
    }
    out
}

/// `xmlBufAttrSerializeTxtContent`: what an attribute value serializes to.
fn escape_attribute(value: &str) -> String {
    let mut out = String::new();
    for c in value.chars() {
        match c {
            '\n' => out.push_str("&#10;"),
            '\r' => out.push_str("&#13;"),
            '\t' => out.push_str("&#9;"),
            '"' => out.push_str("&quot;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '&' => out.push_str("&amp;"),
            c => out.push(c),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn doc_ok(text: &str) -> bool {
        is_document(text)
    }

    fn content_ok(text: &str) -> bool {
        is_well_formed(text, Mode::Content)
    }

    #[test]
    fn well_formed_documents() {
        for ok in [
            "<a/>",
            "<a>b</a>",
            "<a b='1' c=\"2\"/>",
            "<a/>\n",
            "  <a/>  ",
            "<?xml version=\"1.0\"?><a/>",
            "<?xml version=\"1.1\"?><a/>",
            "<!DOCTYPE a><a/>",
            "<!DOCTYPE a SYSTEM \"x.dtd\"><a/>",
            "<a></a >",
            "<a><!-- x --></a>",
            "<a><![CDATA[x]]></a>",
            "<a><![CDATA[x]]]></a>",
            "<a:b/>",
            "<a>b:c</a>",
        ] {
            assert!(doc_ok(ok), "{ok} is a document");
        }
        for bad in [
            "<a>",
            "<a></b>",
            "<a><b></a></b>",
            "",
            "  ",
            "<a/>junk",
            "<a/><b/>",
            "<1a/>",
            "< a/>",
            "<>",
            "<a></a:b>",
            "<a b=1/>",
            "<a b=\"1\" b=\"2\"/>",
            "<a b=\"x\"c=\"y\"/>",
            "<a/ >",
            "<a x=\"<\"/>",
            "<a x=\"&bad;\"/>",
            "<a>&unknown;</a>",
            "<a>x]]>y</a>",
            "<a>&#xD800;</a>",
            "<a>&#x0;</a>",
            "<a>&#xFFFF;</a>",
            "<a>&#X41;</a>",
            "<?xml x?><a/>",
            "<a><?></a>",
            "<!--->",
            "<!-- x -- y -->",
            "<!-- a --->",
            "<?xml version=\"2.0\"?><a/>",
            "<?xml version=\"1.0\">",
            "<a>x</a><a>y</a>",
        ] {
            assert!(!doc_ok(bad), "{bad} is not a document");
        }
    }

    #[test]
    fn well_formed_content() {
        for ok in [
            "text",
            "",
            "  ",
            "<a/>",
            "<a/>text",
            "text<a/>",
            "<a/><b/>",
            "<a>text</a><b/>",
            "&amp;&lt;",
            "&#65;",
            "<a>&#x41;</a>",
            " <a/> ",
            "<a>  </a>",
        ] {
            assert!(content_ok(ok), "{ok} is content");
        }
        for bad in [
            "<a>",
            "<a>text",
            "</a>",
            "</a><b/>",
            "text<!DOCTYPE a><a/>",
            "<a><![CDATA[x]]>]]></a>",
            "<a>&#x41</a>",
            "<a>&amp</a>",
        ] {
            assert!(!content_ok(bad), "{bad} is not content");
        }
    }

    #[test]
    fn doctype_in_content_forces_document() {
        // A DOCTYPE is not content, but SQL/XML lets content hold a document.
        assert!(content_ok("<!DOCTYPE a><a/>"));
        assert!(content_ok("<!DOCTYPE a [<!ENTITY e \"x\">]><a>&e;</a>"));
        assert!(!content_ok("<!DOCTYPE a [<!ENTITY e \"<b>\">]><a>&e;</a>"));
        assert!(!content_ok(
            "<!DOCTYPE a [<!ENTITY e \"<b/>\">]><a>&e;</a></b>"
        ));
        assert!(content_ok("<!DOCTYPE a [<!ENTITY e \"<b/>\">]><a>&e;</a>"));
        assert!(!content_ok("<!DOCTYPE a [<!ENTITY e \"&e;\">]><a>&e;</a>"));
        assert!(content_ok(
            "<!DOCTYPE a [<!ENTITY e \"x\">]><a e2=\"&e;\"/>"
        ));
        // A DOCTYPE does not belong to a balanced chunk.
        assert!(!content_ok("text<!DOCTYPE a><a/>"));
    }

    #[test]
    fn errors_match_postgresql() {
        let case = |value: &str, mode: Mode, code: &str, message: &str| {
            let Err(error) = validate(value, mode) else {
                panic!("{value} was accepted");
            };
            assert_eq!(error.code, code, "{value}");
            assert_eq!(error.message, message, "{value}");
        };
        case("<a>", Mode::Content, "2200N", "invalid XML content");
        case("<a>", Mode::Document, "2200M", "invalid XML document");
        case("", Mode::Document, "2200M", "invalid XML document");
        case(
            "<?xml ?><a/>",
            Mode::Content,
            "2200N",
            "invalid XML content: invalid XML declaration",
        );
        case("text", Mode::Document, "2200M", "invalid XML document");
    }

    #[test]
    fn declarations_match_postgresql() {
        assert_eq!(parse_decl("<a/>").unwrap().len, 0);
        let decl = parse_decl("<?xml version=\"1.0\"?><a/>").unwrap();
        assert_eq!(
            (decl.len, decl.version.as_deref(), decl.standalone),
            (21, Some("1.0"), -1)
        );
        let decl =
            parse_decl("<?xml version='1.1' encoding='UTF-8' standalone='no'?><a/>").unwrap();
        assert_eq!((decl.version.as_deref(), decl.standalone), (Some("1.1"), 0));
        assert_eq!(
            parse_decl("<?xmlfoo x?>").unwrap().len,
            0,
            "a PI named xmlfoo is not a declaration"
        );
        assert!(parse_decl("<?xml ?>").is_err());
        assert!(parse_decl("<?xml version=\"1.0\"").is_err());
        assert!(parse_decl("<?xml version=\"1.0\" standalone=\"maybe\"?>").is_err());
    }

    #[test]
    fn output_matches_postgresql() {
        assert_eq!(output("<a/>"), "<a/>");
        assert_eq!(output("  <a/>  "), "  <a/>  ");
        assert_eq!(output("\n<a/>"), "<a/>");
        assert_eq!(
            output("<?xml version=\"1.0\" encoding=\"UTF-8\"?><a/>"),
            "<a/>"
        );
        assert_eq!(
            output("<?xml version=\"1.1\"?><a/>"),
            "<?xml version=\"1.1\"?><a/>"
        );
        assert_eq!(
            output("<?xml version=\"1.0\" standalone=\"yes\"?><a/>"),
            "<?xml version=\"1.0\" standalone=\"yes\"?><a/>"
        );
    }

    #[test]
    fn concat_matches_postgresql() {
        assert_eq!(concat(&["<a/>", "<b/>"]), "<a/><b/>");
        assert_eq!(concat(&["  <a/>  "]), "  <a/>  ");
        assert_eq!(concat(&["<?xml version=\"1.0\"?><a/>", "<b/>"]), "<a/><b/>");
        assert_eq!(
            concat(&["<?xml version=\"1.1\"?><a/>", "<?xml version=\"1.1\"?><b/>"]),
            "<?xml version=\"1.1\"?><a/><b/>"
        );
        assert_eq!(
            concat(&[
                "<?xml version=\"1.0\" standalone=\"yes\"?><a/>",
                "<?xml version=\"1.0\"?><b/>"
            ]),
            "<a/><b/>"
        );
    }

    #[test]
    fn constructors_match_postgresql() {
        let xml = |value: &str| crate::Value::Text(value.to_string());
        assert_eq!(identifier_to_xml_name("Foo bar", false), "Foo_x0020_bar");
        assert_eq!(identifier_to_xml_name("a b", false), "a_x0020_b");
        assert_eq!(identifier_to_xml_name("xmlfoo", true), "_x0078_mlfoo");
        assert_eq!(identifier_to_xml_name("xmlfoo", false), "xmlfoo");
        assert_eq!(identifier_to_xml_name("a:x", true), "a_x003A_x");
        assert_eq!(identifier_to_xml_name("a:x", false), "a:x");
        assert_eq!(identifier_to_xml_name("_x1", false), "_x005F_x1");
        assert_eq!(element("foo", None, &[]), "<foo/>");
        assert_eq!(
            element("foo", Some(" a=\"1\""), &["bar".to_string()]),
            "<foo a=\"1\">bar</foo>"
        );
        assert_eq!(
            attributes(&[("a b".to_string(), false, Some("x".to_string()),)]),
            " a_x0020_b=\"x\""
        );
        assert_eq!(
            forest(&[("a".to_string(), false, Some("1".to_string()))]),
            Some("<a>1</a>".to_string())
        );
        assert_eq!(forest(&[("a".to_string(), false, None)]), None);
        assert_eq!(escape_xml("a\r&<>"), "a&#x0d;&amp;&lt;&gt;");
        assert_eq!(attribute_value(&xml("a\n\"b")), "a&#10;&quot;b");
        assert_eq!(pi("foo", Some("bar")).unwrap(), "<?foo bar?>");
        assert_eq!(pi("foo", None).unwrap(), "<?foo?>");
        assert!(pi("xml", None).is_err());
        assert!(pi("foo", Some("x?>y")).is_err());
        assert_eq!(
            root("<a/>", Some("1.1"), 0),
            "<?xml version=\"1.1\" standalone=\"yes\"?><a/>"
        );
        assert_eq!(
            root("<a/>", Some("1.0"), 1),
            "<?xml version=\"1.0\" standalone=\"no\"?><a/>"
        );
        assert_eq!(root("<a/>", None, 2), "<a/>");
        // `version no value` drops the declaration's version, and with it the
        // declaration, even where the standalone clause is omitted.
        assert_eq!(root("<?xml version=\"1.1\"?><a/>", None, 3), "<a/>");
        assert_eq!(root("<?xml version=\"1.1\"?><a/>", Some("1.0"), 3), "<a/>");
    }

    #[test]
    fn comment_and_text_match_postgresql() {
        assert_eq!(comment("ok").unwrap(), "<!--ok-->");
        assert_eq!(comment("").unwrap(), "<!---->");
        assert!(comment("a--b").is_err());
        assert!(comment("a-").is_err());
        assert_eq!(escape_special("a\"'<&>"), "a&quot;'&lt;&amp;&gt;");
        assert_eq!(escape_special("<a>b</a>"), "&lt;a&gt;b&lt;/a&gt;");
    }

    #[test]
    fn serialize_indent_matches_postgresql() {
        let indent = |value: &str, mode: Mode| serialize(value, mode, true).unwrap();
        assert_eq!(indent("<a/>", Mode::Content), "<a/>");
        assert_eq!(indent("<a/> ", Mode::Content), "<a/> ");
        assert_eq!(indent("<a> <b/> </a>", Mode::Content), "<a>\n  <b/>\n</a>");
        assert_eq!(
            indent("<a><b/><c/></a>", Mode::Content),
            "<a>\n  <b/>\n  <c/>\n</a>"
        );
        assert_eq!(
            indent("<a><b><c/></b></a>", Mode::Content),
            "<a>\n  <b>\n    <c/>\n  </b>\n</a>"
        );
        assert_eq!(
            indent("<a>  x  <b/> </a>", Mode::Content),
            "<a>  x  <b/> </a>"
        );
        assert_eq!(indent("<a> <b/> x</a>", Mode::Content), "<a><b/> x</a>");
        assert_eq!(indent("<a>  </a>", Mode::Content), "<a>  </a>");
        assert_eq!(indent("a<b/>c", Mode::Content), "a\n<b/>c");
        assert_eq!(indent("<b/>c", Mode::Content), "<b/>c");
        assert_eq!(
            indent("<a>&#65;<!--c--><b/></a>", Mode::Content),
            "<a>A<!--c--><b/></a>"
        );
        assert_eq!(
            indent("<a><!--c--></a>", Mode::Content),
            "<a>\n  <!--c-->\n</a>"
        );
        assert_eq!(
            indent("<a><?pi?></a>", Mode::Content),
            "<a>\n  <?pi?>\n</a>"
        );
        assert_eq!(indent("<a></a>", Mode::Content), "<a/>");
        assert_eq!(
            indent("<a>&#34;&#39;&amp;&lt;&gt;</a>", Mode::Content),
            "<a>\"'&amp;&lt;&gt;</a>"
        );
        assert_eq!(
            indent("<a b=\"&#34;&#39;&amp;&lt;&gt;\"/>", Mode::Content),
            "<a b=\"&quot;'&amp;&lt;&gt;\"/>"
        );
        assert_eq!(indent("<a>&#13;x</a>", Mode::Content), "<a>&#xD;x</a>");
        assert!(serialize("{not xml", Mode::Document, true).is_err());
        assert_eq!(
            indent("<?xml version=\"1.0\"?><a/>", Mode::Document),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<a/>"
        );
        assert_eq!(indent("<a/><!--c-->", Mode::Document), "<a/>\n<!--c-->");
        assert_eq!(
            indent("<!DOCTYPE a><a/>", Mode::Document),
            "<!DOCTYPE a>\n<a/>"
        );
        assert_eq!(
            indent("<!DOCTYPE a [<!ELEMENT a EMPTY>]><a/>", Mode::Document),
            "<!DOCTYPE a [\n<!ELEMENT a EMPTY>\n]>\n<a/>"
        );
        // A DOCTYPE in content is parsed as a document, and no trailing
        // newline is removed for the CONTENT option.
        assert_eq!(
            indent("<!DOCTYPE a><a/>", Mode::Content),
            "<!DOCTYPE a>\n<a/>\n"
        );
    }
}
