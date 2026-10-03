//! PostgreSQL's default text search parser: the state machine of
//! `wparser_def.c`, transcribed with its state/action table. The parser
//! splits a text into typed tokens (`asciiword`, `word`, `numword`, `email`,
//! `url`, `host`, `sfloat`, `version`, hyphenated word parts and the like),
//! which the dictionaries later turn into lexemes.

#![allow(clippy::needless_range_loop)]

// Output token categories, as PostgreSQL numbers them.
pub(crate) const ASCIIWORD: u8 = 1;
pub(crate) const WORD_T: u8 = 2;
pub(crate) const NUMWORD: u8 = 3;
pub(crate) const EMAIL: u8 = 4;
pub(crate) const URL_T: u8 = 5;
pub(crate) const HOST: u8 = 6;
pub(crate) const SCIENTIFIC: u8 = 7;
pub(crate) const VERSIONNUMBER: u8 = 8;
pub(crate) const NUMPARTHWORD: u8 = 9;
pub(crate) const PARTHWORD: u8 = 10;
pub(crate) const ASCIIPARTHWORD: u8 = 11;
pub(crate) const SPACE: u8 = 12;
pub(crate) const TAG_T: u8 = 13;
pub(crate) const PROTOCOL: u8 = 14;
pub(crate) const NUMHWORD: u8 = 15;
pub(crate) const ASCIIHWORD: u8 = 16;
pub(crate) const HWORD: u8 = 17;
pub(crate) const URLPATH: u8 = 18;
pub(crate) const FILEPATH: u8 = 19;
pub(crate) const DECIMAL_T: u8 = 20;
pub(crate) const SIGNEDINT: u8 = 21;
pub(crate) const UNSIGNEDINT: u8 = 22;
pub(crate) const XMLENTITY: u8 = 23;

/// The parser's token type aliases, by number (`tok_alias`).
pub(crate) fn token_alias(ty: u8) -> &'static str {
    match ty {
        ASCIIWORD => "asciiword",
        WORD_T => "word",
        NUMWORD => "numword",
        EMAIL => "email",
        URL_T => "url",
        HOST => "host",
        SCIENTIFIC => "sfloat",
        VERSIONNUMBER => "version",
        NUMPARTHWORD => "hword_numpart",
        PARTHWORD => "hword_part",
        ASCIIPARTHWORD => "hword_asciipart",
        SPACE => "blank",
        TAG_T => "tag",
        PROTOCOL => "protocol",
        NUMHWORD => "numhword",
        ASCIIHWORD => "asciihword",
        HWORD => "hword",
        URLPATH => "url_path",
        FILEPATH => "file",
        DECIMAL_T => "float",
        SIGNEDINT => "int",
        UNSIGNEDINT => "uint",
        XMLENTITY => "entity",
        _ => "",
    }
}

/// The parser's token type descriptions (`lex_descr`).
pub(crate) fn token_desc(ty: u8) -> &'static str {
    match ty {
        ASCIIWORD => "Word, all ASCII",
        WORD_T => "Word, all letters",
        NUMWORD => "Word, letters and digits",
        EMAIL => "Email address",
        URL_T => "URL",
        HOST => "Host",
        SCIENTIFIC => "Scientific notation",
        VERSIONNUMBER => "Version number",
        NUMPARTHWORD => "Hyphenated word part, letters and digits",
        PARTHWORD => "Hyphenated word part, all letters",
        ASCIIPARTHWORD => "Hyphenated word part, all ASCII",
        SPACE => "Space symbols",
        TAG_T => "XML tag",
        PROTOCOL => "Protocol head",
        NUMHWORD => "Hyphenated word, letters and digits",
        ASCIIHWORD => "Hyphenated word, all ASCII",
        HWORD => "Hyphenated word, all letters",
        URLPATH => "URL path",
        FILEPATH => "File or path name",
        DECIMAL_T => "Decimal notation",
        SIGNEDINT => "Signed integer",
        UNSIGNEDINT => "Unsigned integer",
        XMLENTITY => "XML entity",
        _ => "",
    }
}

/// The parser states, in the enum's order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum S {
    Base,
    InNumWord,
    InAsciiWord,
    InWord,
    InUnsignedInt,
    InSignedIntFirst,
    InSignedInt,
    InSpace,
    InUDecimalFirst,
    InUDecimal,
    InDecimalFirst,
    InDecimal,
    InVerVersion,
    InSVerVersion,
    InVersionFirst,
    InVersion,
    InMantissaFirst,
    InMantissaSign,
    InMantissa,
    InXMLEntityFirst,
    InXMLEntity,
    InXMLEntityNumFirst,
    InXMLEntityNum,
    InXMLEntityHexNumFirst,
    InXMLEntityHexNum,
    InXMLEntityEnd,
    InTagFirst,
    InXMLBegin,
    InTagCloseFirst,
    InTagName,
    InTagBeginEnd,
    InTag,
    InTagEscapeK,
    InTagEscapeKK,
    InTagBackSleshed,
    InTagEnd,
    InCommentFirst,
    InCommentLast,
    InComment,
    InCloseCommentFirst,
    InCloseCommentLast,
    InCommentEnd,
    InHostFirstDomain,
    InHostDomainSecond,
    InHostDomain,
    InPortFirst,
    InPort,
    InHostFirstAN,
    InHost,
    InEmail,
    InFileFirst,
    InFileTwiddle,
    InPathFirst,
    InPathFirstFirst,
    InPathSecond,
    InFile,
    InFileNext,
    InURLPathFirst,
    InURLPathStart,
    InURLPath,
    InFURL,
    InProtocolFirst,
    InProtocolSecond,
    InProtocolEnd,
    InHyphenAsciiWordFirst,
    InHyphenAsciiWord,
    InHyphenWordFirst,
    InHyphenWord,
    InHyphenNumWordFirst,
    InHyphenNumWord,
    InHyphenDigitLookahead,
    InParseHyphen,
    InParseHyphenHyphen,
    InHyphenWordPart,
    InHyphenAsciiWordPart,
    InHyphenNumWordPart,
    InHyphenUnsignedInt,
    /// Not a state: the placeholder that keeps an action from setting one.
    Null,
}

/// The character tests of the action table.
#[derive(Clone, Copy)]
enum P {
    /// The `NULL` test of the table: always true.
    Any,
    Eof,
    /// `p_iseqC` with the action's character.
    Eq(u8),
    Ignore,
    Asclet,
    Alpha,
    Digit,
    Alnum,
    NotAlnum,
    Special,
    Space,
    Xdigit,
    UrlChar,
    Host,
    UrlPath,
    StopHost,
}

/// The flags of an action.
struct F;
impl F {
    const NEXT: u16 = 0x0000;
    const BINGO: u16 = 0x0001;
    const POP: u16 = 0x0002;
    const PUSH: u16 = 0x0004;
    const RERUN: u16 = 0x0008;
    const CLEAR: u16 = 0x0010;
    const MERGE: u16 = 0x0020;
    const CLRALL: u16 = 0x0040;
}

/// The special handlers of the action table.
#[derive(Clone, Copy)]
enum SP {
    None,
    Tags,
    FURL,
    Hyphen,
    VerVersion,
}

struct A {
    p: P,
    f: u16,
    s: S,
    t: u8,
    sp: SP,
}

/// A token the parser found.
pub(crate) struct ParseToken {
    pub text: String,
    pub ty: u8,
}

/// One position on the parser's stack.
#[derive(Clone, Copy)]
struct Frame {
    posbyte: usize,
    poschar: usize,
    charlen: usize,
    lenbytetoken: usize,
    lenchartoken: usize,
    state: S,
    /// The index of the action that pushed this frame, when it was pushed.
    pushed: Option<usize>,
}

/// The parser: a byte string and a stack of positions.
pub(crate) struct Parser<'a> {
    bytes: &'a [u8],
    lenstr: usize,
    frames: Vec<Frame>,
    /// The character an `Eq` test compares against (PostgreSQL's `prs->c`).
    c: u8,
    ignore: bool,
    wanthost: bool,
    token_start: usize,
    /// The token's type, byte length, and character length.
    pub ty: u8,
    pub lenbytetoken: usize,
    pub lenchartoken: usize,
}

impl<'a> Parser<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Parser {
            bytes,
            lenstr: bytes.len(),
            frames: vec![Frame {
                posbyte: 0,
                poschar: 0,
                charlen: 0,
                lenbytetoken: 0,
                lenchartoken: 0,
                state: S::Base,
                pushed: None,
            }],
            c: 0,
            ignore: false,
            wanthost: false,
            token_start: 0,
            ty: 0,
            lenbytetoken: 0,
            lenchartoken: 0,
        }
    }

    /// A parser over the bytes from `start` on, as `TParserCopyInit` makes
    /// for a lookahead test.
    fn at(bytes: &'a [u8], start: usize) -> Self {
        Parser::new(&bytes[start..])
    }

    fn frame(&self) -> &Frame {
        self.frames.last().expect("a frame is always present")
    }

    fn frame_mut(&mut self) -> &mut Frame {
        self.frames.last_mut().expect("a frame is always present")
    }

    fn posbyte(&self) -> usize {
        self.frame().posbyte
    }

    fn byte_at_pos(&self) -> u8 {
        self.bytes.get(self.posbyte()).copied().unwrap_or(0)
    }

    /// The current character's length in bytes (0 at the end).
    fn charlen(&self) -> usize {
        self.frame().charlen
    }

    /// The token the parser last found.
    pub(crate) fn token_bytes(&self) -> &[u8] {
        &self.bytes[self.token_start..self.token_start + self.lenbytetoken]
    }

    /// Reads one token, as `TParserGet` does.
    pub(crate) fn next_token(&mut self) -> bool {
        if self.posbyte() >= self.lenstr {
            return false;
        }
        self.token_start = self.posbyte();
        self.frame_mut().pushed = None;
        let mut bingoed = false;

        'outer: while self.posbyte() <= self.lenstr {
            let chlen = if self.posbyte() == self.lenstr {
                0
            } else {
                utf8_len(self.bytes[self.posbyte()])
            };
            self.frame_mut().charlen = chlen;

            // The action to apply: after a pop, the one following the push;
            // otherwise the state's table from its start.
            let table = actions(self.frame().state);
            let mut idx = match self.frame().pushed {
                Some(at) => {
                    self.frame_mut().pushed = None;
                    at + 1
                }
                None => 0,
            };
            if idx >= table.len() {
                idx = table.len() - 1;
            }
            loop {
                let action = &table[idx];
                self.c = match action.p {
                    P::Eq(c) => c,
                    _ => self.c,
                };
                if test_mut(action.p, self) {
                    break;
                }
                idx += 1;
                if idx >= table.len() {
                    idx = table.len() - 1;
                    break;
                }
            }
            let item = &table[idx];

            // A special handler first.
            match item.sp {
                SP::None => {}
                SP::Tags => self.special_tags(),
                SP::FURL => {
                    self.wanthost = true;
                    let (b, c) = {
                        let frame = self.frame();
                        (frame.lenbytetoken, frame.lenchartoken)
                    };
                    self.frame_mut().posbyte -= b;
                    self.frame_mut().poschar -= c;
                }
                SP::Hyphen => {
                    let (b, c) = {
                        let frame = self.frame();
                        (frame.lenbytetoken, frame.lenchartoken)
                    };
                    self.frame_mut().posbyte -= b;
                    self.frame_mut().poschar -= c;
                }
                SP::VerVersion => {
                    let (b, c) = {
                        let frame = self.frame();
                        (frame.lenbytetoken, frame.lenchartoken)
                    };
                    let frame = self.frame_mut();
                    frame.posbyte -= b;
                    frame.poschar -= c;
                    frame.lenbytetoken = 0;
                    frame.lenchartoken = 0;
                }
            }

            if item.f & F::BINGO != 0 {
                let (bytetoken, chartoken) = {
                    let frame = self.frame_mut();
                    let taken = (frame.lenbytetoken, frame.lenchartoken);
                    frame.lenbytetoken = 0;
                    frame.lenchartoken = 0;
                    taken
                };
                self.lenbytetoken = bytetoken;
                self.lenchartoken = chartoken;
                self.ty = item.t;
                bingoed = true;
            }

            if item.f & F::POP != 0 {
                self.frames.pop();
            } else if item.f & F::PUSH != 0 {
                self.frame_mut().pushed = Some(idx);
                // The new frame starts fresh: it does not resume after the
                // pushing action.
                let mut pushed = *self.frame();
                pushed.pushed = None;
                self.frames.push(pushed);
            } else if item.f & F::CLEAR != 0 {
                // Drops the frame below the current one.
                let below = self.frames.len() as isize - 2;
                if below >= 0 {
                    self.frames.remove(below as usize);
                }
            } else if item.f & F::CLRALL != 0 {
                let top = *self.frame();
                self.frames.clear();
                self.frames.push(top);
            } else if item.f & F::MERGE != 0 {
                let top = *self.frame();
                self.frames.pop();
                let below = self.frame_mut();
                below.posbyte = top.posbyte;
                below.poschar = top.poschar;
                below.charlen = top.charlen;
                below.lenbytetoken = top.lenbytetoken;
                below.lenchartoken = top.lenchartoken;
            }

            if item.s != S::Null {
                self.frame_mut().state = item.s;
            }

            if item.f & F::BINGO != 0 || (self.posbyte() >= self.lenstr && item.f & F::RERUN == 0) {
                break 'outer;
            }
            if item.f & (F::RERUN | F::POP) != 0 {
                continue 'outer;
            }
            if self.charlen() != 0 {
                let chlen = self.charlen();
                let frame = self.frame_mut();
                frame.posbyte += chlen;
                frame.lenbytetoken += chlen;
                frame.poschar += 1;
                frame.lenchartoken += 1;
            }
        }

        bingoed
    }

    /// `SpecialTags`: watches for script and style elements, whose contents
    /// are not parsed.
    fn special_tags(&mut self) {
        let len = self.frame().lenchartoken;
        let token = &self.bytes[self.token_start..];
        let starts = |s: &[u8]| token.len() >= s.len() && token[..s.len()].eq_ignore_ascii_case(s);
        match len {
            8 if starts(b"</script") => self.ignore = false,
            7 if starts(b"</style") => self.ignore = false,
            7 if starts(b"<script") => self.ignore = true,
            6 if starts(b"<style") => self.ignore = true,
            _ => {}
        }
    }

    /// `p_ishost`: a host at the current position, consumed if found.
    fn probe_host(&mut self) -> bool {
        let mut sub = Parser::at(self.bytes, self.posbyte());
        sub.wanthost = true;
        if sub.next_token() && sub.ty == HOST {
            let frame = self.frame_mut();
            frame.posbyte += sub.lenbytetoken;
            frame.poschar += sub.lenchartoken;
            frame.lenbytetoken += sub.lenbytetoken;
            frame.lenchartoken += sub.lenchartoken;
            frame.charlen = sub.frame().charlen;
            return true;
        }
        false
    }

    /// `p_isURLPath`: a URL path at the current position, consumed if found.
    fn probe_url_path(&mut self) -> bool {
        let mut sub = Parser::at(self.bytes, self.posbyte());
        sub.frames.push(Frame {
            state: S::InURLPathFirst,
            ..*sub.frame()
        });
        if sub.next_token() && sub.ty == URLPATH {
            let frame = self.frame_mut();
            frame.posbyte += sub.lenbytetoken;
            frame.poschar += sub.lenchartoken;
            frame.lenbytetoken += sub.lenbytetoken;
            frame.lenchartoken += sub.lenchartoken;
            frame.charlen = sub.frame().charlen;
            return true;
        }
        false
    }

    /// The current character, decoded.
    fn current_char(&self) -> Option<char> {
        let pos = self.posbyte();
        let len = self.charlen();
        if len == 0 {
            return None;
        }
        let slice = self.bytes.get(pos..pos + len)?;
        std::str::from_utf8(slice).ok()?.chars().next()
    }
}

/// The current character's length in bytes, from its first byte.
fn utf8_len(first: u8) -> usize {
    match first {
        0x00..=0x7f => 1,
        0xc0..=0xdf => 2,
        0xe0..=0xef => 3,
        _ => 4,
    }
}

fn is_ascii_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

fn is_urlchar(b: u8) -> bool {
    if b <= 0x20 || b >= 0x7f {
        return false;
    }
    !matches!(
        b,
        b'"' | b'<' | b'>' | b'\\' | b'^' | b'`' | b'{' | b'|' | b'}'
    )
}

/// Whether the character has no displayed width or is one of the combining
/// marks PostgreSQL lists. Such characters do not break a word, though they
/// are not letters.
fn is_special(c: char) -> bool {
    unicode_normalization::char::is_combining_mark(c) || STRANGE_LETTER.contains(&(c as u32))
}

/// Runs an action's character test.
fn test(p: P, prs: &Parser) -> bool {
    let chlen = prs.charlen();
    let byte = prs.byte_at_pos();
    let ascii = chlen == 1 && byte < 0x80;
    let alpha = if ascii {
        byte.is_ascii_alphabetic()
    } else {
        // A non-ASCII character is a letter in PostgreSQL's C-locale rule.
        chlen > 0
    };
    let digit = ascii && byte.is_ascii_digit();
    match p {
        P::Any => true,
        P::Eof => prs.posbyte() == prs.lenstr || chlen == 0,
        P::Eq(c) => chlen == 1 && byte == c,
        P::Ignore => prs.ignore,
        P::Asclet => ascii && alpha,
        P::Alpha => alpha,
        P::Digit => digit,
        P::Alnum => alpha || digit,
        P::NotAlnum => !(alpha || digit),
        P::Space => ascii && is_ascii_space(byte),
        P::Xdigit => ascii && byte.is_ascii_hexdigit(),
        P::UrlChar => chlen == 1 && is_urlchar(byte),
        P::Special => prs.current_char().is_some_and(is_special),
        P::StopHost => {
            if prs.wanthost {
                // The test has no way to clear the flag through a shared
                // reference; the caller clears it via `test_mut`.
                true
            } else {
                false
            }
        }
        // The two lookahead tests consume what they find, so they are
        // handled by `test_mut`.
        P::Host | P::UrlPath => false,
    }
}

/// Runs an action's test, for the tests that consume what they match.
fn test_mut(p: P, prs: &mut Parser) -> bool {
    match p {
        P::StopHost => {
            if prs.wanthost {
                prs.wanthost = false;
                true
            } else {
                false
            }
        }
        P::Host => prs.probe_host(),
        P::UrlPath => prs.probe_url_path(),
        other => test(other, prs),
    }
}

/// Parses a text into its typed tokens.
pub(crate) fn parse(text: &str) -> Vec<ParseToken> {
    let mut parser = Parser::new(text.as_bytes());
    let mut out = Vec::new();
    while parser.next_token() {
        out.push(ParseToken {
            text: String::from_utf8_lossy(parser.token_bytes()).into_owned(),
            ty: parser.ty,
        });
    }
    out
}

/// The `strange_letter` table: spacing combining marks, which do not break a
/// word though they are not letters.
const STRANGE_LETTER: [u32; 228] = [
    0x0903, 0x093E, 0x093F, 0x0940, 0x0949, 0x094A, 0x094B, 0x094C, 0x0982, 0x0983, 0x09BE, 0x09BF,
    0x09C0, 0x09C7, 0x09C8, 0x09CB, 0x09CC, 0x09D7, 0x0A03, 0x0A3E, 0x0A3F, 0x0A40, 0x0A83, 0x0ABE,
    0x0ABF, 0x0AC0, 0x0AC9, 0x0ACB, 0x0ACC, 0x0B02, 0x0B03, 0x0B3E, 0x0B40, 0x0B47, 0x0B48, 0x0B4B,
    0x0B4C, 0x0B57, 0x0BBE, 0x0BBF, 0x0BC1, 0x0BC2, 0x0BC6, 0x0BC7, 0x0BC8, 0x0BCA, 0x0BCB, 0x0BCC,
    0x0BD7, 0x0C01, 0x0C02, 0x0C03, 0x0C41, 0x0C42, 0x0C43, 0x0C44, 0x0C82, 0x0C83, 0x0CBE, 0x0CC0,
    0x0CC1, 0x0CC2, 0x0CC3, 0x0CC4, 0x0CC7, 0x0CC8, 0x0CCA, 0x0CCB, 0x0CD5, 0x0CD6, 0x0D02, 0x0D03,
    0x0D3E, 0x0D3F, 0x0D40, 0x0D46, 0x0D47, 0x0D48, 0x0D4A, 0x0D4B, 0x0D4C, 0x0D57, 0x0D82, 0x0D83,
    0x0DCF, 0x0DD0, 0x0DD1, 0x0DD8, 0x0DD9, 0x0DDA, 0x0DDB, 0x0DDC, 0x0DDD, 0x0DDE, 0x0DDF, 0x0DF2,
    0x0DF3, 0x0F3E, 0x0F3F, 0x0F7F, 0x102B, 0x102C, 0x1031, 0x1038, 0x103B, 0x103C, 0x1056, 0x1057,
    0x1062, 0x1063, 0x1064, 0x1067, 0x1068, 0x1069, 0x106A, 0x106B, 0x106C, 0x106D, 0x1083, 0x1084,
    0x1087, 0x1088, 0x1089, 0x108A, 0x108B, 0x108C, 0x108F, 0x17B6, 0x17BE, 0x17BF, 0x17C0, 0x17C1,
    0x17C2, 0x17C3, 0x17C4, 0x17C5, 0x17C7, 0x17C8, 0x1923, 0x1924, 0x1925, 0x1926, 0x1929, 0x192A,
    0x192B, 0x1930, 0x1931, 0x1933, 0x1934, 0x1935, 0x1936, 0x1937, 0x1938, 0x19B0, 0x19B1, 0x19B2,
    0x19B3, 0x19B4, 0x19B5, 0x19B6, 0x19B7, 0x19B8, 0x19B9, 0x19BA, 0x19BB, 0x19BC, 0x19BD, 0x19BE,
    0x19BF, 0x19C0, 0x19C8, 0x19C9, 0x1A19, 0x1A1A, 0x1A1B, 0x1B04, 0x1B35, 0x1B3B, 0x1B3D, 0x1B3E,
    0x1B3F, 0x1B40, 0x1B41, 0x1B43, 0x1B44, 0x1B82, 0x1BA1, 0x1BA6, 0x1BA7, 0x1BAA, 0x1C24, 0x1C25,
    0x1C26, 0x1C27, 0x1C28, 0x1C29, 0x1C2A, 0x1C2B, 0x1C34, 0x1C35, 0xA823, 0xA824, 0xA827, 0xA880,
    0xA881, 0xA8B4, 0xA8B5, 0xA8B6, 0xA8B7, 0xA8B8, 0xA8B9, 0xA8BA, 0xA8BB, 0xA8BC, 0xA8BD, 0xA8BE,
    0xA8BF, 0xA8C0, 0xA8C1, 0xA8C2, 0xA8C3, 0xA952, 0xA953, 0xAA2F, 0xAA30, 0xAA33, 0xAA34, 0xAA4D,
];

/// The state/action table of the default parser, states in the enum's order.
fn actions(state: S) -> &'static [A] {
    static ACTIONS: [&[A]; 77] = [
        /* TPS_Base */
        &[
            A {
                p: P::Eof,
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(60),
                f: F::PUSH,
                s: S::InTagFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Ignore,
                f: F::NEXT,
                s: S::InSpace,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Asclet,
                f: F::NEXT,
                s: S::InAsciiWord,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Alpha,
                f: F::NEXT,
                s: S::InWord,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::InUnsignedInt,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(45),
                f: F::PUSH,
                s: S::InSignedIntFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(43),
                f: F::PUSH,
                s: S::InSignedIntFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(38),
                f: F::PUSH,
                s: S::InXMLEntityFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(126),
                f: F::PUSH,
                s: S::InFileTwiddle,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(47),
                f: F::PUSH,
                s: S::InFileFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(46),
                f: F::PUSH,
                s: S::InPathFirstFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::NEXT,
                s: S::InSpace,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InNumWord */
        &[
            A {
                p: P::Eof,
                f: F::BINGO,
                s: S::Base,
                t: NUMWORD,
                sp: SP::None,
            },
            A {
                p: P::Alnum,
                f: F::NEXT,
                s: S::InNumWord,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Special,
                f: F::NEXT,
                s: S::InNumWord,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(64),
                f: F::PUSH,
                s: S::InEmail,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(47),
                f: F::PUSH,
                s: S::InFileFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(46),
                f: F::PUSH,
                s: S::InFileNext,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(45),
                f: F::PUSH,
                s: S::InHyphenNumWordFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::BINGO,
                s: S::Base,
                t: NUMWORD,
                sp: SP::None,
            },
        ],
        /* TPS_InAsciiWord */
        &[
            A {
                p: P::Eof,
                f: F::BINGO,
                s: S::Base,
                t: ASCIIWORD,
                sp: SP::None,
            },
            A {
                p: P::Asclet,
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(46),
                f: F::PUSH,
                s: S::InHostFirstDomain,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(46),
                f: F::PUSH,
                s: S::InFileNext,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(45),
                f: F::PUSH,
                s: S::InHostFirstAN,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(45),
                f: F::PUSH,
                s: S::InHyphenAsciiWordFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(95),
                f: F::PUSH,
                s: S::InHostFirstAN,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(64),
                f: F::PUSH,
                s: S::InEmail,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(58),
                f: F::PUSH,
                s: S::InProtocolFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(47),
                f: F::PUSH,
                s: S::InFileFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::PUSH,
                s: S::InHost,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::InNumWord,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Alpha,
                f: F::NEXT,
                s: S::InWord,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Special,
                f: F::NEXT,
                s: S::InWord,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::BINGO,
                s: S::Base,
                t: ASCIIWORD,
                sp: SP::None,
            },
        ],
        /* TPS_InWord */
        &[
            A {
                p: P::Eof,
                f: F::BINGO,
                s: S::Base,
                t: WORD_T,
                sp: SP::None,
            },
            A {
                p: P::Alpha,
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Special,
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::InNumWord,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(45),
                f: F::PUSH,
                s: S::InHyphenWordFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::BINGO,
                s: S::Base,
                t: WORD_T,
                sp: SP::None,
            },
        ],
        /* TPS_InUnsignedInt */
        &[
            A {
                p: P::Eof,
                f: F::BINGO,
                s: S::Base,
                t: UNSIGNEDINT,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(46),
                f: F::PUSH,
                s: S::InHostFirstDomain,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(46),
                f: F::PUSH,
                s: S::InUDecimalFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(101),
                f: F::PUSH,
                s: S::InMantissaFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(69),
                f: F::PUSH,
                s: S::InMantissaFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(45),
                f: F::PUSH,
                s: S::InHostFirstAN,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(95),
                f: F::PUSH,
                s: S::InHostFirstAN,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(64),
                f: F::PUSH,
                s: S::InEmail,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Asclet,
                f: F::PUSH,
                s: S::InHost,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Alpha,
                f: F::NEXT,
                s: S::InNumWord,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Special,
                f: F::NEXT,
                s: S::InNumWord,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(47),
                f: F::PUSH,
                s: S::InFileFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::BINGO,
                s: S::Base,
                t: UNSIGNEDINT,
                sp: SP::None,
            },
        ],
        /* TPS_InSignedIntFirst */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT | F::CLEAR,
                s: S::InSignedInt,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InSignedInt */
        &[
            A {
                p: P::Eof,
                f: F::BINGO,
                s: S::Base,
                t: SIGNEDINT,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(46),
                f: F::PUSH,
                s: S::InDecimalFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(101),
                f: F::PUSH,
                s: S::InMantissaFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(69),
                f: F::PUSH,
                s: S::InMantissaFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::BINGO,
                s: S::Base,
                t: SIGNEDINT,
                sp: SP::None,
            },
        ],
        /* TPS_InSpace */
        &[
            A {
                p: P::Eof,
                f: F::BINGO,
                s: S::Base,
                t: SPACE,
                sp: SP::None,
            },
            A {
                p: P::Eq(60),
                f: F::BINGO,
                s: S::Base,
                t: SPACE,
                sp: SP::None,
            },
            A {
                p: P::Ignore,
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(45),
                f: F::BINGO,
                s: S::Base,
                t: SPACE,
                sp: SP::None,
            },
            A {
                p: P::Eq(43),
                f: F::BINGO,
                s: S::Base,
                t: SPACE,
                sp: SP::None,
            },
            A {
                p: P::Eq(38),
                f: F::BINGO,
                s: S::Base,
                t: SPACE,
                sp: SP::None,
            },
            A {
                p: P::Eq(47),
                f: F::BINGO,
                s: S::Base,
                t: SPACE,
                sp: SP::None,
            },
            A {
                p: P::NotAlnum,
                f: F::NEXT,
                s: S::InSpace,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::BINGO,
                s: S::Base,
                t: SPACE,
                sp: SP::None,
            },
        ],
        /* TPS_InUDecimalFirst */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::CLEAR,
                s: S::InUDecimal,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InUDecimal */
        &[
            A {
                p: P::Eof,
                f: F::BINGO,
                s: S::Base,
                t: DECIMAL_T,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::InUDecimal,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(46),
                f: F::PUSH,
                s: S::InVersionFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(101),
                f: F::PUSH,
                s: S::InMantissaFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(69),
                f: F::PUSH,
                s: S::InMantissaFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::BINGO,
                s: S::Base,
                t: DECIMAL_T,
                sp: SP::None,
            },
        ],
        /* TPS_InDecimalFirst */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::CLEAR,
                s: S::InDecimal,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InDecimal */
        &[
            A {
                p: P::Eof,
                f: F::BINGO,
                s: S::Base,
                t: DECIMAL_T,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::InDecimal,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(46),
                f: F::PUSH,
                s: S::InVerVersion,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(101),
                f: F::PUSH,
                s: S::InMantissaFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(69),
                f: F::PUSH,
                s: S::InMantissaFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::BINGO,
                s: S::Base,
                t: DECIMAL_T,
                sp: SP::None,
            },
        ],
        /* TPS_InVerVersion */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::RERUN,
                s: S::InSVerVersion,
                t: 0,
                sp: SP::VerVersion,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InSVerVersion */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::BINGO | F::CLRALL,
                s: S::InUnsignedInt,
                t: SPACE,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InVersionFirst */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::CLEAR,
                s: S::InVersion,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InVersion */
        &[
            A {
                p: P::Eof,
                f: F::BINGO,
                s: S::Base,
                t: VERSIONNUMBER,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::InVersion,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(46),
                f: F::PUSH,
                s: S::InVersionFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::BINGO,
                s: S::Base,
                t: VERSIONNUMBER,
                sp: SP::None,
            },
        ],
        /* TPS_InMantissaFirst */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::CLEAR,
                s: S::InMantissa,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(43),
                f: F::NEXT,
                s: S::InMantissaSign,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(45),
                f: F::NEXT,
                s: S::InMantissaSign,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InMantissaSign */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::CLEAR,
                s: S::InMantissa,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InMantissa */
        &[
            A {
                p: P::Eof,
                f: F::BINGO,
                s: S::Base,
                t: SCIENTIFIC,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::InMantissa,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::BINGO,
                s: S::Base,
                t: SCIENTIFIC,
                sp: SP::None,
            },
        ],
        /* TPS_InXMLEntityFirst */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(35),
                f: F::NEXT,
                s: S::InXMLEntityNumFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Asclet,
                f: F::NEXT,
                s: S::InXMLEntity,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(58),
                f: F::NEXT,
                s: S::InXMLEntity,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(95),
                f: F::NEXT,
                s: S::InXMLEntity,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InXMLEntity */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Alnum,
                f: F::NEXT,
                s: S::InXMLEntity,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(58),
                f: F::NEXT,
                s: S::InXMLEntity,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(95),
                f: F::NEXT,
                s: S::InXMLEntity,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(46),
                f: F::NEXT,
                s: S::InXMLEntity,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(45),
                f: F::NEXT,
                s: S::InXMLEntity,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(59),
                f: F::NEXT,
                s: S::InXMLEntityEnd,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InXMLEntityNumFirst */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(120),
                f: F::NEXT,
                s: S::InXMLEntityHexNumFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(88),
                f: F::NEXT,
                s: S::InXMLEntityHexNumFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::InXMLEntityNum,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InXMLEntityNum */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::InXMLEntityNum,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(59),
                f: F::NEXT,
                s: S::InXMLEntityEnd,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InXMLEntityHexNumFirst */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Xdigit,
                f: F::NEXT,
                s: S::InXMLEntityHexNum,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InXMLEntityHexNum */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Xdigit,
                f: F::NEXT,
                s: S::InXMLEntityHexNum,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(59),
                f: F::NEXT,
                s: S::InXMLEntityEnd,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InXMLEntityEnd */
        &[A {
            p: P::Any,
            f: F::BINGO | F::CLEAR,
            s: S::Base,
            t: XMLENTITY,
            sp: SP::None,
        }],
        /* TPS_InTagFirst */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(47),
                f: F::PUSH,
                s: S::InTagCloseFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(33),
                f: F::PUSH,
                s: S::InCommentFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(63),
                f: F::PUSH,
                s: S::InXMLBegin,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Asclet,
                f: F::PUSH,
                s: S::InTagName,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(58),
                f: F::PUSH,
                s: S::InTagName,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(95),
                f: F::PUSH,
                s: S::InTagName,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InXMLBegin */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(120),
                f: F::NEXT,
                s: S::InTag,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InTagCloseFirst */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Asclet,
                f: F::NEXT,
                s: S::InTagName,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InTagName */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(47),
                f: F::NEXT,
                s: S::InTagBeginEnd,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(62),
                f: F::NEXT,
                s: S::InTagEnd,
                t: 0,
                sp: SP::Tags,
            },
            A {
                p: P::Space,
                f: F::NEXT,
                s: S::InTag,
                t: 0,
                sp: SP::Tags,
            },
            A {
                p: P::Alnum,
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(58),
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(95),
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(46),
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(45),
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InTagBeginEnd */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(62),
                f: F::NEXT,
                s: S::InTagEnd,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InTag */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(62),
                f: F::NEXT,
                s: S::InTagEnd,
                t: 0,
                sp: SP::Tags,
            },
            A {
                p: P::Eq(39),
                f: F::NEXT,
                s: S::InTagEscapeK,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(34),
                f: F::NEXT,
                s: S::InTagEscapeKK,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Asclet,
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(61),
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(45),
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(95),
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(35),
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(47),
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(58),
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(46),
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(38),
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(63),
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(37),
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(126),
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Space,
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::Tags,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InTagEscapeK */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(92),
                f: F::PUSH,
                s: S::InTagBackSleshed,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(39),
                f: F::NEXT,
                s: S::InTag,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::NEXT,
                s: S::InTagEscapeK,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InTagEscapeKK */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(92),
                f: F::PUSH,
                s: S::InTagBackSleshed,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(34),
                f: F::NEXT,
                s: S::InTag,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::NEXT,
                s: S::InTagEscapeKK,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InTagBackSleshed */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::MERGE,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InTagEnd */
        &[A {
            p: P::Any,
            f: F::BINGO | F::CLRALL,
            s: S::Base,
            t: TAG_T,
            sp: SP::None,
        }],
        /* TPS_InCommentFirst */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(45),
                f: F::NEXT,
                s: S::InCommentLast,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(68),
                f: F::NEXT,
                s: S::InTag,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(100),
                f: F::NEXT,
                s: S::InTag,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InCommentLast */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(45),
                f: F::NEXT,
                s: S::InComment,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InComment */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(45),
                f: F::NEXT,
                s: S::InCloseCommentFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InCloseCommentFirst */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(45),
                f: F::NEXT,
                s: S::InCloseCommentLast,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::NEXT,
                s: S::InComment,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InCloseCommentLast */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(45),
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(62),
                f: F::NEXT,
                s: S::InCommentEnd,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::NEXT,
                s: S::InComment,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InCommentEnd */
        &[A {
            p: P::Any,
            f: F::BINGO | F::CLRALL,
            s: S::Base,
            t: TAG_T,
            sp: SP::None,
        }],
        /* TPS_InHostFirstDomain */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Asclet,
                f: F::NEXT,
                s: S::InHostDomainSecond,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::InHost,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InHostDomainSecond */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Asclet,
                f: F::NEXT,
                s: S::InHostDomain,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::PUSH,
                s: S::InHost,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(45),
                f: F::PUSH,
                s: S::InHostFirstAN,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(95),
                f: F::PUSH,
                s: S::InHostFirstAN,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(46),
                f: F::PUSH,
                s: S::InHostFirstDomain,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(64),
                f: F::PUSH,
                s: S::InEmail,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InHostDomain */
        &[
            A {
                p: P::Eof,
                f: F::BINGO | F::CLRALL,
                s: S::Base,
                t: HOST,
                sp: SP::None,
            },
            A {
                p: P::Asclet,
                f: F::NEXT,
                s: S::InHostDomain,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::PUSH,
                s: S::InHost,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(58),
                f: F::PUSH,
                s: S::InPortFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(45),
                f: F::PUSH,
                s: S::InHostFirstAN,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(95),
                f: F::PUSH,
                s: S::InHostFirstAN,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(46),
                f: F::PUSH,
                s: S::InHostFirstDomain,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(64),
                f: F::PUSH,
                s: S::InEmail,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::StopHost,
                f: F::BINGO | F::CLRALL,
                s: S::InURLPathStart,
                t: HOST,
                sp: SP::None,
            },
            A {
                p: P::Eq(47),
                f: F::PUSH,
                s: S::InFURL,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::BINGO | F::CLRALL,
                s: S::Base,
                t: HOST,
                sp: SP::None,
            },
        ],
        /* TPS_InPortFirst */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::InPort,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InPort */
        &[
            A {
                p: P::Eof,
                f: F::BINGO | F::CLRALL,
                s: S::Base,
                t: HOST,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::InPort,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::StopHost,
                f: F::BINGO | F::CLRALL,
                s: S::InURLPathStart,
                t: HOST,
                sp: SP::None,
            },
            A {
                p: P::Eq(47),
                f: F::PUSH,
                s: S::InFURL,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::BINGO | F::CLRALL,
                s: S::Base,
                t: HOST,
                sp: SP::None,
            },
        ],
        /* TPS_InHostFirstAN */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::InHost,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Asclet,
                f: F::NEXT,
                s: S::InHost,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InHost */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::InHost,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Asclet,
                f: F::NEXT,
                s: S::InHost,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(64),
                f: F::PUSH,
                s: S::InEmail,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(46),
                f: F::PUSH,
                s: S::InHostFirstDomain,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(45),
                f: F::PUSH,
                s: S::InHostFirstAN,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(95),
                f: F::PUSH,
                s: S::InHostFirstAN,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InEmail */
        &[
            A {
                p: P::StopHost,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Host,
                f: F::BINGO | F::CLRALL,
                s: S::Base,
                t: EMAIL,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InFileFirst */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Asclet,
                f: F::NEXT,
                s: S::InFile,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::InFile,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(46),
                f: F::NEXT,
                s: S::InPathFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(95),
                f: F::NEXT,
                s: S::InFile,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(126),
                f: F::PUSH,
                s: S::InFileTwiddle,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InFileTwiddle */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Asclet,
                f: F::NEXT,
                s: S::InFile,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::InFile,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(95),
                f: F::NEXT,
                s: S::InFile,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(47),
                f: F::NEXT,
                s: S::InFileFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InPathFirst */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Asclet,
                f: F::NEXT,
                s: S::InFile,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::InFile,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(95),
                f: F::NEXT,
                s: S::InFile,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(46),
                f: F::NEXT,
                s: S::InPathSecond,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(47),
                f: F::NEXT,
                s: S::InFileFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InPathFirstFirst */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(46),
                f: F::NEXT,
                s: S::InPathSecond,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(47),
                f: F::NEXT,
                s: S::InFileFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InPathSecond */
        &[
            A {
                p: P::Eof,
                f: F::BINGO | F::CLEAR,
                s: S::Base,
                t: FILEPATH,
                sp: SP::None,
            },
            A {
                p: P::Eq(47),
                f: F::NEXT | F::PUSH,
                s: S::InFileFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(47),
                f: F::BINGO | F::CLEAR,
                s: S::Base,
                t: FILEPATH,
                sp: SP::None,
            },
            A {
                p: P::Space,
                f: F::BINGO | F::CLEAR,
                s: S::Base,
                t: FILEPATH,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InFile */
        &[
            A {
                p: P::Eof,
                f: F::BINGO,
                s: S::Base,
                t: FILEPATH,
                sp: SP::None,
            },
            A {
                p: P::Asclet,
                f: F::NEXT,
                s: S::InFile,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::InFile,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(46),
                f: F::PUSH,
                s: S::InFileNext,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(95),
                f: F::NEXT,
                s: S::InFile,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(45),
                f: F::NEXT,
                s: S::InFile,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(47),
                f: F::PUSH,
                s: S::InFileFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::BINGO,
                s: S::Base,
                t: FILEPATH,
                sp: SP::None,
            },
        ],
        /* TPS_InFileNext */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Asclet,
                f: F::CLEAR,
                s: S::InFile,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::CLEAR,
                s: S::InFile,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(95),
                f: F::CLEAR,
                s: S::InFile,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InURLPathFirst */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::UrlChar,
                f: F::NEXT,
                s: S::InURLPath,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InURLPathStart */
        &[A {
            p: P::Any,
            f: F::NEXT,
            s: S::InURLPath,
            t: 0,
            sp: SP::None,
        }],
        /* TPS_InURLPath */
        &[
            A {
                p: P::Eof,
                f: F::BINGO,
                s: S::Base,
                t: URLPATH,
                sp: SP::None,
            },
            A {
                p: P::UrlChar,
                f: F::NEXT,
                s: S::InURLPath,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::BINGO,
                s: S::Base,
                t: URLPATH,
                sp: SP::None,
            },
        ],
        /* TPS_InFURL */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::UrlPath,
                f: F::BINGO | F::CLRALL,
                s: S::Base,
                t: URL_T,
                sp: SP::FURL,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InProtocolFirst */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(47),
                f: F::NEXT,
                s: S::InProtocolSecond,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InProtocolSecond */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(47),
                f: F::NEXT,
                s: S::InProtocolEnd,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InProtocolEnd */
        &[A {
            p: P::Any,
            f: F::BINGO | F::CLRALL,
            s: S::Base,
            t: PROTOCOL,
            sp: SP::None,
        }],
        /* TPS_InHyphenAsciiWordFirst */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Asclet,
                f: F::NEXT,
                s: S::InHyphenAsciiWord,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Alpha,
                f: F::NEXT,
                s: S::InHyphenWord,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::InHyphenDigitLookahead,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InHyphenAsciiWord */
        &[
            A {
                p: P::Eof,
                f: F::BINGO | F::CLRALL,
                s: S::InParseHyphen,
                t: ASCIIHWORD,
                sp: SP::Hyphen,
            },
            A {
                p: P::Asclet,
                f: F::NEXT,
                s: S::InHyphenAsciiWord,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Alpha,
                f: F::NEXT,
                s: S::InHyphenWord,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Special,
                f: F::NEXT,
                s: S::InHyphenWord,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::InHyphenNumWord,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(45),
                f: F::PUSH,
                s: S::InHyphenAsciiWordFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::BINGO | F::CLRALL,
                s: S::InParseHyphen,
                t: ASCIIHWORD,
                sp: SP::Hyphen,
            },
        ],
        /* TPS_InHyphenWordFirst */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Alpha,
                f: F::NEXT,
                s: S::InHyphenWord,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::InHyphenDigitLookahead,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InHyphenWord */
        &[
            A {
                p: P::Eof,
                f: F::BINGO | F::CLRALL,
                s: S::InParseHyphen,
                t: HWORD,
                sp: SP::Hyphen,
            },
            A {
                p: P::Alpha,
                f: F::NEXT,
                s: S::InHyphenWord,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Special,
                f: F::NEXT,
                s: S::InHyphenWord,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::InHyphenNumWord,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(45),
                f: F::PUSH,
                s: S::InHyphenWordFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::BINGO | F::CLRALL,
                s: S::InParseHyphen,
                t: HWORD,
                sp: SP::Hyphen,
            },
        ],
        /* TPS_InHyphenNumWordFirst */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Alpha,
                f: F::NEXT,
                s: S::InHyphenNumWord,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::InHyphenDigitLookahead,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InHyphenNumWord */
        &[
            A {
                p: P::Eof,
                f: F::BINGO | F::CLRALL,
                s: S::InParseHyphen,
                t: NUMHWORD,
                sp: SP::Hyphen,
            },
            A {
                p: P::Alnum,
                f: F::NEXT,
                s: S::InHyphenNumWord,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Special,
                f: F::NEXT,
                s: S::InHyphenNumWord,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(45),
                f: F::PUSH,
                s: S::InHyphenNumWordFirst,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::BINGO | F::CLRALL,
                s: S::InParseHyphen,
                t: NUMHWORD,
                sp: SP::Hyphen,
            },
        ],
        /* TPS_InHyphenDigitLookahead */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::InHyphenDigitLookahead,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Alpha,
                f: F::NEXT,
                s: S::InHyphenNumWord,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Special,
                f: F::NEXT,
                s: S::InHyphenNumWord,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InParseHyphen */
        &[
            A {
                p: P::Eof,
                f: F::RERUN,
                s: S::Base,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Asclet,
                f: F::NEXT,
                s: S::InHyphenAsciiWordPart,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Alpha,
                f: F::NEXT,
                s: S::InHyphenWordPart,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::PUSH,
                s: S::InHyphenUnsignedInt,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Eq(45),
                f: F::PUSH,
                s: S::InParseHyphenHyphen,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::RERUN,
                s: S::Base,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InParseHyphenHyphen */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Alnum,
                f: F::BINGO | F::CLEAR,
                s: S::InParseHyphen,
                t: SPACE,
                sp: SP::None,
            },
            A {
                p: P::Special,
                f: F::BINGO | F::CLEAR,
                s: S::InParseHyphen,
                t: SPACE,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
        /* TPS_InHyphenWordPart */
        &[
            A {
                p: P::Eof,
                f: F::BINGO,
                s: S::Base,
                t: PARTHWORD,
                sp: SP::None,
            },
            A {
                p: P::Alpha,
                f: F::NEXT,
                s: S::InHyphenWordPart,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Special,
                f: F::NEXT,
                s: S::InHyphenWordPart,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::InHyphenNumWordPart,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::BINGO,
                s: S::InParseHyphen,
                t: PARTHWORD,
                sp: SP::None,
            },
        ],
        /* TPS_InHyphenAsciiWordPart */
        &[
            A {
                p: P::Eof,
                f: F::BINGO,
                s: S::Base,
                t: ASCIIPARTHWORD,
                sp: SP::None,
            },
            A {
                p: P::Asclet,
                f: F::NEXT,
                s: S::InHyphenAsciiWordPart,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Alpha,
                f: F::NEXT,
                s: S::InHyphenWordPart,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Special,
                f: F::NEXT,
                s: S::InHyphenWordPart,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::InHyphenNumWordPart,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::BINGO,
                s: S::InParseHyphen,
                t: ASCIIPARTHWORD,
                sp: SP::None,
            },
        ],
        /* TPS_InHyphenNumWordPart */
        &[
            A {
                p: P::Eof,
                f: F::BINGO,
                s: S::Base,
                t: NUMPARTHWORD,
                sp: SP::None,
            },
            A {
                p: P::Alnum,
                f: F::NEXT,
                s: S::InHyphenNumWordPart,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Special,
                f: F::NEXT,
                s: S::InHyphenNumWordPart,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::BINGO,
                s: S::InParseHyphen,
                t: NUMPARTHWORD,
                sp: SP::None,
            },
        ],
        /* TPS_InHyphenUnsignedInt */
        &[
            A {
                p: P::Eof,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Digit,
                f: F::NEXT,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Alpha,
                f: F::CLEAR,
                s: S::InHyphenNumWordPart,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Special,
                f: F::CLEAR,
                s: S::InHyphenNumWordPart,
                t: 0,
                sp: SP::None,
            },
            A {
                p: P::Any,
                f: F::POP,
                s: S::Null,
                t: 0,
                sp: SP::None,
            },
        ],
    ];
    ACTIONS[state as usize]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Unescapes a corpus field (`\\`, `\t`, `\n`).
    fn unescape(s: &str) -> String {
        let mut out = String::new();
        let mut chars = s.chars();
        while let Some(c) = chars.next() {
            if c != '\\' {
                out.push(c);
                continue;
            }
            match chars.next() {
                Some('t') => out.push('\t'),
                Some('n') => out.push('\n'),
                Some(other) => out.push(other),
                None => break,
            }
        }
        out
    }

    #[test]
    fn the_default_parser_matches_postgresql() {
        let corpus = include_str!("ts_parse_corpus.tsv");
        let mut checked = 0;
        for line in corpus.lines() {
            let mut fields = line.split('\t');
            let text = unescape(fields.next().expect("a text field"));
            let expected: Vec<(u8, String)> = fields
                .map(|field| {
                    let (tokid, token) = field.split_once(':').expect("tokid:token");
                    (tokid.parse().expect("a token id"), unescape(token))
                })
                .collect();
            let found: Vec<(u8, String)> =
                parse(&text).into_iter().map(|t| (t.ty, t.text)).collect();
            assert_eq!(found, expected, "parsing {text:?}");
            checked += 1;
        }
        assert!(checked > 70, "the corpus should not shrink");
    }
}
