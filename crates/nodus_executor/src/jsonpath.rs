//! PostgreSQL's SQL/JSON path language (`jsonpath`): the literal type with its
//! canonical form, the parser and printer for the path syntax, and the
//! evaluator behind the `@?`/`@@` operators and the `jsonb_path_*` functions.
//!
//! A jsonpath value is kept as its canonical text, like ranges and geometric
//! types; the declared type makes the text a path. The evaluator walks
//! `serde_json` values following PostgreSQL's lax/strict rules: in lax mode
//! (the default) an operation applied to an array is applied to its elements,
//! and structural mismatches produce nothing instead of failing; strict mode
//! reports them.

use crate::numeric::Numeric;
use serde_json::Value as J;

/// Whether a declared type names the `jsonpath` type.
pub(crate) fn is_type(data_type: &str) -> bool {
    let text = data_type.trim();
    let text = text
        .strip_prefix("pg_catalog.")
        .or_else(|| text.strip_prefix("PG_CATALOG."))
        .unwrap_or(text);
    text.eq_ignore_ascii_case("jsonpath")
}

/// A failure during parsing or evaluation: the message (with its fields),
/// whether it is a structural one (which lax mode and `exists()` treat as an
/// absent item rather than an error), and whether it is fatal — never
/// suppressed, even by `silent` (a missing jsonpath variable is).
pub(crate) struct Error {
    pub text: String,
    structural: bool,
    fatal: bool,
}

impl Error {
    fn structural(message: impl Into<String>) -> Error {
        Error {
            text: message.into(),
            structural: true,
            fatal: false,
        }
    }

    fn runtime(message: impl Into<String>) -> Error {
        Error {
            text: message.into(),
            structural: false,
            fatal: false,
        }
    }

    fn fatal(message: impl Into<String>) -> Error {
        Error {
            text: message.into(),
            structural: false,
            fatal: true,
        }
    }
}

type Result<T> = std::result::Result<T, Error>;

fn db(message: impl Into<String>, code: &str) -> String {
    crate::error_fields::DbError::new(message)
        .code(code)
        .into_text()
}

// ---------------------------------------------------------------- the AST

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Mode {
    Lax,
    Strict,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Path {
    pub mode: Mode,
    pub expr: Expr,
}

/// Where a path starts: the document (`$`), the current item (`@`), or a
/// jsonpath variable (`$name`, whose value comes from the `vars` object).
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PathBase {
    Root,
    Current,
    Var(String),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Expr {
    /// `$`, `@`, or a variable, followed by steps.
    Path {
        base: PathBase,
        steps: Vec<Step>,
    },
    Compare(CmpOp, Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    Arith(ArithOp, Box<Expr>, Box<Expr>),
    Neg(Box<Expr>),
    Exists(Box<Expr>),
    LikeRegex {
        operand: Box<Expr>,
        pattern: String,
        flags: String,
    },
    StartsWith {
        operand: Box<Expr>,
        prefix: String,
    },
    /// A method applied to a parenthesized expression or variable:
    /// `($.a + 1).type()`, `$x.size()`.
    MethodCall {
        method: Method,
        operand: Box<Expr>,
    },
    Literal(Lit),
    Var(String),
    /// `(expr)`: kept so the printer drops the parens.
    Nested(Box<Expr>),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum CmpOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl CmpOp {
    fn symbol(self) -> &'static str {
        match self {
            CmpOp::Eq => "==",
            CmpOp::NotEq => "!=",
            CmpOp::Lt => "<",
            CmpOp::LtEq => "<=",
            CmpOp::Gt => ">",
            CmpOp::GtEq => ">=",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

impl ArithOp {
    fn symbol(self) -> &'static str {
        match self {
            ArithOp::Add => "+",
            ArithOp::Sub => "-",
            ArithOp::Mul => "*",
            ArithOp::Div => "/",
            ArithOp::Mod => "%",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Lit {
    Num(Numeric),
    Str(String),
    Bool(bool),
    Null,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Step {
    Key(String),
    AnyKey,
    /// `[n]`, `[last]`, `[last - n]`.
    Index(Index),
    /// `[a to b]`.
    Slice(Index, Index),
    AnyIndex,
    Filter(Expr),
    Method(Method),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Index {
    Num(i64),
    /// `last - n` (`last + n` is stored as a negative offset).
    Last(i64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Method {
    Type,
    Size,
    Double,
    Floor,
    Ceiling,
    Abs,
    Integer,
    Number,
    String,
    Boolean,
}

impl Method {
    fn name(self) -> &'static str {
        match self {
            Method::Type => "type",
            Method::Size => "size",
            Method::Double => "double",
            Method::Floor => "floor",
            Method::Ceiling => "ceiling",
            Method::Abs => "abs",
            Method::Integer => "integer",
            Method::Number => "number",
            Method::String => "string",
            Method::Boolean => "boolean",
        }
    }
}

// ---------------------------------------------------------------- lexer

#[derive(Clone, Debug, PartialEq)]
enum Tok {
    /// A number, in its exact decimal text.
    Num(String),
    /// A double-quoted string, unescaped.
    Str(String),
    /// A bare identifier (member name, `last`, `true`, ...).
    Ident(String),
    /// `$name`: a variable (quoted names keep their text here).
    Var(String),
    Dollar,
    At,
    Dot,
    Star,
    LBracket,
    RBracket,
    LParen,
    RParen,
    Question,
    To,
    EqEq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Plus,
    Minus,
    Slash,
    Percent,
    AndAnd,
    OrOr,
    Bang,
    Eof,
}

impl Tok {
    fn describe(&self) -> String {
        match self {
            Tok::Num(n) => n.clone(),
            Tok::Str(s) => format!("\"{s}\""),
            Tok::Ident(s) => s.clone(),
            Tok::Var(v) => format!("${v}"),
            Tok::Dollar => "$".into(),
            Tok::At => "@".into(),
            Tok::Dot => ".".into(),
            Tok::Star => "*".into(),
            Tok::LBracket => "[".into(),
            Tok::RBracket => "]".into(),
            Tok::LParen => "(".into(),
            Tok::RParen => ")".into(),
            Tok::Question => "?".into(),
            Tok::To => "to".into(),
            Tok::EqEq => "==".into(),
            Tok::NotEq => "!=".into(),
            Tok::Lt => "<".into(),
            Tok::LtEq => "<=".into(),
            Tok::Gt => ">".into(),
            Tok::GtEq => ">=".into(),
            Tok::Plus => "+".into(),
            Tok::Minus => "-".into(),
            Tok::Slash => "/".into(),
            Tok::Percent => "%".into(),
            Tok::AndAnd => "&&".into(),
            Tok::OrOr => "||".into(),
            Tok::Bang => "!".into(),
            Tok::Eof => "end".into(),
        }
    }
}

struct Lexer {
    chars: Vec<char>,
    pos: usize,
}

impl Lexer {
    fn new(text: &str) -> Lexer {
        Lexer {
            chars: text.chars().collect(),
            pos: 0,
        }
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn peek_at(&self, offset: usize) -> Option<char> {
        self.chars.get(self.pos + offset).copied()
    }

    fn bump(&mut self) -> Option<char> {
        let c = self.peek();
        if c.is_some() {
            self.pos += 1;
        }
        c
    }

    fn skip_space(&mut self) {
        loop {
            match self.peek() {
                Some(c) if c.is_whitespace() => {
                    self.pos += 1;
                }
                Some('/') if self.peek_at(1) == Some('*') => {
                    self.pos += 2;
                    while self.peek().is_some()
                        && !(self.peek() == Some('*') && self.peek_at(1) == Some('/'))
                    {
                        self.pos += 1;
                    }
                    self.pos = (self.pos + 2).min(self.chars.len());
                }
                _ => break,
            }
        }
    }

    /// A double-quoted string with `\` escapes; `\"` is `"`, `\\` is `\`, and
    /// any other backslash is kept literally (PostgreSQL's rule).
    fn quoted(&mut self) -> Result<String> {
        self.bump();
        let mut out = String::new();
        loop {
            match self.bump() {
                None => return Err(syntax_at_end()),
                Some('"') => return Ok(out),
                Some('\\') => match self.bump() {
                    None => return Err(syntax_at_end()),
                    Some('"') => out.push('"'),
                    Some('\\') => out.push('\\'),
                    Some(other) => {
                        out.push('\\');
                        out.push(other);
                    }
                },
                Some(c) => out.push(c),
            }
        }
    }

    fn number(&mut self) -> String {
        let start = self.pos;
        while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
            self.pos += 1;
        }
        if self.peek() == Some('.') && matches!(self.peek_at(1), Some(c) if c.is_ascii_digit()) {
            self.pos += 1;
            while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                self.pos += 1;
            }
        }
        if matches!(self.peek(), Some('e' | 'E')) {
            let save = self.pos;
            self.pos += 1;
            if matches!(self.peek(), Some('+' | '-')) {
                self.pos += 1;
            }
            if matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                    self.pos += 1;
                }
            } else {
                self.pos = save;
            }
        }
        self.chars[start..self.pos].iter().collect()
    }

    fn token(&mut self) -> Result<Tok> {
        self.skip_space();
        let Some(c) = self.peek() else {
            return Ok(Tok::Eof);
        };
        match c {
            '$' => {
                self.pos += 1;
                if self.peek() == Some('"') {
                    let name = self.quoted()?;
                    return Ok(Tok::Var(name));
                }
                if matches!(self.peek(), Some(c) if c.is_alphabetic() || c == '_') {
                    let start = self.pos;
                    while matches!(self.peek(), Some(c) if c.is_alphanumeric() || c == '_') {
                        self.pos += 1;
                    }
                    return Ok(Tok::Var(self.chars[start..self.pos].iter().collect()));
                }
                Ok(Tok::Dollar)
            }
            '@' => {
                self.pos += 1;
                Ok(Tok::At)
            }
            '"' => Ok(Tok::Str(self.quoted()?)),
            '.' => {
                self.pos += 1;
                Ok(Tok::Dot)
            }
            '*' => {
                self.pos += 1;
                Ok(Tok::Star)
            }
            '[' => {
                self.pos += 1;
                Ok(Tok::LBracket)
            }
            ']' => {
                self.pos += 1;
                Ok(Tok::RBracket)
            }
            '(' => {
                self.pos += 1;
                Ok(Tok::LParen)
            }
            ')' => {
                self.pos += 1;
                Ok(Tok::RParen)
            }
            '?' => {
                self.pos += 1;
                Ok(Tok::Question)
            }
            '=' => {
                self.pos += 1;
                if self.peek() == Some('=') {
                    self.pos += 1;
                    Ok(Tok::EqEq)
                } else {
                    Err(syntax_near("="))
                }
            }
            '!' => {
                self.pos += 1;
                if self.peek() == Some('=') {
                    self.pos += 1;
                    Ok(Tok::NotEq)
                } else {
                    Ok(Tok::Bang)
                }
            }
            '<' => {
                self.pos += 1;
                match self.peek() {
                    Some('=') => {
                        self.pos += 1;
                        Ok(Tok::LtEq)
                    }
                    Some('>') => {
                        self.pos += 1;
                        Ok(Tok::NotEq)
                    }
                    _ => Ok(Tok::Lt),
                }
            }
            '>' => {
                self.pos += 1;
                if self.peek() == Some('=') {
                    self.pos += 1;
                    Ok(Tok::GtEq)
                } else {
                    Ok(Tok::Gt)
                }
            }
            '+' => {
                self.pos += 1;
                Ok(Tok::Plus)
            }
            '-' => {
                self.pos += 1;
                Ok(Tok::Minus)
            }
            '/' => {
                self.pos += 1;
                Ok(Tok::Slash)
            }
            '%' => {
                self.pos += 1;
                Ok(Tok::Percent)
            }
            '&' => {
                self.pos += 1;
                if self.peek() == Some('&') {
                    self.pos += 1;
                    Ok(Tok::AndAnd)
                } else {
                    Err(syntax_near("&"))
                }
            }
            '|' => {
                self.pos += 1;
                if self.peek() == Some('|') {
                    self.pos += 1;
                    Ok(Tok::OrOr)
                } else {
                    Err(syntax_near("|"))
                }
            }
            c if c.is_ascii_digit() => Ok(Tok::Num(self.number())),
            c if c.is_alphabetic() || c == '_' => {
                let start = self.pos;
                while matches!(self.peek(), Some(c) if c.is_alphanumeric() || c == '_') {
                    self.pos += 1;
                }
                let word: String = self.chars[start..self.pos].iter().collect();
                Ok(if word == "to" {
                    Tok::To
                } else {
                    Tok::Ident(word)
                })
            }
            other => Err(syntax_near(&other.to_string())),
        }
    }
}

fn syntax_near(token: &str) -> Error {
    Error::runtime(db(
        format!("syntax error at or near \"{token}\" of jsonpath input"),
        "42601",
    ))
}

fn syntax_at_end() -> Error {
    Error::runtime(db("syntax error at end of jsonpath input", "42601"))
}

// ---------------------------------------------------------------- parser

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> &Tok {
        self.toks.get(self.pos).unwrap_or(&Tok::Eof)
    }

    fn bump(&mut self) -> Tok {
        let tok = self.peek().clone();
        if self.pos < self.toks.len() {
            self.pos += 1;
        }
        tok
    }

    fn eat(&mut self, want: &Tok) -> bool {
        if self.peek() == want {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, want: &Tok) -> Result<()> {
        if self.eat(want) {
            Ok(())
        } else {
            Err(self.syntax_here())
        }
    }

    fn syntax_here(&self) -> Error {
        match self.peek() {
            Tok::Eof => syntax_at_end(),
            tok => syntax_near(&tok.describe()),
        }
    }

    /// `or` := `and` (`||` `and`)*
    fn or_expr(&mut self) -> Result<Expr> {
        let mut left = self.and_expr()?;
        while self.peek() == &Tok::OrOr {
            if !is_predicate(&left) {
                return Err(syntax_near("||"));
            }
            self.bump();
            let right = self.and_expr()?;
            if !is_predicate(&right) {
                return Err(self.syntax_here());
            }
            left = Expr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    /// `and` := `not` (`&&` `not`)*
    fn and_expr(&mut self) -> Result<Expr> {
        let mut left = self.not_expr()?;
        while self.peek() == &Tok::AndAnd {
            if !is_predicate(&left) {
                return Err(syntax_near("&&"));
            }
            self.bump();
            let right = self.not_expr()?;
            if !is_predicate(&right) {
                return Err(self.syntax_here());
            }
            left = Expr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    /// `not` := `!` comparison | comparison
    fn not_expr(&mut self) -> Result<Expr> {
        if self.eat(&Tok::Bang) {
            let inner = self.comparison()?;
            if !is_predicate(&inner) {
                return Err(self.syntax_here());
            }
            return Ok(Expr::Not(Box::new(inner)));
        }
        self.comparison()
    }

    /// `comparison` := `addsub` [ comparison | `like_regex` | `starts with` ]
    fn comparison(&mut self) -> Result<Expr> {
        let left = self.addsub()?;
        if let Tok::Ident(word) = self.peek().clone() {
            match word.as_str() {
                "like_regex" => {
                    self.bump();
                    let pattern = match self.peek().clone() {
                        Tok::Str(s) => {
                            self.bump();
                            s
                        }
                        _ => return Err(self.syntax_here()),
                    };
                    let mut flags = String::new();
                    if let Tok::Ident(next) = self.peek().clone()
                        && next.eq_ignore_ascii_case("flag")
                    {
                        self.bump();
                        match self.peek().clone() {
                            Tok::Str(f) => {
                                self.bump();
                                flags = f;
                            }
                            _ => return Err(self.syntax_here()),
                        }
                    }
                    check_like_regex(&pattern, &flags)?;
                    return Ok(Expr::LikeRegex {
                        operand: Box::new(left),
                        pattern,
                        flags,
                    });
                }
                "starts" => {
                    self.bump();
                    match self.peek().clone() {
                        Tok::Ident(with) if with.eq_ignore_ascii_case("with") => {
                            self.bump();
                        }
                        _ => return Err(self.syntax_here()),
                    }
                    let prefix = match self.peek().clone() {
                        Tok::Str(s) => {
                            self.bump();
                            s
                        }
                        _ => return Err(self.syntax_here()),
                    };
                    return Ok(Expr::StartsWith {
                        operand: Box::new(left),
                        prefix,
                    });
                }
                _ => {}
            }
        }
        let op = match self.peek() {
            Tok::EqEq => CmpOp::Eq,
            Tok::NotEq => CmpOp::NotEq,
            Tok::Lt => CmpOp::Lt,
            Tok::LtEq => CmpOp::LtEq,
            Tok::Gt => CmpOp::Gt,
            Tok::GtEq => CmpOp::GtEq,
            _ => return Ok(left),
        };
        // A parenthesized predicate is not a comparison operand
        // (`($.a == 1) == true` is a syntax error in PostgreSQL).
        if nested_predicate(&left) {
            let symbol = op.symbol();
            return Err(syntax_near(symbol));
        }
        self.bump();
        let right = self.addsub()?;
        if nested_predicate(&right) {
            return Err(self.syntax_here());
        }
        Ok(Expr::Compare(op, Box::new(left), Box::new(right)))
    }

    /// `addsub` := `muldiv` (('+'|'-') `muldiv`)*
    fn addsub(&mut self) -> Result<Expr> {
        let mut left = self.muldiv()?;
        loop {
            let op = match self.peek() {
                Tok::Plus => ArithOp::Add,
                Tok::Minus => ArithOp::Sub,
                _ => return Ok(left),
            };
            self.bump();
            let right = self.muldiv()?;
            left = Expr::Arith(op, Box::new(left), Box::new(right));
        }
    }

    /// `muldiv` := `unary` (('*'|'/'|'%') `unary`)*
    fn muldiv(&mut self) -> Result<Expr> {
        let mut left = self.unary()?;
        loop {
            let op = match self.peek() {
                Tok::Star => ArithOp::Mul,
                Tok::Slash => ArithOp::Div,
                Tok::Percent => ArithOp::Mod,
                _ => return Ok(left),
            };
            self.bump();
            let right = self.unary()?;
            left = Expr::Arith(op, Box::new(left), Box::new(right));
        }
    }

    /// `unary` := '-' `unary` | `primary`
    fn unary(&mut self) -> Result<Expr> {
        if self.eat(&Tok::Minus) {
            let inner = self.unary()?;
            return Ok(Expr::Neg(Box::new(inner)));
        }
        self.primary()
    }

    fn primary(&mut self) -> Result<Expr> {
        match self.peek().clone() {
            Tok::Num(text) => {
                self.bump();
                Ok(Expr::Literal(Lit::Num(number_literal(&text)?)))
            }
            Tok::Str(s) => {
                self.bump();
                Ok(Expr::Literal(Lit::Str(s)))
            }
            Tok::Var(name) => {
                self.bump();
                // Accessors continue a variable as a path root; methods and
                // a bare variable are value expressions.
                if matches!(self.peek(), Tok::Dot | Tok::LBracket | Tok::Question) {
                    let steps = self.steps()?;
                    return Ok(Expr::Path {
                        base: PathBase::Var(name),
                        steps,
                    });
                }
                let mut expr = Expr::Var(name);
                while let Some(method) = self.method_suffix()? {
                    expr = Expr::MethodCall {
                        method,
                        operand: Box::new(expr),
                    };
                }
                Ok(expr)
            }
            Tok::LParen => {
                self.bump();
                let inner = self.or_expr()?;
                self.expect(&Tok::RParen)?;
                let mut expr = Expr::Nested(Box::new(inner));
                while let Some(method) = self.method_suffix()? {
                    expr = Expr::MethodCall {
                        method,
                        operand: Box::new(expr),
                    };
                }
                Ok(expr)
            }
            Tok::Dollar | Tok::At => self.path_expr(),
            Tok::Ident(word) => match word.as_str() {
                "true" => {
                    self.bump();
                    Ok(Expr::Literal(Lit::Bool(true)))
                }
                "false" => {
                    self.bump();
                    Ok(Expr::Literal(Lit::Bool(false)))
                }
                "null" => {
                    self.bump();
                    Ok(Expr::Literal(Lit::Null))
                }
                "exists" => {
                    self.bump();
                    self.expect(&Tok::LParen)?;
                    let inner = self.primary()?;
                    self.expect(&Tok::RParen)?;
                    Ok(Expr::Exists(Box::new(inner)))
                }
                // A bare word is not a path: PostgreSQL reports the end of
                // the input when nothing follows, or the following character.
                other => {
                    let _ = other;
                    if self.toks.get(self.pos + 1) == Some(&Tok::Eof) {
                        Err(syntax_at_end())
                    } else {
                        Err(syntax_near(" "))
                    }
                }
            },
            _ => Err(self.syntax_here()),
        }
    }

    /// `$`/`@` followed by steps; a filter's predicate is a full expression.
    fn path_expr(&mut self) -> Result<Expr> {
        let base = match self.peek() {
            Tok::Dollar => PathBase::Root,
            Tok::At => PathBase::Current,
            _ => return Err(self.syntax_here()),
        };
        self.bump();
        let steps = self.steps()?;
        Ok(Expr::Path { base, steps })
    }

    /// The accessors of a path: `.key`, `[index]`, `? (predicate)`, and the
    /// like, until one that is not.
    fn steps(&mut self) -> Result<Vec<Step>> {
        let mut steps: Vec<Step> = Vec::new();
        loop {
            match self.peek().clone() {
                Tok::Dot => {
                    self.bump();
                    match self.peek().clone() {
                        Tok::Ident(name) => {
                            self.bump();
                            let method = match name.as_str() {
                                "bigint" | "decimal" | "date" | "time" | "time_tz"
                                | "timestamp" | "timestamp_tz" | "datetime" => {
                                    return Err(Error::runtime(db(
                                        format!("jsonpath item method .{name}() is not supported"),
                                        "0A000",
                                    )));
                                }
                                _ => method_named(&name),
                            };
                            match method {
                                Some(method) => {
                                    self.expect(&Tok::LParen)?;
                                    self.expect(&Tok::RParen)?;
                                    steps.push(Step::Method(method));
                                }
                                None => steps.push(Step::Key(name)),
                            }
                        }
                        Tok::Str(name) => {
                            self.bump();
                            steps.push(Step::Key(name));
                        }
                        Tok::Star => {
                            self.bump();
                            steps.push(Step::AnyKey);
                        }
                        other => {
                            return Err(match other {
                                Tok::Eof => syntax_at_end(),
                                tok => syntax_near(&tok.describe()),
                            });
                        }
                    }
                }
                Tok::LBracket => {
                    self.bump();
                    steps.push(self.bracket_step()?);
                }
                Tok::Question => {
                    self.bump();
                    self.expect(&Tok::LParen)?;
                    let predicate = self.or_expr()?;
                    if !is_predicate(&predicate) {
                        return Err(self.syntax_here());
                    }
                    self.expect(&Tok::RParen)?;
                    steps.push(Step::Filter(predicate));
                }
                _ => break,
            }
        }
        Ok(steps)
    }

    /// A `.name()` method suffix, when one follows.
    fn method_suffix(&mut self) -> Result<Option<Method>> {
        if self.peek() != &Tok::Dot {
            return Ok(None);
        }
        self.bump();
        let name = match self.peek().clone() {
            Tok::Ident(name) => {
                self.bump();
                name
            }
            _ => return Err(self.syntax_here()),
        };
        let method = method_named(&name).ok_or_else(|| syntax_near(&name))?;
        self.expect(&Tok::LParen)?;
        self.expect(&Tok::RParen)?;
        Ok(Some(method))
    }

    fn bracket_step(&mut self) -> Result<Step> {
        if self.eat(&Tok::Star) {
            self.expect(&Tok::RBracket)?;
            return Ok(Step::AnyIndex);
        }
        let first = self.index_value()?;
        if self.eat(&Tok::To) {
            let second = self.index_value()?;
            self.expect(&Tok::RBracket)?;
            return Ok(Step::Slice(first, second));
        }
        self.expect(&Tok::RBracket)?;
        Ok(Step::Index(first))
    }

    /// `n`, `-n`, `last`, `last - n`, `last + n`.
    fn index_value(&mut self) -> Result<Index> {
        let negative = self.eat(&Tok::Minus);
        match self.peek().clone() {
            Tok::Num(text) => {
                self.bump();
                let n: i64 = text.parse().map_err(|_| syntax_near(&text))?;
                Ok(Index::Num(if negative { -n } else { n }))
            }
            Tok::Ident(word) if !negative && word.eq_ignore_ascii_case("last") => {
                self.bump();
                match self.peek().clone() {
                    Tok::Minus => {
                        self.bump();
                        match self.peek().clone() {
                            Tok::Num(text) => {
                                self.bump();
                                let n: i64 = text.parse().map_err(|_| syntax_near(&text))?;
                                Ok(Index::Last(n))
                            }
                            _ => Err(self.syntax_here()),
                        }
                    }
                    Tok::Plus => {
                        self.bump();
                        match self.peek().clone() {
                            Tok::Num(text) => {
                                self.bump();
                                let n: i64 = text.parse().map_err(|_| syntax_near(&text))?;
                                Ok(Index::Last(-n))
                            }
                            _ => Err(self.syntax_here()),
                        }
                    }
                    _ => Ok(Index::Last(0)),
                }
            }
            _ => Err(self.syntax_here()),
        }
    }
}

/// Whether an expression may stand as a predicate (a filter body or an
/// operand of `&&`/`||`/`!`). PostgreSQL rejects bare paths, literals, and
/// arithmetic there.
fn is_predicate(expr: &Expr) -> bool {
    match expr {
        Expr::Compare(..)
        | Expr::And(..)
        | Expr::Or(..)
        | Expr::Not(..)
        | Expr::Exists(..)
        | Expr::LikeRegex { .. }
        | Expr::StartsWith { .. } => true,
        Expr::Nested(inner) => is_predicate(inner),
        _ => false,
    }
}

/// Whether an expression is a parenthesized predicate, which PostgreSQL
/// does not accept where a value is expected.
fn nested_predicate(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Nested(inner)
            if matches!(
                **inner,
                Expr::Compare(..) | Expr::And(..) | Expr::Or(..) | Expr::Not(..)
            )
    )
}

/// The method a name spells, when PostgreSQL has one NodusDB implements.
fn method_named(name: &str) -> Option<Method> {
    Some(match name {
        "type" => Method::Type,
        "size" => Method::Size,
        "double" => Method::Double,
        "floor" => Method::Floor,
        "ceiling" => Method::Ceiling,
        "abs" => Method::Abs,
        "integer" => Method::Integer,
        "number" => Method::Number,
        "string" => Method::String,
        "boolean" => Method::Boolean,
        _ => return None,
    })
}

/// Validates a `like_regex` predicate's flags and pattern at parse time, as
/// PostgreSQL does: the `x` flag is not implemented, other unknown flags are
/// rejected, and the pattern must compile.
fn check_like_regex(pattern: &str, flags: &str) -> Result<()> {
    let mut regex_flags = String::new();
    for flag in flags.chars() {
        match flag {
            'i' | 's' | 'm' | 'q' => regex_flags.push(flag),
            'x' => {
                return Err(Error::runtime(db(
                    "XQuery \"x\" flag (expanded regular expressions) is not implemented",
                    "0A000",
                )));
            }
            other => {
                let error =
                    crate::error_fields::DbError::new("invalid input syntax for type jsonpath")
                        .code("42601")
                        .detail(format!(
                            "Unrecognized flag character \"{other}\" in LIKE_REGEX predicate."
                        ))
                        .into_text();
                return Err(Error::runtime(error));
            }
        }
    }
    crate::pg_regex::compile(pattern, &regex_flags)
        .map(|_| ())
        .map_err(|e| Error::runtime(db(format!("invalid regular expression: {e}"), "2201B")))
}

fn number_literal(text: &str) -> Result<Numeric> {
    Numeric::parse(text).map_err(|_| syntax_near(text))
}

/// Parses a jsonpath literal. `Err` is PostgreSQL's message.
pub(crate) fn parse(text: &str) -> Result<Path> {
    let mut lexer = Lexer::new(text);
    let mut toks = Vec::new();
    loop {
        let tok = lexer.token()?;
        let done = tok == Tok::Eof;
        toks.push(tok);
        if done {
            break;
        }
    }
    let mut parser = Parser { toks, pos: 0 };
    let mode = match parser.peek().clone() {
        Tok::Ident(word) if word.eq_ignore_ascii_case("strict") => {
            parser.bump();
            Mode::Strict
        }
        Tok::Ident(word) if word.eq_ignore_ascii_case("lax") => {
            parser.bump();
            Mode::Lax
        }
        _ => Mode::Lax,
    };
    let expr = parser.or_expr()?;
    if parser.peek() != &Tok::Eof {
        return Err(parser.syntax_here());
    }
    if uses_current_at_root(&expr) {
        return Err(Error::runtime(db(
            "@ is not allowed in root expressions",
            "42601",
        )));
    }
    Ok(Path { mode, expr })
}

/// Whether `@` appears outside a filter; PostgreSQL forbids it in the root
/// expression. Inside a filter's predicate `@` is the current item.
fn uses_current_at_root(expr: &Expr) -> bool {
    match expr {
        Expr::Path { base, steps } => {
            matches!(base, PathBase::Current) || steps.iter().any(uses_at_in_step)
        }
        Expr::Compare(_, l, r) | Expr::And(l, r) | Expr::Or(l, r) | Expr::Arith(_, l, r) => {
            uses_current_at_root(l) || uses_current_at_root(r)
        }
        Expr::Not(e)
        | Expr::Neg(e)
        | Expr::Exists(e)
        | Expr::LikeRegex { operand: e, .. }
        | Expr::StartsWith { operand: e, .. }
        | Expr::MethodCall { operand: e, .. }
        | Expr::Nested(e) => uses_current_at_root(e),
        Expr::Literal(_) | Expr::Var(_) => false,
    }
}

fn uses_at_in_step(step: &Step) -> bool {
    match step {
        Step::Filter(pred) => uses_current_at_filter(pred),
        _ => false,
    }
}

/// `@` inside a filter is allowed anywhere.
fn uses_current_at_filter(_expr: &Expr) -> bool {
    false
}

// ---------------------------------------------------------------- printer

fn escape_string(text: &str, out: &mut String) {
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            other => out.push(other),
        }
    }
    out.push('"');
}

/// The canonical text of a parsed path, as PostgreSQL's `jsonpath` output
/// writes it.
pub(crate) fn print(path: &Path) -> String {
    let mut out = String::new();
    if path.mode == Mode::Strict {
        out.push_str("strict ");
    }
    // A root expression that is not a path or a value is parenthesized
    // (`($."a" + 1)`, `($."a" == 1)`).
    fn root_wrapped(expr: &Expr) -> bool {
        match expr {
            Expr::Nested(inner) => root_wrapped(inner),
            Expr::Arith(..)
            | Expr::Compare(..)
            | Expr::And(..)
            | Expr::Or(..)
            | Expr::LikeRegex { .. }
            | Expr::StartsWith { .. } => true,
            _ => false,
        }
    }
    let wrap = root_wrapped(&path.expr);
    if wrap {
        out.push('(');
    }
    write_expr(&path.expr, 0, &mut out);
    if wrap {
        out.push(')');
    }
    out
}

/// Precedence: `||` 1, `&&` 2, `!`/`exists` 3, comparison 4, `+`/`-` 5,
/// `*`/`/`/`%` 6, unary `-` 7.
fn precedence(expr: &Expr) -> u8 {
    match expr {
        Expr::Or(..) => 1,
        Expr::And(..) => 2,
        Expr::Not(..) | Expr::Exists(..) => 3,
        Expr::Compare(..) | Expr::LikeRegex { .. } | Expr::StartsWith { .. } => 4,
        Expr::Arith(ArithOp::Add | ArithOp::Sub, ..) => 5,
        Expr::Arith(..) => 6,
        Expr::Neg(..) => 7,
        _ => 8,
    }
}

/// Whether an expression prints without parentheses before a method suffix.
fn method_operand_simple(expr: &Expr) -> bool {
    match expr {
        Expr::Nested(inner) => method_operand_simple(inner),
        Expr::Path { .. } | Expr::Var(_) | Expr::MethodCall { .. } => true,
        _ => false,
    }
}

fn write_expr(expr: &Expr, parent: u8, out: &mut String) {
    // Explicit parens are transparent: precedence decides where they print.
    if let Expr::Nested(inner) = expr {
        write_expr(inner, parent, out);
        return;
    }
    let prec = precedence(expr);
    let wrap = prec < parent;
    if wrap {
        out.push('(');
    }
    match expr {
        Expr::Path { base, steps } => {
            match base {
                PathBase::Root => out.push('$'),
                PathBase::Current => out.push('@'),
                PathBase::Var(name) => {
                    out.push('$');
                    escape_string(name, out);
                }
            }
            for step in steps {
                write_step(step, out);
            }
        }
        Expr::Compare(op, l, r) => {
            write_expr(l, prec, out);
            out.push(' ');
            out.push_str(op.symbol());
            out.push(' ');
            write_expr(r, prec + 1, out);
        }
        Expr::And(l, r) => {
            write_expr(l, prec, out);
            out.push_str(" && ");
            write_expr(r, prec + 1, out);
        }
        Expr::Or(l, r) => {
            write_expr(l, prec, out);
            out.push_str(" || ");
            write_expr(r, prec + 1, out);
        }
        Expr::Not(inner) => {
            out.push('!');
            // PostgreSQL always parenthesizes the negated predicate.
            out.push('(');
            write_expr(inner, 0, out);
            out.push(')');
        }
        Expr::Neg(inner) => {
            out.push('-');
            write_expr(inner, prec, out);
        }
        Expr::Exists(inner) => {
            out.push_str("exists (");
            write_expr(inner, 0, out);
            out.push(')');
        }
        Expr::LikeRegex {
            operand,
            pattern,
            flags,
        } => {
            write_expr(operand, prec, out);
            out.push_str(" like_regex ");
            escape_string(pattern, out);
            if !flags.is_empty() {
                out.push_str(" flag ");
                escape_string(flags, out);
            }
        }
        Expr::StartsWith { operand, prefix } => {
            write_expr(operand, prec, out);
            out.push_str(" starts with ");
            escape_string(prefix, out);
        }
        Expr::Arith(op, l, r) => {
            write_expr(l, prec, out);
            out.push(' ');
            out.push_str(op.symbol());
            out.push(' ');
            write_expr(r, prec + 1, out);
        }
        Expr::MethodCall { method, operand } => {
            if method_operand_simple(operand) {
                write_expr(operand, 0, out);
            } else {
                out.push('(');
                write_expr(operand, 0, out);
                out.push(')');
            }
            out.push('.');
            out.push_str(method.name());
            out.push_str("()");
        }
        Expr::Literal(lit) => match lit {
            Lit::Num(n) => out.push_str(&n.to_string()),
            Lit::Str(s) => escape_string(s, out),
            Lit::Bool(true) => out.push_str("true"),
            Lit::Bool(false) => out.push_str("false"),
            Lit::Null => out.push_str("null"),
        },
        Expr::Var(name) => {
            out.push('$');
            escape_string(name, out);
        }
        Expr::Nested(inner) => write_expr(inner, 0, out),
    }
    if wrap {
        out.push(')');
    }
}

fn write_step(step: &Step, out: &mut String) {
    match step {
        Step::Key(name) => {
            out.push('.');
            escape_string(name, out);
        }
        Step::AnyKey => out.push_str(".*"),
        Step::Index(index) => {
            out.push('[');
            write_index(*index, out);
            out.push(']');
        }
        Step::Slice(a, b) => {
            out.push('[');
            write_index(*a, out);
            out.push_str(" to ");
            write_index(*b, out);
            out.push(']');
        }
        Step::AnyIndex => out.push_str("[*]"),
        Step::Filter(pred) => {
            out.push_str("?(");
            write_expr(pred, 0, out);
            out.push(')');
        }
        Step::Method(method) => {
            out.push('.');
            out.push_str(method.name());
            out.push_str("()");
        }
    }
}

fn write_index(index: Index, out: &mut String) {
    match index {
        Index::Num(n) => out.push_str(&n.to_string()),
        Index::Last(0) => out.push_str("last"),
        Index::Last(n) if n > 0 => {
            out.push_str("last - ");
            out.push_str(&n.to_string());
        }
        Index::Last(n) => {
            out.push_str("last + ");
            out.push_str(&(-n).to_string());
        }
    }
}

/// The canonical form of a jsonpath literal, or PostgreSQL's error.
pub(crate) fn canonical(text: &str) -> std::result::Result<String, String> {
    let path = parse(text).map_err(|e| e.text)?;
    Ok(print(&path))
}

// ---------------------------------------------------------------- evaluator

struct Eval<'a> {
    root: &'a J,
    vars: Option<&'a J>,
    mode: Mode,
}

impl<'a> Eval<'a> {
    fn structural(&self, message: impl Into<String>, code: &str) -> Error {
        Error::structural(db(message, code))
    }

    /// The items an operation applies to: in lax mode an array is unwrapped a
    /// single level; otherwise the item itself.
    fn operands<'j>(&self, item: &'j J) -> Vec<&'j J> {
        match item {
            J::Array(items) if self.mode == Mode::Lax => items.iter().collect(),
            other => vec![other],
        }
    }

    /// Evaluates an expression as a stream of items, pushing each produced
    /// item into `out` as it is found, so a failure leaves earlier items.
    fn stream(&mut self, expr: &Expr, current: &J, out: &mut Vec<J>) -> Result<()> {
        match expr {
            Expr::Nested(inner) => self.stream(inner, current, out),
            Expr::Path { base, steps } => {
                let start = match base {
                    PathBase::Root => self.root.clone(),
                    PathBase::Current => current.clone(),
                    PathBase::Var(name) => self.variable(name)?,
                };
                let mut items = vec![start];
                for (i, step) in steps.iter().enumerate() {
                    let last = i + 1 == steps.len();
                    let mut next = Vec::new();
                    for item in items {
                        if let Some(mut produced) = self.apply(step, &item)? {
                            if last {
                                out.append(&mut produced);
                            } else {
                                next.append(&mut produced);
                            }
                        }
                    }
                    items = next;
                }
                if steps.is_empty() {
                    out.extend(items);
                }
                Ok(())
            }
            Expr::MethodCall { method, operand } => {
                let items = self.items(operand, current)?;
                for item in items {
                    self.method_into(*method, &item, out)?;
                }
                Ok(())
            }
            other => {
                out.push(self.value(other, current)?);
                Ok(())
            }
        }
    }

    /// Applies a method to each of an expression's items, as a method step
    /// does.
    fn method_into(&mut self, method: Method, item: &J, out: &mut Vec<J>) -> Result<()> {
        if matches!(method, Method::Type | Method::Size) {
            out.push(self.method(method, item)?);
        } else {
            for op in self.operands(item) {
                out.push(self.method(method, op)?);
            }
        }
        Ok(())
    }

    /// All items an expression produces (paths stream; a value is one item).
    fn items(&mut self, expr: &Expr, current: &J) -> Result<Vec<J>> {
        let mut out = Vec::new();
        self.stream(expr, current, &mut out)?;
        Ok(out)
    }

    /// Applies one step to an item. `Ok(None)` marks a structural mismatch
    /// suppressed by lax mode (the step produced nothing); `Err` is a raised
    /// failure (structural in strict mode, runtime otherwise).
    fn apply(&mut self, step: &Step, item: &J) -> Result<Option<Vec<J>>> {
        match step {
            Step::Key(key) => {
                let mut out = Vec::new();
                for op in self.operands(item) {
                    match op {
                        J::Object(map) => match map.get(key) {
                            Some(v) => out.push(v.clone()),
                            None => {
                                if self.mode == Mode::Strict {
                                    return Err(self.structural(
                                        format!("JSON object does not contain key {key:?}"),
                                        "2203A",
                                    ));
                                }
                            }
                        },
                        _ => {
                            if self.mode == Mode::Strict {
                                return Err(self.structural(
                                    "jsonpath member accessor can only be applied to an object",
                                    "2203A",
                                ));
                            }
                        }
                    }
                }
                Ok(Some(out))
            }
            Step::AnyKey => {
                let mut out = Vec::new();
                for op in self.operands(item) {
                    match op {
                        J::Object(map) => out.extend(map.values().cloned()),
                        _ => {
                            if self.mode == Mode::Strict {
                                return Err(self.structural(
                                    "jsonpath wildcard member accessor can only be applied to an object",
                                    "2203C",
                                ));
                            }
                        }
                    }
                }
                Ok(Some(out))
            }
            Step::Index(index) => {
                let (len, as_array) = self.array_shape(item)?;
                let Some(i) = resolve_index(*index, len) else {
                    if self.mode == Mode::Strict {
                        return Err(
                            self.structural("jsonpath array subscript is out of bounds", "22033")
                        );
                    }
                    return Ok(Some(Vec::new()));
                };
                Ok(Some(vec![as_array[i].clone()]))
            }
            Step::Slice(a, b) => {
                let (len, as_array) = self.array_shape(item)?;
                let start = slice_bound(*a, len);
                let end = slice_bound(*b, len);
                match self.mode {
                    Mode::Strict => {
                        let in_range = |i: i64| i >= 0 && (i as usize) < len;
                        if !in_range(start) || !in_range(end) {
                            return Err(self
                                .structural("jsonpath array subscript is out of bounds", "22033"));
                        }
                        let (start, end) = (start as usize, end as usize);
                        if start > end {
                            return Ok(Some(Vec::new()));
                        }
                        Ok(Some(as_array[start..=end].to_vec()))
                    }
                    Mode::Lax => {
                        // A negative start clamps to the first element; a
                        // negative end makes the slice empty.
                        let start = start.max(0);
                        if start as usize >= len {
                            return Ok(Some(Vec::new()));
                        }
                        if end < 0 {
                            return Ok(Some(Vec::new()));
                        }
                        let start = start as usize;
                        let end = (end as usize).min(len - 1);
                        if start > end {
                            return Ok(Some(Vec::new()));
                        }
                        Ok(Some(as_array[start..=end].to_vec()))
                    }
                }
            }
            Step::AnyIndex => match item {
                // The wildcard reads the array itself (it is not unwrapped).
                J::Array(items) => Ok(Some(items.clone())),
                other if self.mode == Mode::Lax => Ok(Some(vec![other.clone()])),
                _ => Err(self.structural(
                    "jsonpath array accessor can only be applied to an array",
                    "22039",
                )),
            },
            Step::Filter(pred) => {
                let mut out = Vec::new();
                for op in self.operands(item) {
                    if self.predicate(pred, op)? == Some(true) {
                        out.push(op.clone());
                    }
                }
                Ok(Some(out))
            }
            Step::Method(method) => {
                // `.type()` and `.size()` describe the item itself; the value
                // methods unwrap an array in lax mode.
                let mut out = Vec::new();
                self.method_into(*method, item, &mut out)?;
                Ok(Some(out))
            }
        }
    }

    /// The array an index step reads: the item when it is an array; in lax
    /// mode a non-array is a single-element array; strict mode fails.
    fn array_shape(&self, item: &J) -> Result<(usize, Vec<J>)> {
        match item {
            J::Array(items) => Ok((items.len(), items.clone())),
            _ => {
                if self.mode == Mode::Lax {
                    Ok((1, vec![item.clone()]))
                } else {
                    Err(self.structural(
                        "jsonpath array accessor can only be applied to an array",
                        "22039",
                    ))
                }
            }
        }
    }

    /// A value expression: literals, variables, comparisons, arithmetic, and
    /// predicates used as values produce exactly one item.
    fn value(&mut self, expr: &Expr, current: &J) -> Result<J> {
        match expr {
            Expr::Literal(lit) => Ok(literal_json(lit)),
            Expr::Var(name) => self.variable(name),
            Expr::Nested(inner) => self.value(inner, current),
            Expr::Path { .. } => {
                let items = self.items(expr, current)?;
                Ok(match items.len() {
                    1 => items.into_iter().next().unwrap_or(J::Null),
                    _ => {
                        return Err(Error::runtime(db(
                            "single boolean result is expected",
                            "22038",
                        )));
                    }
                })
            }
            Expr::MethodCall { .. } => {
                let mut items = Vec::new();
                self.stream(expr, current, &mut items)?;
                Ok(match items.len() {
                    1 => items.into_iter().next().unwrap_or(J::Null),
                    _ => {
                        return Err(Error::runtime(db(
                            "single boolean result is expected",
                            "22038",
                        )));
                    }
                })
            }
            Expr::Compare(op, l, r) => {
                let ls = self.items(l, current)?;
                let rs = self.items(r, current)?;
                Ok(self.compare(*op, &ls, &rs))
            }
            Expr::Arith(op, l, r) => {
                let a = self.single(l, current, "left", *op)?;
                let b = self.single(r, current, "right", *op)?;
                self.arith(*op, a, b)
            }
            Expr::Neg(inner) => {
                let v = self.single(inner, current, "operand", ArithOp::Sub)?;
                let Some(n) = number_value(&v) else {
                    return Err(Error::runtime(db(
                        "operand of unary jsonpath operator - is not a numeric value",
                        "2203B",
                    )));
                };
                Ok(numeric_json(&-n))
            }
            Expr::And(l, r) => {
                let a = self.predicate(l, current)?;
                if a == Some(false) {
                    return Ok(J::Bool(false));
                }
                let b = self.predicate(r, current)?;
                Ok(tri_json(match (a, b) {
                    (Some(true), Some(true)) => Some(true),
                    (Some(false), _) | (_, Some(false)) => Some(false),
                    _ => None,
                }))
            }
            Expr::Or(l, r) => {
                let a = self.predicate(l, current)?;
                if a == Some(true) {
                    return Ok(J::Bool(true));
                }
                let b = self.predicate(r, current)?;
                Ok(tri_json(match (a, b) {
                    (Some(true), _) | (_, Some(true)) => Some(true),
                    (Some(false), Some(false)) => Some(false),
                    _ => None,
                }))
            }
            Expr::Not(inner) => Ok(tri_json(self.predicate(inner, current)?.map(|b| !b))),
            Expr::Exists(inner) => {
                let items = match self.items(inner, current) {
                    Ok(items) => items,
                    Err(e) if e.structural => Vec::new(),
                    Err(e) => return Err(e),
                };
                Ok(J::Bool(!items.is_empty()))
            }
            Expr::LikeRegex {
                operand,
                pattern,
                flags,
            } => Ok(tri_json(
                self.string_value(operand, current)?
                    .map(|s| compile_regex(pattern, flags).map(|re| re.is_match(&s)))
                    .transpose()?,
            )),
            Expr::StartsWith { operand, prefix } => Ok(tri_json(
                self.string_value(operand, current)?
                    .map(|s| s.starts_with(prefix.as_str())),
            )),
        }
    }

    /// The single value an arithmetic operand must be.
    fn single(&mut self, expr: &Expr, current: &J, side: &str, op: ArithOp) -> Result<J> {
        let items = self.items(expr, current)?;
        if items.len() != 1 {
            return Err(Error::runtime(db(
                format!(
                    "{side} operand of jsonpath operator {} is not a single numeric value",
                    op.symbol()
                ),
                "22038",
            )));
        }
        Ok(items.into_iter().next().unwrap_or(J::Null))
    }

    /// The tri-state result of a predicate. An empty operand is false; a
    /// non-string operand of `like_regex`/`starts with` is unknown.
    fn predicate(&mut self, expr: &Expr, current: &J) -> Result<Option<bool>> {
        match expr {
            Expr::Compare(op, l, r) => {
                let ls = self.items(l, current)?;
                let rs = self.items(r, current)?;
                Ok(self.compare_opt(*op, &ls, &rs))
            }
            Expr::And(l, r) => {
                let a = self.predicate(l, current)?;
                if a == Some(false) {
                    return Ok(Some(false));
                }
                let b = self.predicate(r, current)?;
                Ok(match (a, b) {
                    (Some(true), Some(true)) => Some(true),
                    (Some(false), _) | (_, Some(false)) => Some(false),
                    _ => None,
                })
            }
            Expr::Or(l, r) => {
                let a = self.predicate(l, current)?;
                if a == Some(true) {
                    return Ok(Some(true));
                }
                let b = self.predicate(r, current)?;
                Ok(match (a, b) {
                    (Some(true), _) | (_, Some(true)) => Some(true),
                    (Some(false), Some(false)) => Some(false),
                    _ => None,
                })
            }
            Expr::Not(inner) => Ok(self.predicate(inner, current)?.map(|b| !b)),
            Expr::Exists(inner) => {
                let items = match self.items(inner, current) {
                    Ok(items) => items,
                    Err(e) if e.structural => Vec::new(),
                    Err(e) => return Err(e),
                };
                Ok(Some(!items.is_empty()))
            }
            Expr::LikeRegex {
                operand,
                pattern,
                flags,
            } => Ok(self
                .string_value(operand, current)?
                .map(|s| compile_regex(pattern, flags).map(|re| re.is_match(&s)))
                .transpose()?),
            Expr::StartsWith { operand, prefix } => Ok(self
                .string_value(operand, current)?
                .map(|s| s.starts_with(prefix.as_str()))),
            Expr::Nested(inner) => self.predicate(inner, current),
            _ => Err(Error::runtime(db(
                "single boolean result is expected",
                "22038",
            ))),
        }
    }

    /// A string operand of `like_regex`/`starts with`: `None` when unknown
    /// (no items or a non-string item).
    fn string_value(&mut self, expr: &Expr, current: &J) -> Result<Option<String>> {
        let items = self.items(expr, current)?;
        Ok(match items.first() {
            Some(J::String(s)) => Some(s.clone()),
            _ => None,
        })
    }

    fn variable(&self, name: &str) -> Result<J> {
        match self.vars {
            Some(J::Object(map)) => match map.get(name) {
                Some(v) => Ok(v.clone()),
                None => Err(Error::fatal(db(
                    format!("could not find jsonpath variable {name:?}"),
                    "42704",
                ))),
            },
            _ => Err(Error::fatal(db(
                format!("could not find jsonpath variable {name:?}"),
                "42704",
            ))),
        }
    }

    fn arith(&mut self, op: ArithOp, a: J, b: J) -> Result<J> {
        let symbol = op.symbol();
        let (x, y) = match (number_value(&a), number_value(&b)) {
            (Some(x), Some(y)) => (x, y),
            (x, _) => {
                let side = if x.is_none() { "left" } else { "right" };
                return Err(Error::runtime(db(
                    format!(
                        "{side} operand of jsonpath operator {symbol} is not a single numeric value"
                    ),
                    "22038",
                )));
            }
        };
        let out = match op {
            ArithOp::Add => &x + &y,
            ArithOp::Sub => &x - &y,
            ArithOp::Mul => &x * &y,
            ArithOp::Div => match x.checked_div(&y) {
                Ok(v) => v,
                Err(e) => return Err(Error::runtime(db(e, "22012"))),
            },
            ArithOp::Mod => match x.checked_rem(&y) {
                Ok(v) => v,
                Err(e) => return Err(Error::runtime(db(e, "22012"))),
            },
        };
        Ok(numeric_json(&out))
    }

    fn method(&mut self, method: Method, item: &J) -> Result<J> {
        match method {
            Method::Type => Ok(J::String(
                match item {
                    J::Null => "null",
                    J::Bool(_) => "boolean",
                    J::Number(_) => "number",
                    J::String(_) => "string",
                    J::Array(_) => "array",
                    J::Object(_) => "object",
                }
                .to_string(),
            )),
            Method::Size => match item {
                J::Array(items) => Ok(J::Number(serde_json::Number::from(items.len() as u64))),
                // Lax mode takes anything else as one element; strict mode
                // wants an array.
                _ if self.mode == Mode::Lax => Ok(J::Number(serde_json::Number::from(1))),
                _ => Err(Error::runtime(db(
                    "jsonpath item method .size() can only be applied to an array",
                    "22039",
                ))),
            },
            Method::Double => match number_value(item) {
                Some(n) => Ok(float_json(n.to_f64())),
                None => match item {
                    J::String(s) => match s.trim().parse::<f64>() {
                        Ok(f) => Ok(float_json(f)),
                        Err(_) => Err(method_argument_error(s, "double precision")),
                    },
                    _ => Err(method_type_error("double", "string or numeric")),
                },
            },
            Method::Floor | Method::Ceiling | Method::Abs => {
                let Some(n) = number_value(item) else {
                    return Err(method_type_error(method.name(), "numeric"));
                };
                Ok(numeric_json(&match method {
                    Method::Floor => n.floor(),
                    Method::Ceiling => n.ceil(),
                    _ => n.abs(),
                }))
            }
            Method::Integer => {
                let n = match item {
                    J::Number(_) => number_value(item),
                    J::String(s) => match Numeric::parse(s.trim()) {
                        Ok(n) => Some(n),
                        Err(_) => return Err(method_argument_error(s, "integer")),
                    },
                    _ => None,
                };
                let Some(n) = n else {
                    return Err(method_type_error("integer", "string or numeric"));
                };
                let rounded = n.round(0);
                match rounded.to_i64() {
                    Some(i) => Ok(J::Number(serde_json::Number::from(i))),
                    None => Err(method_argument_error(
                        &crate::json_text::jsonb_text(item),
                        "integer",
                    )),
                }
            }
            Method::Number => match item {
                J::Number(_) => Ok(item.clone()),
                J::String(s) => match Numeric::parse(s.trim()) {
                    Ok(n) => Ok(numeric_json(&n)),
                    Err(_) => Err(method_argument_error(s, "numeric")),
                },
                _ => Err(method_type_error("number", "string or numeric")),
            },
            Method::String => match item {
                J::String(_) => Ok(item.clone()),
                J::Number(_) => Ok(J::String(crate::json_text::jsonb_text(item))),
                J::Bool(b) => Ok(J::String(b.to_string())),
                _ => Err(Error::runtime(db(
                    "jsonpath item method .string() can only be applied to a boolean, string, numeric, or datetime value",
                    "22036",
                ))),
            },
            Method::Boolean => match item {
                J::Bool(_) => Ok(item.clone()),
                J::Number(_) => Ok(J::Bool(number_value(item).is_some_and(|n| !n.is_zero()))),
                J::String(s) => match crate::planner::parse_bool_text(s) {
                    crate::Value::Bool(b) => Ok(J::Bool(b)),
                    _ => Err(method_argument_error(s, "boolean")),
                },
                _ => Err(Error::runtime(db(
                    "jsonpath item method .boolean() can only be applied to a boolean, string, or numeric value",
                    "22036",
                ))),
            },
        }
    }

    /// A comparison of two item sequences: any true wins; otherwise an
    /// unknown comparison makes the result unknown; an empty side is false.
    fn compare(&mut self, op: CmpOp, left: &[J], right: &[J]) -> J {
        tri_json(self.compare_opt(op, left, right))
    }

    fn compare_opt(&self, op: CmpOp, left: &[J], right: &[J]) -> Option<bool> {
        let mut unknown = false;
        for l in left {
            for r in right {
                for a in self.comparison_operands(l) {
                    for b in self.comparison_operands(r) {
                        match compare_scalar(op, a, b) {
                            Some(true) => return Some(true),
                            Some(false) => {}
                            None => unknown = true,
                        }
                    }
                }
            }
        }
        if unknown { None } else { Some(false) }
    }

    /// Comparison operands: in lax mode an array value is unwrapped one level
    /// (`$.a > 2` compares the elements of an array `a`).
    fn comparison_operands<'j>(&self, item: &'j J) -> Vec<&'j J> {
        match item {
            J::Array(items) if self.mode == Mode::Lax => items.iter().collect(),
            other => vec![other],
        }
    }
}

fn tri_json(value: Option<bool>) -> J {
    match value {
        Some(b) => J::Bool(b),
        None => J::Null,
    }
}

fn method_type_error(method: &str, expected: &str) -> Error {
    Error::runtime(db(
        format!("jsonpath item method .{method}() can only be applied to a {expected} value"),
        "22036",
    ))
}

fn method_argument_error(value: &str, target: &str) -> Error {
    Error::runtime(db(
        format!("argument {value:?} of jsonpath item method is invalid for type {target}"),
        "22036",
    ))
}

fn literal_json(lit: &Lit) -> J {
    match lit {
        Lit::Num(n) => numeric_json(n),
        Lit::Str(s) => J::String(s.clone()),
        Lit::Bool(b) => J::Bool(*b),
        Lit::Null => J::Null,
    }
}

fn numeric_json(n: &Numeric) -> J {
    n.to_string()
        .parse::<serde_json::Number>()
        .map(J::Number)
        .unwrap_or(J::Null)
}

fn float_json(f: f64) -> J {
    match Numeric::from_f64_exact(f) {
        Some(n) => numeric_json(&n),
        None => J::Null,
    }
}

fn number_value(value: &J) -> Option<Numeric> {
    match value {
        J::Number(n) => Numeric::parse(&n.to_string()).ok(),
        _ => None,
    }
}

fn resolve_index(index: Index, len: usize) -> Option<usize> {
    let i = slice_bound(index, len);
    if i < 0 || i as usize >= len {
        None
    } else {
        Some(i as usize)
    }
}

/// A slice bound's position, which may fall outside the array (slices clamp
/// or fail depending on the mode).
fn slice_bound(index: Index, len: usize) -> i64 {
    match index {
        Index::Num(n) => n,
        Index::Last(n) => len as i64 - 1 - n,
    }
}

fn compile_regex(pattern: &str, flags: &str) -> Result<regex::Regex> {
    let mut regex_flags = String::new();
    for flag in flags.chars() {
        match flag {
            'i' | 's' | 'm' | 'q' => regex_flags.push(flag),
            'x' => {
                return Err(Error::runtime(db(
                    "XQuery \"x\" flag (expanded regular expressions) is not implemented",
                    "0A000",
                )));
            }
            other => {
                let error =
                    crate::error_fields::DbError::new("invalid input syntax for type jsonpath")
                        .code("42601")
                        .detail(format!(
                            "Unrecognized flag character \"{other}\" in LIKE_REGEX predicate."
                        ))
                        .into_text();
                return Err(Error::runtime(error));
            }
        }
    }
    crate::pg_regex::compile(pattern, &regex_flags)
        .map_err(|e| Error::runtime(db(format!("invalid regular expression: {e}"), "2201B")))
}

/// A comparison of one pair of values, `None` when PostgreSQL's jsonpath
/// treats it as unknown (mismatched scalar types, arrays, objects).
fn compare_scalar(op: CmpOp, left: &J, right: &J) -> Option<bool> {
    use std::cmp::Ordering;
    let ord = match (left, right) {
        (J::Null, J::Null) => {
            return match op {
                CmpOp::Eq | CmpOp::LtEq | CmpOp::GtEq => Some(true),
                _ => Some(false),
            };
        }
        (J::Null, _) | (_, J::Null) => {
            return match op {
                CmpOp::NotEq => Some(true),
                _ => Some(false),
            };
        }
        (J::Number(_), J::Number(_)) => {
            let (a, b) = (number_value(left)?, number_value(right)?);
            a.cmp(&b)
        }
        (J::String(a), J::String(b)) => a.as_bytes().cmp(b.as_bytes()),
        (J::Bool(a), J::Bool(b)) => a.cmp(b),
        _ => return None,
    };
    Some(match op {
        CmpOp::Eq => ord == Ordering::Equal,
        CmpOp::NotEq => ord != Ordering::Equal,
        CmpOp::Lt => ord == Ordering::Less,
        CmpOp::LtEq => ord != Ordering::Greater,
        CmpOp::Gt => ord == Ordering::Greater,
        CmpOp::GtEq => ord != Ordering::Less,
    })
}

// ---------------------------------------------------------------- entry points

/// Runs a path against a target, remembering both the items produced and the
/// failure that stopped it (items produced before a failure are the partial
/// result `silent` callers keep).
struct Run {
    items: Vec<J>,
    error: Option<Error>,
}

fn run(path_text: &str, target: &J, vars: Option<&J>) -> std::result::Result<Run, String> {
    let path = parse(path_text).map_err(|e| e.text)?;
    let mut eval = Eval {
        root: target,
        vars,
        mode: path.mode,
    };
    let mut items = Vec::new();
    let error = eval.stream(&path.expr, target, &mut items).err();
    Ok(Run { items, error })
}

/// Runs a path against a target. With `silent`, a suppressible failure keeps
/// the items produced before it; otherwise the failure is `Err`.
pub(crate) fn execute(
    path_text: &str,
    target: &J,
    vars: Option<&J>,
    silent: bool,
) -> std::result::Result<Vec<J>, String> {
    let run = run(path_text, target, vars)?;
    match (run.error, silent) {
        (None, _) => Ok(run.items),
        (Some(e), true) if !e.fatal => Ok(run.items),
        (Some(e), _) => Err(e.text),
    }
}

/// `jsonb @? jsonpath` and `jsonb_path_exists`: whether the path produces any
/// item. `Ok(None)` is SQL NULL (a suppressed failure).
pub(crate) fn exists(
    target: &J,
    path_text: &str,
    vars: Option<&J>,
    silent: bool,
) -> std::result::Result<Option<bool>, String> {
    let run = run(path_text, target, vars)?;
    match (run.error, silent) {
        (None, _) => Ok(Some(!run.items.is_empty())),
        (Some(e), true) if !e.fatal => Ok(None),
        (Some(e), _) => Err(e.text),
    }
}

/// `jsonb @@ jsonpath` and `jsonb_path_match`: the path's boolean result.
/// `Ok(None)` is SQL NULL (unknown or a suppressed failure).
pub(crate) fn matches(
    target: &J,
    path_text: &str,
    vars: Option<&J>,
    silent: bool,
) -> std::result::Result<Option<bool>, String> {
    let run = run(path_text, target, vars)?;
    match (run.error, silent) {
        (None, _) => match run.items.as_slice() {
            [J::Bool(b)] => Ok(Some(*b)),
            [J::Null] => Ok(None),
            _ => {
                if silent {
                    Ok(None)
                } else {
                    Err(db("single boolean result is expected", "22038"))
                }
            }
        },
        (Some(e), true) if !e.fatal => Ok(None),
        (Some(e), _) => Err(e.text),
    }
}

/// `jsonb_path_query`: the items the path produces.
pub(crate) fn query(
    target: &J,
    path_text: &str,
    vars: Option<&J>,
    silent: bool,
) -> std::result::Result<Vec<J>, String> {
    execute(path_text, target, vars, silent)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(text: &str) -> J {
        crate::json_text::parse(text).expect("test JSON")
    }

    fn canonical_of(text: &str) -> String {
        canonical(text).expect("parses")
    }

    fn run(path: &str, doc: &str) -> Vec<String> {
        query(&target(doc), path, None, false)
            .expect("runs")
            .iter()
            .map(crate::json_text::jsonb_text)
            .collect()
    }

    fn fails(path: &str, doc: &str) -> bool {
        query(&target(doc), path, None, false).is_err()
    }

    #[test]
    fn literals_print_in_postgres_canonical_form() {
        assert_eq!(canonical_of("$"), "$");
        assert_eq!(canonical_of("$.a.b"), "$.\"a\".\"b\"");
        assert_eq!(canonical_of("$.\"a b\""), "$.\"a b\"");
        assert_eq!(canonical_of("$[1 to 3][last]"), "$[1 to 3][last]");
        assert_eq!(canonical_of("$.a  ? (@ > 2)"), "$.\"a\"?(@ > 2)");
        assert_eq!(canonical_of("$ ? (@ == 1 && @ < 2)"), "$?(@ == 1 && @ < 2)");
        assert_eq!(
            canonical_of("strict $.a ? (@ == 1)"),
            "strict $.\"a\"?(@ == 1)"
        );
        assert_eq!(canonical_of("$.a[*].b"), "$.\"a\"[*].\"b\"");
        assert_eq!(canonical_of("($.a)"), "$.\"a\"");
        assert_eq!(canonical_of("($.a + 1)"), "($.\"a\" + 1)");
        assert_eq!(canonical_of("$.a == 1"), "($.\"a\" == 1)");
        assert_eq!(canonical_of("1e2"), "100");
        assert_eq!(canonical_of("$.a ? (@ > 1.0)"), "$.\"a\"?(@ > 1.0)");
        assert_eq!(canonical_of("$var"), "$\"var\"");
        assert_eq!(canonical_of("exists($.a)"), "exists ($.\"a\")");
        assert_eq!(
            canonical_of("$ ? (!(exists(@.a)))"),
            "$?(!(exists (@.\"a\")))"
        );
        assert_eq!(
            canonical_of("$ ? (@ like_regex \"x\" flag \"i\")"),
            "$?(@ like_regex \"x\" flag \"i\")"
        );
        assert_eq!(
            canonical_of("$ ? (@ starts with \"x\")"),
            "$?(@ starts with \"x\")"
        );
        assert_eq!(canonical_of("$.a[last - 1]"), "$.\"a\"[last - 1]");
        assert_eq!(canonical_of("($.a + 1).type()"), "($.\"a\" + 1).type()");
        assert_eq!(canonical_of("$x.type()"), "$\"x\".type()");
        assert_eq!(canonical_of("$.a <> 1"), "($.\"a\" != 1)");
    }

    #[test]
    fn syntax_errors_match_postgres_messages() {
        let error = |text: &str| parse(text).expect_err("rejects").text;
        assert!(error("x").contains("syntax error at end of jsonpath input"));
        assert!(error("$.").contains("syntax error at end of jsonpath input"));
        assert!(error("@ == 1").contains("@ is not allowed in root expressions"));
        assert!(error("$.a ? (@ == 1) --x").contains("syntax error"));
        assert!(
            error("$ ? (@ like_regex \"a\" flag \"z\")").contains("Unrecognized flag character")
        );
        assert!(error("$ ? (@ like_regex \"a\" flag \"x\")").contains("XQuery"));
        assert!(error("$.a ? (@ > 0) && true").contains("syntax error at or near \"&&\""));
        assert!(error("($.a == 1) == true").contains("syntax error at or near \"==\""));
    }

    #[test]
    fn navigation_follows_lax_and_strict_rules() {
        assert_eq!(run("$.a.b", r#"{"a":[{"b":1}]}"#), ["1"]);
        // A nested array is unwrapped once per step, not recursively.
        assert!(run("$.a.b", r#"{"a":[[{"b":1}]]}"#).is_empty());
        assert_eq!(run("$[*].b", r#"[[{"b":1}]]"#), ["1"]);
        assert_eq!(run("$.a[*]", r#"{"a":[[1,2],[3]]}"#), ["[1, 2]", "[3]"]);
        assert_eq!(run("$.a[*][*]", r#"{"a":[[1,2],[3]]}"#), ["1", "2", "3"]);
        // A non-array reads as one element for indexes.
        assert_eq!(run("$.a[0]", r#"{"a":{"x":1}}"#), ["{\"x\": 1}"]);
        // Missing members are nothing in lax mode.
        assert!(run("$.b", r#"{"a":1}"#).is_empty());
        // Strict mode reports them.
        assert!(fails("strict $.b", r#"{"a":1}"#));
        assert!(fails("strict $.a.b", r#"{"a":1}"#));
        assert!(fails("strict $[0]", r#"{"a":1}"#));
    }

    #[test]
    fn slices_and_last() {
        assert_eq!(run("$.a[1 to last]", r#"{"a":[1,2,3]}"#), ["2", "3"]);
        assert_eq!(run("$.a[1 to last - 1]", r#"{"a":[1,2,3,4]}"#), ["2", "3"]);
        assert_eq!(run("$.a[0 to 5]", r#"{"a":[1,2]}"#), ["1", "2"]);
        assert!(run("$.a[2 to 1]", r#"{"a":[1,2]}"#).is_empty());
        // A negative start clamps to the first element in lax mode.
        assert_eq!(run("lax $.a[-1 to 0]", r#"{"a":[1,2]}"#), ["1"]);
        assert!(run("lax $.a[0 to -1]", r#"{"a":[1,2]}"#).is_empty());
        assert!(fails("strict $.a[0 to 5]", r#"{"a":[1,2]}"#));
        assert!(run("lax $.a[5]", r#"{"a":[1,2]}"#).is_empty());
        assert!(fails("strict $.a[5]", r#"{"a":[1,2]}"#));
    }

    #[test]
    fn filters_compare_with_postgres_semantics() {
        let doc = r#"{"a":[1,2,3]}"#;
        assert_eq!(run("$.a[*] ? (@ > 1)", doc), ["2", "3"]);
        assert_eq!(run("$.a[*] ? (@ > 1 && @ < 3)", doc), ["2"]);
        assert_eq!(run("$.a ? (@ > 1)", doc), ["2", "3"]);
        // Incomparable types are unknown, so nothing matches.
        assert!(run("$.a[*] ? (@ == \"x\")", doc).is_empty());
        // Null compares: equal to null, and unequal to anything else.
        assert_eq!(run("$.a ? (@ != null)", r#"{"a":1}"#), ["1"]);
        assert!(run("$.a ? (@ == null)", r#"{"a":1}"#).is_empty());
        assert_eq!(run("$.a ? (@ == null)", r#"{"a":null}"#), ["null"]);
        // A comparison over a multi-item path is true if any item matches.
        assert_eq!(run("$.a[*] > 2", doc), ["true"]);
        assert_eq!(run("$.a[*] != 1", doc), ["true"]);
        assert_eq!(run("$.a[*] > 5", doc), ["false"]);
        assert_eq!(run("$.a.type() == \"array\"", doc), ["true"]);
        assert_eq!(run("$.a like_regex \"x\"", r#"{"a":"xy"}"#), ["true"]);
        assert_eq!(run("$.a starts with \"x\"", r#"{"a":"xy"}"#), ["true"]);
        assert_eq!(run("$.a starts with \"x\"", r#"{"a":1}"#), ["null"]);
        assert_eq!(run("exists($.a)", doc), ["true"]);
        assert_eq!(run("exists($.b)", doc), ["false"]);
    }

    #[test]
    fn arithmetic_and_methods() {
        assert_eq!(run("$.a + 1", r#"{"a":5}"#), ["6"]);
        assert_eq!(run("$.a / 2", r#"{"a":5}"#), ["2.5000000000000000"]);
        assert_eq!(run("$.a % 3", r#"{"a":5}"#), ["2"]);
        assert_eq!(run("$.a * 0.5", r#"{"a":5}"#), ["2.5"]);
        assert_eq!(run("-$.a", r#"{"a":5}"#), ["-5"]);
        assert!(fails("$.a / 0", r#"{"a":5}"#));
        assert!(fails("$.a + 1", r#"{"a":"x"}"#));
        assert_eq!(run("$.a.floor()", r#"{"a":1.7}"#), ["1"]);
        assert_eq!(run("$.a.ceiling()", r#"{"a":1.2}"#), ["2"]);
        assert_eq!(run("$.a.abs()", r#"{"a":-2}"#), ["2"]);
        assert_eq!(run("$.a.integer()", r#"{"a":2.5}"#), ["3"]);
        assert_eq!(run("$.a.string()", r#"{"a":1.50}"#), ["\"1.50\""]);
        assert_eq!(run("$.a.boolean()", r#"{"a":0}"#), ["false"]);
        assert_eq!(run("$.a.type()", r#"{"a":[1,2]}"#), ["\"array\""]);
        assert_eq!(run("$.a.size()", r#"{"a":[1,2]}"#), ["2"]);
        assert_eq!(run("$.a.size()", r#"{"a":{"x":1}}"#), ["1"]);
        assert!(fails("strict $.a.size()", r#"{"a":1}"#));
        assert_eq!(
            run("($.a[*]).type()", r#"{"a":[1,2]}"#),
            ["\"number\"", "\"number\""]
        );
        assert_eq!(run("($.a).size()", r#"{"a":[1,2]}"#), ["2"]);
    }

    #[test]
    fn variables_are_fatal_and_silent_keeps_partial_results() {
        let doc = target(r#"{"a":[1,2,3]}"#);
        assert_eq!(
            query(
                &doc,
                "$.a[*] ? (@ > $min)",
                Some(&target(r#"{"min":1}"#)),
                false
            )
            .expect("runs"),
            [J::from(2), J::from(3)]
        );
        let missing = query(&doc, "$.a[*] ? (@ > $min)", None, false);
        assert!(missing.is_err());
        // `silent` does not suppress a missing variable.
        let silent = query(&doc, "$.a[*] ? (@ > $min)", None, true);
        assert!(silent.is_err());
        // It does keep the items produced before a failure.
        let partial = query(&target(r#"[1,"x"]"#), "$[*].floor()", None, true).expect("silent");
        assert_eq!(partial, [J::from(1)]);
        assert!(query(&target(r#"[1,"x"]"#), "$[*].floor()", None, false).is_err());
    }

    #[test]
    fn variable_paths_start_at_the_variable() {
        // `$x.b` is a path rooted at the variable, whose value comes from
        // the `vars` object.
        let vars = target(r#"{"x":{"b":{"c":5}}}"#);
        assert_eq!(
            query(&target("null"), "$x.b.c", Some(&vars), false).expect("runs"),
            [J::from(5)]
        );
        assert_eq!(
            query(
                &target("null"),
                "$x[*].b",
                Some(&target(r#"{"x":[{"b":1},{"b":2}]}"#)),
                false
            )
            .expect("runs"),
            [J::from(1), J::from(2)]
        );
        let missing = query(&target("null"), "$x.b", None, false);
        assert!(matches!(missing, Err(message) if message.contains("variable")));
        // The output form quotes the variable, as PostgreSQL prints it.
        assert_eq!(canonical("$x.b").expect("parses"), "$\"x\".\"b\"");
    }

    #[test]
    fn operators_and_functions_shape_their_results() {
        let doc = target(r#"{"a":[1,2,3]}"#);
        assert_eq!(exists(&doc, "$.a[*] ? (@ > 2)", None, true), Ok(Some(true)));
        assert!(matches!(
            exists(&target(r#"{"a":1}"#), "strict $.b", None, false),
            Err(message) if message.contains("does not contain")
        ));
        assert_eq!(
            exists(&target(r#"{"a":1}"#), "strict $.b", None, true),
            Ok(None)
        );
        assert_eq!(matches(&doc, "$.a[*] > 2", None, true), Ok(Some(true)));
        assert_eq!(matches(&doc, "$.a[*] > 5", None, true), Ok(Some(false)));
        assert_eq!(matches(&target(r#"{"a":1}"#), "$.a", None, true), Ok(None));
        assert!(matches(&target(r#"{"a":1}"#), "$.a", None, false).is_err());
    }
}
