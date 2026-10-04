//! PostgreSQL's `xpath` and `xpath_exists`: XPath 1.0 expressions over an
//! [`crate::xml`] document, evaluated as libxml2 evaluates them.
//!
//! The expression is parsed into the XPath grammar — location paths with the
//! axes and node tests, predicates, the union operator, comparisons,
//! arithmetic, and the core function library — and evaluated over a tree
//! built from the document. As in PostgreSQL, the context node is the
//! document node (so a relative path finds nothing), and a prefix in the
//! expression is resolved from the namespace array the call passes, never
//! from the document itself.

use crate::xml::{Document, Element, Node};

/// An error as PostgreSQL words it: the message with its SQLSTATE, and the
/// DETAIL libxml2 adds.
pub(crate) struct XpError {
    pub(crate) message: String,
    pub(crate) code: &'static str,
    pub(crate) detail: Option<String>,
}

impl XpError {
    fn new(message: impl Into<String>, code: &'static str) -> XpError {
        XpError {
            message: message.into(),
            code,
            detail: None,
        }
    }

    fn with_detail(
        message: impl Into<String>,
        code: &'static str,
        detail: impl Into<String>,
    ) -> XpError {
        XpError {
            message: message.into(),
            code,
            detail: Some(detail.into()),
        }
    }

    /// The error as the executor's channel carries it.
    pub(crate) fn into_text(self) -> String {
        let error = crate::error_fields::DbError::new(self.message).code(self.code);
        let error = match self.detail {
            Some(detail) => error.detail(detail),
            None => error,
        };
        error.into_text()
    }
}

/// An invalid expression, as libxml2 reports one.
fn invalid_expression() -> XpError {
    XpError::with_detail("invalid XPath expression", "10608", "Invalid expression")
}

fn undefined_prefix() -> XpError {
    XpError::with_detail(
        "could not create XPath object",
        "10608",
        "Undefined namespace prefix",
    )
}

/// `xpath(expression, document [, namespaces])`: the nodes the expression
/// selects, serialized, in document order; a result that is not a node-set
/// is one entry holding its string value.
pub(crate) fn xpath(
    expression: &str,
    document: &Document,
    namespaces: &[(String, String)],
) -> Result<Vec<String>, XpError> {
    let tree = Tree::build(document);
    let value = evaluate(expression, &tree, namespaces)?;
    Ok(match value {
        Value::Nodes(nodes) => nodes.iter().map(|&node| tree.serialize(node)).collect(),
        // A result that is not a node-set becomes an XML value: its string
        // is written markup-safe, as libxml2 writes one.
        other => vec![crate::xml::escape_xml(&other.string_value(&tree))],
    })
}

/// `xpath_exists(expression, document [, namespaces])`: whether the
/// expression selects a node.
pub(crate) fn xpath_exists(
    expression: &str,
    document: &Document,
    namespaces: &[(String, String)],
) -> Result<bool, XpError> {
    let tree = Tree::build(document);
    Ok(matches!(
        evaluate(expression, &tree, namespaces)?,
        Value::Nodes(nodes) if !nodes.is_empty()
    ))
}

/// Parses the expression and evaluates it with the document node as the
/// context node.
fn evaluate(
    expression: &str,
    tree: &Tree,
    namespaces: &[(String, String)],
) -> Result<Value, XpError> {
    if expression.trim().is_empty() {
        return Err(XpError::new("empty XPath expression", "10608"));
    }
    let mut parser = Parser {
        chars: expression.chars().collect(),
        pos: 0,
        namespaces,
    };
    let ast = parser.parse()?;
    Context {
        tree,
        ns: namespaces,
        node: 0,
        position: 1,
        size: 1,
    }
    .eval(&ast)
}

// ---------------------------------------------------------------------------
// The tree the expression walks. Node 0 is the document node.

struct Tree<'a> {
    doc: &'a Document,
    nodes: Vec<TNode<'a>>,
}

struct TNode<'a> {
    parent: Option<usize>,
    kind: Kind<'a>,
}

enum Kind<'a> {
    Document,
    Element {
        /// The element's own node, for serialization.
        node: &'a Node,
        local: String,
        /// The namespace URI the element's name is bound to.
        uri: String,
        children: Vec<usize>,
        attrs: Vec<usize>,
    },
    Text(&'a str),
    Cdata(&'a str),
    Comment(&'a str),
    Pi {
        name: &'a str,
        content: Option<&'a str>,
    },
    Doctype(&'a str),
    Attribute {
        name: &'a str,
        value: &'a str,
    },
}

/// The XML namespace, always bound.
const XML_NS: &str = "http://www.w3.org/XML/1998/namespace";

impl<'a> Tree<'a> {
    /// The tree of a document: the document node, its nodes, and each
    /// element's attributes as attribute nodes.
    fn build(doc: &'a Document) -> Tree<'a> {
        let mut tree = Tree {
            doc,
            nodes: vec![TNode {
                parent: None,
                kind: Kind::Document,
            }],
        };
        let root_ns = vec![("xml".to_string(), XML_NS.to_string())];
        let children = tree.add_children(0, &doc.children, &root_ns);
        tree.set_children(0, children);
        tree
    }

    fn add_children(
        &mut self,
        parent: usize,
        children: &'a [Node],
        inherited: &[(String, String)],
    ) -> Vec<usize> {
        let mut out = Vec::with_capacity(children.len());
        for child in children {
            let index = match child {
                Node::Element(element) => {
                    let ns = element_bindings(element, inherited);
                    let (local, uri) = resolve_name(&element.name, &ns);
                    let slot = self.nodes.len();
                    self.nodes.push(TNode {
                        parent: Some(parent),
                        kind: Kind::Element {
                            node: child,
                            local,
                            uri,
                            children: Vec::new(),
                            attrs: Vec::new(),
                        },
                    });
                    let mut attrs = Vec::new();
                    for (name, value) in &element.attrs {
                        if name == "xmlns" || name.starts_with("xmlns:") {
                            continue;
                        }
                        attrs.push(self.push(slot, Kind::Attribute { name, value }));
                    }
                    let kids = self.add_children(slot, &element.children, &ns);
                    if let Kind::Element {
                        attrs: slots,
                        children,
                        ..
                    } = &mut self.nodes[slot].kind
                    {
                        *slots = attrs;
                        *children = kids;
                    }
                    slot
                }
                Node::Text(text) => self.push(parent, Kind::Text(text)),
                Node::Cdata(text) => self.push(parent, Kind::Cdata(text)),
                Node::Comment(text) => self.push(parent, Kind::Comment(text)),
                Node::Pi { name, content } => self.push(
                    parent,
                    Kind::Pi {
                        name,
                        content: content.as_deref(),
                    },
                ),
                Node::Doctype(text) => self.push(parent, Kind::Doctype(text)),
            };
            out.push(index);
        }
        out
    }

    fn push(&mut self, parent: usize, kind: Kind<'a>) -> usize {
        let index = self.nodes.len();
        self.nodes.push(TNode {
            parent: Some(parent),
            kind,
        });
        index
    }

    fn set_children(&mut self, node: usize, children: Vec<usize>) {
        if let Kind::Element {
            children: slots, ..
        } = &mut self.nodes[node].kind
        {
            *slots = children;
        }
    }

    /// The node's children (an element's; the document node's were filled
    /// when the tree was built).
    fn children(&self, node: usize) -> Vec<usize> {
        match &self.nodes[node].kind {
            Kind::Element { children, .. } => children.clone(),
            Kind::Document => (1..self.nodes.len())
                .filter(|&i| self.nodes[i].parent == Some(node))
                .collect(),
            _ => Vec::new(),
        }
    }

    fn attrs(&self, node: usize) -> Vec<usize> {
        match &self.nodes[node].kind {
            Kind::Element { attrs, .. } => attrs.clone(),
            _ => Vec::new(),
        }
    }

    fn is_attribute(&self, node: usize) -> bool {
        matches!(self.nodes[node].kind, Kind::Attribute { .. })
    }

    fn is_element(&self, node: usize) -> bool {
        matches!(self.nodes[node].kind, Kind::Element { .. })
    }

    fn parent(&self, node: usize) -> Option<usize> {
        self.nodes[node].parent
    }

    /// The node's (prefix, local name, namespace URI).
    fn name(&self, node: usize) -> (String, String, String) {
        match &self.nodes[node].kind {
            Kind::Element { local, uri, .. } => (String::new(), local.clone(), uri.clone()),
            Kind::Attribute { name, .. } => {
                let (local, uri) = split_name(name);
                (String::new(), local, uri)
            }
            Kind::Pi { name, .. } => (String::new(), name.to_string(), String::new()),
            _ => (String::new(), String::new(), String::new()),
        }
    }

    /// A node's string value: what `string()` and a text comparison take.
    fn string_value(&self, node: usize) -> String {
        match &self.nodes[node].kind {
            Kind::Document | Kind::Element { .. } => {
                let mut out = String::new();
                self.text_content(node, &mut out);
                out
            }
            Kind::Text(text) | Kind::Cdata(text) | Kind::Comment(text) => text.to_string(),
            Kind::Pi { content, .. } => content.unwrap_or_default().to_string(),
            Kind::Attribute { value, .. } => value.to_string(),
            Kind::Doctype(_) => String::new(),
        }
    }

    fn text_content(&self, node: usize, out: &mut String) {
        for child in self.children(node) {
            match &self.nodes[child].kind {
                Kind::Text(text) | Kind::Cdata(text) => out.push_str(text),
                Kind::Element { .. } => self.text_content(child, out),
                _ => {}
            }
        }
    }

    /// The nodes of the subtree, in document order.
    fn descendants(&self, node: usize, out: &mut Vec<usize>) {
        for child in self.children(node) {
            out.push(child);
            if self.is_element(child) {
                self.descendants(child, out);
            }
        }
    }

    /// The ancestors, nearest first.
    fn ancestors(&self, node: usize) -> Vec<usize> {
        let mut out = Vec::new();
        let mut at = node;
        while let Some(parent) = self.parent(at) {
            out.push(parent);
            at = parent;
        }
        out
    }

    /// Every node in document order.
    fn all_nodes(&self) -> Vec<usize> {
        let mut out = Vec::new();
        self.descendants(0, &mut out);
        out
    }

    /// A node as `xpath` returns one.
    fn serialize(&self, node: usize) -> String {
        match &self.nodes[node].kind {
            Kind::Document => crate::xml::serialize_document(self.doc),
            Kind::Element { node, .. } => crate::xml::serialize_node(node),
            // An attribute result is its value, dumped escaped, as libxml2
            // writes one.
            Kind::Attribute { value, .. } => crate::xml::escape_xml(value),
            Kind::Text(text) | Kind::Cdata(text) => crate::xml::escape_xml(text),
            Kind::Comment(text) => format!("<!--{text}-->"),
            Kind::Pi { name, content } => match content {
                Some(content) if !content.is_empty() => format!("<?{name} {content}?>"),
                _ => format!("<?{name}?>"),
            },
            Kind::Doctype(text) => text.to_string(),
        }
    }
}

/// An element's in-scope namespace bindings: the inherited ones with its own
/// `xmlns` attributes added.
fn element_bindings(element: &Element, inherited: &[(String, String)]) -> Vec<(String, String)> {
    let mut ns = inherited.to_vec();
    for (name, value) in &element.attrs {
        if name == "xmlns" {
            ns.retain(|(prefix, _)| !prefix.is_empty());
            ns.push((String::new(), value.clone()));
        } else if let Some(prefix) = name.strip_prefix("xmlns:") {
            ns.retain(|(seen, _)| seen != prefix);
            ns.push((prefix.to_string(), value.clone()));
        }
    }
    ns
}

/// A name as (local name, namespace URI) with the bindings applied — for an
/// element's own name, where an unprefixed name takes the default namespace.
fn resolve_name(name: &str, ns: &[(String, String)]) -> (String, String) {
    match name.split_once(':') {
        Some((prefix, local)) => {
            let uri = ns
                .iter()
                .rev()
                .find(|(seen, _)| seen == prefix)
                .map(|(_, uri)| uri.clone())
                .unwrap_or_default();
            (local.to_string(), uri)
        }
        None => {
            let uri = ns
                .iter()
                .rev()
                .find(|(prefix, _)| prefix.is_empty())
                .map(|(_, uri)| uri.clone())
                .unwrap_or_default();
            (name.to_string(), uri)
        }
    }
}

/// A name with no bindings at hand (an attribute's, which takes none).
fn split_name(name: &str) -> (String, String) {
    match name.split_once(':') {
        Some((_, local)) => (local.to_string(), String::new()),
        None => (name.to_string(), String::new()),
    }
}

// ---------------------------------------------------------------------------
// The expression grammar.

#[derive(Debug, Clone)]
enum Expr {
    Or(Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Cmp(CmpOp, Box<Expr>, Box<Expr>),
    Arith(ArithOp, Box<Expr>, Box<Expr>),
    Neg(Box<Expr>),
    Union(Box<Expr>, Box<Expr>),
    /// A location path; `absolute` starts at the document node.
    Path {
        absolute: bool,
        steps: Vec<Step>,
    },
    /// `expr[predicate]...`.
    Filter(Box<Expr>, Vec<Expr>),
    Call(String, Vec<Expr>),
    Number(f64),
    Literal(String),
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

#[derive(Debug, Clone)]
struct Step {
    axis: Axis,
    test: NodeTest,
    predicates: Vec<Expr>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Axis {
    Child,
    Descendant,
    DescendantOrSelf,
    SelfAxis,
    Parent,
    Ancestor,
    AncestorOrSelf,
    FollowingSibling,
    PrecedingSibling,
    Following,
    Preceding,
    Attribute,
}

#[derive(Debug, Clone)]
enum NodeTest {
    /// `name`, `prefix:name`, `*`, `prefix:*`.
    Name {
        prefix: Option<String>,
        local: Option<String>,
    },
    Node,
    Text,
    Comment,
    Pi(Option<String>),
}

struct Parser<'a> {
    chars: Vec<char>,
    pos: usize,
    namespaces: &'a [(String, String)],
}

impl Parser<'_> {
    fn parse(&mut self) -> Result<Expr, XpError> {
        let expr = self.expr()?;
        self.skip_ws();
        if self.pos < self.chars.len() {
            return Err(invalid_expression());
        }
        Ok(expr)
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn peek_at(&self, offset: usize) -> Option<char> {
        self.chars.get(self.pos + offset).copied()
    }

    fn bump(&mut self) -> Option<char> {
        let c = self.peek()?;
        self.pos += 1;
        Some(c)
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(' ' | '\t' | '\n' | '\r')) {
            self.pos += 1;
        }
    }

    fn eat(&mut self, c: char) -> bool {
        if self.peek() == Some(c) {
            self.pos += 1;
            return true;
        }
        false
    }

    /// The name at the cursor, consumed.
    fn word(&mut self) -> String {
        let start = self.pos;
        while matches!(self.chars.get(self.pos), Some(c) if c.is_alphanumeric() || *c == '_' || *c == '-' || *c == '.')
        {
            self.pos += 1;
        }
        self.chars[start..self.pos].iter().collect()
    }

    /// Whether the operator `word` follows, consuming it if so. An operator
    /// is a whole word: the `div` in `divide` is a name.
    fn eat_word(&mut self, word: &str) -> bool {
        let save = self.pos;
        self.skip_ws();
        if self.word() == word {
            if self
                .peek()
                .is_some_and(|c| c.is_alphanumeric() || c == '_' || c == '-')
            {
                self.pos = save;
                return false;
            }
            return true;
        }
        self.pos = save;
        false
    }

    fn expr(&mut self) -> Result<Expr, XpError> {
        self.or_expr()
    }

    fn or_expr(&mut self) -> Result<Expr, XpError> {
        let mut left = self.and_expr()?;
        while self.eat_word("or") {
            let right = self.and_expr()?;
            left = Expr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn and_expr(&mut self) -> Result<Expr, XpError> {
        let mut left = self.eq_expr()?;
        while self.eat_word("and") {
            let right = self.eq_expr()?;
            left = Expr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn eq_expr(&mut self) -> Result<Expr, XpError> {
        let mut left = self.rel_expr()?;
        loop {
            self.skip_ws();
            let op = if self.eat('=') {
                CmpOp::Eq
            } else if self.peek() == Some('!') && self.peek_at(1) == Some('=') {
                self.pos += 2;
                CmpOp::Ne
            } else {
                break;
            };
            let right = self.rel_expr()?;
            left = Expr::Cmp(op, Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn rel_expr(&mut self) -> Result<Expr, XpError> {
        let mut left = self.add_expr()?;
        loop {
            self.skip_ws();
            let op = if self.peek() == Some('<') {
                if self.peek_at(1) == Some('=') {
                    self.pos += 2;
                    CmpOp::Le
                } else {
                    self.pos += 1;
                    CmpOp::Lt
                }
            } else if self.peek() == Some('>') {
                if self.peek_at(1) == Some('=') {
                    self.pos += 2;
                    CmpOp::Ge
                } else {
                    self.pos += 1;
                    CmpOp::Gt
                }
            } else {
                break;
            };
            let right = self.add_expr()?;
            left = Expr::Cmp(op, Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn add_expr(&mut self) -> Result<Expr, XpError> {
        let mut left = self.mul_expr()?;
        loop {
            self.skip_ws();
            let op = if self.eat('+') {
                ArithOp::Add
            } else if self.eat('-') {
                ArithOp::Sub
            } else {
                break;
            };
            let right = self.mul_expr()?;
            left = Expr::Arith(op, Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn mul_expr(&mut self) -> Result<Expr, XpError> {
        let mut left = self.unary_expr()?;
        loop {
            self.skip_ws();
            let op = if self.eat('*') {
                ArithOp::Mul
            } else if self.eat_word("div") {
                ArithOp::Div
            } else if self.eat_word("mod") {
                ArithOp::Mod
            } else {
                break;
            };
            let right = self.unary_expr()?;
            left = Expr::Arith(op, Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn unary_expr(&mut self) -> Result<Expr, XpError> {
        self.skip_ws();
        if self.peek() == Some('-') {
            self.pos += 1;
            let inner = self.unary_expr()?;
            return Ok(Expr::Neg(Box::new(inner)));
        }
        self.union_expr()
    }

    fn union_expr(&mut self) -> Result<Expr, XpError> {
        let mut left = self.path_expr()?;
        loop {
            self.skip_ws();
            if self.peek() != Some('|') {
                break;
            }
            self.pos += 1;
            let right = self.path_expr()?;
            left = Expr::Union(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn path_expr(&mut self) -> Result<Expr, XpError> {
        self.skip_ws();
        // A location path starts with `/`, `//`, `.`, `..`, `@`, `*`, an
        // axis name, or a name test; anything else is a primary expression.
        let starts_path = matches!(self.peek(), Some('/' | '.' | '@' | '*'))
            || self.peek().is_some_and(|c| c.is_alphabetic() || c == '_');
        if !starts_path {
            let primary = self.primary()?;
            return self.filter_tail(primary);
        }
        // A leading name may be a function call (only as a primary), an axis
        // (`axis::`), or a name test.
        if self.peek().is_some_and(|c| c.is_alphabetic() || c == '_') {
            let save = self.pos;
            let name = self.word();
            let mut probe = self.pos;
            while matches!(self.chars.get(probe), Some(c) if c.is_whitespace()) {
                probe += 1;
            }
            // `node()`, `text()`, `comment()`, and
            // `processing-instruction()` are node tests of a step, not
            // function calls.
            let node_test = matches!(
                name.as_str(),
                "node" | "text" | "comment" | "processing-instruction"
            );
            let call = self.chars.get(probe) == Some(&'(') && !node_test;
            self.pos = save;
            if call {
                let primary = self.primary()?;
                return self.filter_tail(primary);
            }
        }
        let mut steps = Vec::new();
        let absolute = if self.eat('/') {
            if self.eat('/') {
                steps.push(Step {
                    axis: Axis::DescendantOrSelf,
                    test: NodeTest::Node,
                    predicates: Vec::new(),
                });
            }
            true
        } else {
            false
        };
        if absolute && self.at_step_end() {
            // `/` alone: the document node.
            return Ok(Expr::Path { absolute, steps });
        }
        self.step(&mut steps)?;
        self.relative_tail(&mut steps)?;
        Ok(Expr::Path { absolute, steps })
    }

    /// Whether the cursor sits at the end of the expression or at `)`, `]`,
    /// or an operator — the places a path may end.
    fn at_step_end(&self) -> bool {
        match self.peek() {
            None | Some(')' | ']' | ',' | '|' | '=') => true,
            Some('/') => false,
            _ => false,
        }
    }

    /// A `[predicate]` tail after a primary expression, and the `/`-steps
    /// that may follow it.
    fn filter_tail(&mut self, primary: Expr) -> Result<Expr, XpError> {
        let mut predicates = self.predicates()?;
        self.skip_ws();
        let mut steps = Vec::new();
        if self.peek() == Some('/') {
            self.pos += 1;
            if self.eat('/') {
                steps.push(Step {
                    axis: Axis::DescendantOrSelf,
                    test: NodeTest::Node,
                    predicates: Vec::new(),
                });
            }
            self.step(&mut steps)?;
            self.relative_tail(&mut steps)?;
        }
        if steps.is_empty() {
            if predicates.is_empty() {
                return Ok(primary);
            }
            predicates.shrink_to_fit();
            return Ok(Expr::Filter(Box::new(primary), predicates));
        }
        // `(expr)/step`: the expression's nodes, then the steps. A predicate
        // on the primary keeps its meaning as a filter.
        let mut all = Vec::new();
        if predicates.is_empty() {
            all.push(Step {
                axis: Axis::SelfAxis,
                test: NodeTest::Node,
                predicates: Vec::new(),
            });
        } else {
            all.push(Step {
                axis: Axis::SelfAxis,
                test: NodeTest::Node,
                predicates,
            });
        }
        all.extend(steps);
        Ok(Expr::Path {
            absolute: false,
            steps: all,
        })
    }

    /// The steps after the first one of a relative path.
    fn relative_tail(&mut self, steps: &mut Vec<Step>) -> Result<(), XpError> {
        loop {
            self.skip_ws();
            if self.peek() != Some('/') {
                break;
            }
            self.pos += 1;
            if self.eat('/') {
                steps.push(Step {
                    axis: Axis::DescendantOrSelf,
                    test: NodeTest::Node,
                    predicates: Vec::new(),
                });
            }
            self.step(steps)?;
        }
        Ok(())
    }

    /// One step: `.`, `..`, `@name`, `axis::test`, or a name test.
    fn step(&mut self, steps: &mut Vec<Step>) -> Result<(), XpError> {
        self.skip_ws();
        if self.peek() == Some('.') && self.peek_at(1) == Some('.') {
            self.pos += 2;
            steps.push(Step {
                axis: Axis::Parent,
                test: NodeTest::Node,
                predicates: Vec::new(),
            });
            return Ok(());
        }
        if self.peek() == Some('.') && !self.peek_at(1).is_some_and(|c| c.is_ascii_digit()) {
            self.pos += 1;
            steps.push(Step {
                axis: Axis::SelfAxis,
                test: NodeTest::Node,
                predicates: Vec::new(),
            });
            return Ok(());
        }
        if self.eat('@') {
            let (test, predicates) = self.step_tail()?;
            steps.push(Step {
                axis: Axis::Attribute,
                test,
                predicates,
            });
            return Ok(());
        }
        // An axis name before `::`.
        if self.peek().is_some_and(|c| c.is_alphabetic() || c == '_') {
            let save = self.pos;
            let name = self.word();
            if self.chars.get(self.pos) == Some(&':') && self.peek_at(1) == Some(':') {
                self.pos += 2;
                let axis = axis_named(&name)?;
                let (test, predicates) = self.step_tail()?;
                steps.push(Step {
                    axis,
                    test,
                    predicates,
                });
                return Ok(());
            }
            self.pos = save;
        }
        let (test, predicates) = self.step_tail()?;
        steps.push(Step {
            axis: Axis::Child,
            test,
            predicates,
        });
        Ok(())
    }

    /// A name test or a node-type test, with its predicates.
    fn step_tail(&mut self) -> Result<(NodeTest, Vec<Expr>), XpError> {
        self.skip_ws();
        let test = if self.eat('*') {
            if self.eat(':') {
                // `prefix:*` is not in XPath 1.0.
                return Err(invalid_expression());
            }
            NodeTest::Name {
                prefix: None,
                local: None,
            }
        } else if self.peek() == Some('@') {
            // `@@name` is not a step in XPath 1.0.
            return Err(invalid_expression());
        } else if self.peek().is_some_and(|c| c.is_alphabetic() || c == '_') {
            let name = self.word();
            if self.peek() == Some('(') {
                self.pos += 1;
                self.skip_ws();
                let test = match name.as_str() {
                    "node" => NodeTest::Node,
                    "text" => NodeTest::Text,
                    "comment" => NodeTest::Comment,
                    "processing-instruction" => {
                        self.skip_ws();
                        if self.peek() == Some('\'') || self.peek() == Some('"') {
                            let quote = self.bump().expect("checked");
                            let mut literal = String::new();
                            loop {
                                match self.bump() {
                                    Some(c) if c == quote => break,
                                    Some(c) => literal.push(c),
                                    None => return Err(invalid_expression()),
                                }
                            }
                            NodeTest::Pi(Some(literal))
                        } else {
                            NodeTest::Pi(None)
                        }
                    }
                    _ => return Err(invalid_expression()),
                };
                self.skip_ws();
                if !self.eat(')') {
                    return Err(invalid_expression());
                }
                test
            } else if self.peek() == Some(':') && self.peek_at(1) != Some(':') {
                self.pos += 1;
                let local = self.word();
                if local.is_empty() {
                    return Err(invalid_expression());
                }
                self.check_prefix(&name)?;
                NodeTest::Name {
                    prefix: Some(name),
                    local: Some(local),
                }
            } else {
                NodeTest::Name {
                    prefix: None,
                    local: Some(name),
                }
            }
        } else {
            return Err(invalid_expression());
        };
        let predicates = self.predicates()?;
        Ok((test, predicates))
    }

    /// A prefix must be one the namespace array declares.
    fn check_prefix(&self, prefix: &str) -> Result<(), XpError> {
        if self.namespaces.iter().any(|(seen, _)| seen == prefix) {
            return Ok(());
        }
        Err(undefined_prefix())
    }

    fn predicates(&mut self) -> Result<Vec<Expr>, XpError> {
        let mut predicates = Vec::new();
        loop {
            self.skip_ws();
            if self.peek() != Some('[') {
                break;
            }
            self.pos += 1;
            let predicate = self.expr()?;
            self.skip_ws();
            if !self.eat(']') {
                return Err(invalid_expression());
            }
            predicates.push(predicate);
        }
        Ok(predicates)
    }

    fn primary(&mut self) -> Result<Expr, XpError> {
        self.skip_ws();
        match self.peek() {
            Some('(') => {
                self.pos += 1;
                let inner = self.expr()?;
                self.skip_ws();
                if !self.eat(')') {
                    return Err(invalid_expression());
                }
                Ok(inner)
            }
            // PostgreSQL's xpath has no variables.
            Some('$') => Err(invalid_expression()),
            Some(quote @ ('\'' | '"')) => {
                self.pos += 1;
                let mut literal = String::new();
                loop {
                    match self.bump() {
                        Some(c) if c == quote => break,
                        Some(c) => literal.push(c),
                        None => return Err(invalid_expression()),
                    }
                }
                Ok(Expr::Literal(literal))
            }
            Some(c) if c.is_ascii_digit() || c == '.' => {
                let mut text = String::new();
                while matches!(self.peek(), Some(c) if c.is_ascii_digit() || c == '.') {
                    text.push(self.bump().expect("checked"));
                }
                if matches!(self.peek(), Some('e' | 'E')) {
                    text.push(self.bump().expect("checked"));
                    if matches!(self.peek(), Some('+' | '-')) {
                        text.push(self.bump().expect("checked"));
                    }
                    while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                        text.push(self.bump().expect("checked"));
                    }
                }
                text.parse::<f64>()
                    .map(Expr::Number)
                    .map_err(|_| invalid_expression())
            }
            Some(c) if c.is_alphabetic() || c == '_' => {
                let name = self.word();
                self.skip_ws();
                if !self.eat('(') {
                    return Err(invalid_expression());
                }
                let mut args = Vec::new();
                self.skip_ws();
                if !self.eat(')') {
                    loop {
                        args.push(self.expr()?);
                        self.skip_ws();
                        if self.eat(',') {
                            continue;
                        }
                        if self.eat(')') {
                            break;
                        }
                        return Err(invalid_expression());
                    }
                }
                Ok(Expr::Call(name, args))
            }
            _ => Err(invalid_expression()),
        }
    }
}

fn axis_named(name: &str) -> Result<Axis, XpError> {
    Ok(match name {
        "child" => Axis::Child,
        "descendant" => Axis::Descendant,
        "descendant-or-self" => Axis::DescendantOrSelf,
        "self" => Axis::SelfAxis,
        "parent" => Axis::Parent,
        "ancestor" => Axis::Ancestor,
        "ancestor-or-self" => Axis::AncestorOrSelf,
        "following-sibling" => Axis::FollowingSibling,
        "preceding-sibling" => Axis::PrecedingSibling,
        "following" => Axis::Following,
        "preceding" => Axis::Preceding,
        "attribute" => Axis::Attribute,
        _ => return Err(invalid_expression()),
    })
}

// ---------------------------------------------------------------------------
// Evaluation.

/// An XPath value: a node-set, a string, a number, or a boolean.
enum Value {
    Nodes(Vec<usize>),
    Str(String),
    Num(f64),
    Bool(bool),
}

impl Value {
    fn string_value(&self, tree: &Tree) -> String {
        match self {
            Value::Str(text) => text.clone(),
            Value::Num(number) => number_to_string(*number),
            Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
            Value::Nodes(nodes) => nodes
                .first()
                .map(|&node| tree.string_value(node))
                .unwrap_or_default(),
        }
    }

    fn number_value(&self, tree: &Tree) -> f64 {
        match self {
            Value::Num(number) => *number,
            Value::Bool(b) => {
                if *b {
                    1.0
                } else {
                    0.0
                }
            }
            Value::Str(text) => text.trim().parse().unwrap_or(f64::NAN),
            Value::Nodes(_) => self.string_value(tree).trim().parse().unwrap_or(f64::NAN),
        }
    }

    fn bool_value(&self) -> bool {
        match self {
            Value::Bool(b) => *b,
            Value::Num(number) => *number != 0.0 && !number.is_nan(),
            Value::Str(text) => !text.is_empty(),
            Value::Nodes(nodes) => !nodes.is_empty(),
        }
    }
}

/// A number as XPath prints one (an integer without a decimal point).
fn number_to_string(number: f64) -> String {
    if number.is_nan() {
        return "NaN".to_string();
    }
    if number == f64::INFINITY {
        return "Infinity".to_string();
    }
    if number == f64::NEG_INFINITY {
        return "-Infinity".to_string();
    }
    if number.fract() == 0.0 && number.abs() < 1e15 {
        return format!("{}", number as i64);
    }
    format!("{number}")
}

#[derive(Clone, Copy)]
struct Context<'a> {
    tree: &'a Tree<'a>,
    ns: &'a [(String, String)],
    node: usize,
    position: usize,
    size: usize,
}

impl<'a> Context<'a> {
    fn at(&self, node: usize, position: usize, size: usize) -> Context<'a> {
        Context {
            tree: self.tree,
            ns: self.ns,
            node,
            position,
            size,
        }
    }

    fn eval(&self, expr: &Expr) -> Result<Value, XpError> {
        Ok(match expr {
            Expr::Or(left, right) => {
                Value::Bool(self.eval(left)?.bool_value() || self.eval(right)?.bool_value())
            }
            Expr::And(left, right) => {
                Value::Bool(self.eval(left)?.bool_value() && self.eval(right)?.bool_value())
            }
            Expr::Neg(inner) => Value::Num(-self.eval(inner)?.number_value(self.tree)),
            Expr::Arith(op, left, right) => {
                let left = self.eval(left)?.number_value(self.tree);
                let right = self.eval(right)?.number_value(self.tree);
                Value::Num(match op {
                    ArithOp::Add => left + right,
                    ArithOp::Sub => left - right,
                    ArithOp::Mul => left * right,
                    ArithOp::Div => left / right,
                    ArithOp::Mod => left % right,
                })
            }
            Expr::Cmp(op, left, right) => {
                let left = self.eval(left)?;
                let right = self.eval(right)?;
                Value::Bool(compare(*op, &left, &right, self.tree))
            }
            Expr::Union(left, right) => {
                let mut nodes = match self.eval(left)? {
                    Value::Nodes(nodes) => nodes,
                    _ => return Err(invalid_expression()),
                };
                match self.eval(right)? {
                    Value::Nodes(other) => nodes.extend(other),
                    _ => return Err(invalid_expression()),
                }
                nodes.sort_unstable();
                nodes.dedup();
                Value::Nodes(nodes)
            }
            Expr::Path { absolute, steps } => {
                let mut value = Value::Nodes(vec![if *absolute { 0 } else { self.node }]);
                for step in steps {
                    value = self.step(step, value)?;
                }
                value
            }
            Expr::Filter(inner, predicates) => {
                let mut value = self.eval(inner)?;
                for predicate in predicates {
                    value = self.filter(value, predicate)?;
                }
                value
            }
            Expr::Call(name, args) => self.call(name, args)?,
            Expr::Number(number) => Value::Num(*number),
            Expr::Literal(text) => Value::Str(text.clone()),
        })
    }

    /// Applies one step to a node-set.
    fn step(&self, step: &Step, value: Value) -> Result<Value, XpError> {
        let Value::Nodes(nodes) = value else {
            return Err(invalid_expression());
        };
        let mut out = Vec::new();
        for node in nodes {
            let mut candidates = self.axis(step.axis, node);
            candidates.retain(|&candidate| self.matches(&step.test, candidate));
            let size = candidates.len();
            for (index, candidate) in candidates.iter().enumerate() {
                if self
                    .at(*candidate, index + 1, size)
                    .predicates(&step.predicates)?
                {
                    out.push(*candidate);
                }
            }
        }
        out.sort_unstable();
        out.dedup();
        Ok(Value::Nodes(out))
    }

    /// Whether a node passes every predicate, at its proximity position.
    fn predicates(&self, predicates: &[Expr]) -> Result<bool, XpError> {
        for predicate in predicates {
            let value = self.eval(predicate)?;
            let keep = match value {
                Value::Num(number) => number == self.position as f64,
                other => other.bool_value(),
            };
            if !keep {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Filters a node-set by a predicate over the whole set.
    fn filter(&self, value: Value, predicate: &Expr) -> Result<Value, XpError> {
        let Value::Nodes(nodes) = value else {
            return Err(invalid_expression());
        };
        let size = nodes.len();
        let mut out = Vec::new();
        for (index, node) in nodes.iter().enumerate() {
            let value = self.at(*node, index + 1, size).eval(predicate)?;
            let keep = match value {
                Value::Num(number) => number == (index + 1) as f64,
                other => other.bool_value(),
            };
            if keep {
                out.push(*node);
            }
        }
        Ok(Value::Nodes(out))
    }

    /// The nodes the axis selects from `node`.
    fn axis(&self, axis: Axis, node: usize) -> Vec<usize> {
        let tree = self.tree;
        match axis {
            Axis::Child => tree.children(node),
            Axis::Attribute => tree.attrs(node),
            Axis::Descendant => {
                let mut out = Vec::new();
                tree.descendants(node, &mut out);
                out.retain(|&candidate| !tree.is_attribute(candidate));
                out
            }
            Axis::DescendantOrSelf => {
                let mut out = vec![node];
                let mut below = Vec::new();
                tree.descendants(node, &mut below);
                below.retain(|&candidate| !tree.is_attribute(candidate));
                out.extend(below);
                out
            }
            Axis::SelfAxis => vec![node],
            Axis::Parent => tree.parent(node).into_iter().collect(),
            Axis::Ancestor => tree.ancestors(node),
            Axis::AncestorOrSelf => {
                let mut out = vec![node];
                out.extend(tree.ancestors(node));
                out
            }
            Axis::FollowingSibling | Axis::PrecedingSibling => match tree.parent(node) {
                Some(parent) => {
                    let siblings = tree.children(parent);
                    match siblings.iter().position(|&sibling| sibling == node) {
                        Some(at) if axis == Axis::FollowingSibling => siblings[at + 1..].to_vec(),
                        Some(at) => siblings[..at].to_vec(),
                        None => Vec::new(),
                    }
                }
                None => Vec::new(),
            },
            Axis::Following | Axis::Preceding => {
                if tree.is_attribute(node) {
                    return Vec::new();
                }
                let mut descendants = Vec::new();
                tree.descendants(node, &mut descendants);
                let ancestors = tree.ancestors(node);
                tree.all_nodes()
                    .into_iter()
                    .filter(|&candidate| {
                        if tree.is_attribute(candidate) {
                            return false;
                        }
                        if axis == Axis::Following {
                            candidate > node && !descendants.contains(&candidate)
                        } else {
                            candidate < node && !ancestors.contains(&candidate)
                        }
                    })
                    .collect()
            }
        }
    }

    /// Whether a node matches a step's node test.
    fn matches(&self, test: &NodeTest, node: usize) -> bool {
        let (_, local, uri) = self.tree.name(node);
        match test {
            NodeTest::Node => true,
            NodeTest::Text => {
                matches!(self.tree.nodes[node].kind, Kind::Text(_) | Kind::Cdata(_))
            }
            NodeTest::Comment => matches!(self.tree.nodes[node].kind, Kind::Comment(_)),
            NodeTest::Pi(name) => {
                matches!(self.tree.nodes[node].kind, Kind::Pi { .. })
                    && name.as_ref().is_none_or(|wanted| wanted == &local)
            }
            NodeTest::Name {
                prefix,
                local: wanted,
            } => {
                if !matches!(
                    self.tree.nodes[node].kind,
                    Kind::Element { .. } | Kind::Attribute { .. }
                ) {
                    return false;
                }
                let wanted_uri = match prefix {
                    Some(prefix) => self
                        .ns
                        .iter()
                        .find(|(seen, _)| seen == prefix)
                        .map(|(_, uri)| uri.clone())
                        .unwrap_or_default(),
                    // An unprefixed name test matches no namespace.
                    None => String::new(),
                };
                if wanted_uri != uri {
                    return false;
                }
                match wanted {
                    Some(wanted) => wanted == &local,
                    None => true,
                }
            }
        }
    }

    fn call(&self, name: &str, args: &[Expr]) -> Result<Value, XpError> {
        let arg = |index: usize| -> Result<Value, XpError> {
            match args.get(index) {
                Some(expr) => self.eval(expr),
                None => Err(invalid_expression()),
            }
        };
        let no_args = |count: usize| -> Result<(), XpError> {
            if args.len() == count {
                Ok(())
            } else {
                Err(invalid_expression())
            }
        };
        Ok(match name {
            "true" => {
                no_args(0)?;
                Value::Bool(true)
            }
            "false" => {
                no_args(0)?;
                Value::Bool(false)
            }
            "not" => Value::Bool(!arg(0)?.bool_value()),
            "boolean" => Value::Bool(arg(0)?.bool_value()),
            "string" => Value::Str(match args.len() {
                0 => self.tree.string_value(self.node),
                _ => arg(0)?.string_value(self.tree),
            }),
            "number" => Value::Num(match args.len() {
                0 => self
                    .tree
                    .string_value(self.node)
                    .trim()
                    .parse()
                    .unwrap_or(f64::NAN),
                _ => arg(0)?.number_value(self.tree),
            }),
            "position" => {
                no_args(0)?;
                Value::Num(self.position as f64)
            }
            "last" => {
                no_args(0)?;
                Value::Num(self.size as f64)
            }
            "count" => match arg(0)? {
                Value::Nodes(nodes) => Value::Num(nodes.len() as f64),
                _ => return Err(invalid_expression()),
            },
            "name" | "local-name" | "namespace-uri" => {
                let node = match args.len() {
                    0 => Some(self.node),
                    _ => match arg(0)? {
                        Value::Nodes(nodes) => nodes.first().copied(),
                        _ => return Err(invalid_expression()),
                    },
                };
                let (_, local, uri) = node.map_or_else(
                    || (String::new(), String::new(), String::new()),
                    |node| self.tree.name(node),
                );
                Value::Str(match name {
                    "local-name" => local,
                    "namespace-uri" => uri,
                    _ => local,
                })
            }
            "text" => match arg(0)? {
                Value::Nodes(nodes) => Value::Str(
                    nodes
                        .first()
                        .map(|&node| self.tree.string_value(node))
                        .unwrap_or_default(),
                ),
                other => Value::Str(other.string_value(self.tree)),
            },
            "concat" => {
                let mut out = String::new();
                for index in 0..args.len() {
                    out.push_str(&arg(index)?.string_value(self.tree));
                }
                Value::Str(out)
            }
            "starts-with" => {
                let haystack = arg(0)?.string_value(self.tree);
                let needle = arg(1)?.string_value(self.tree);
                Value::Bool(haystack.starts_with(&needle))
            }
            "contains" => {
                let haystack = arg(0)?.string_value(self.tree);
                let needle = arg(1)?.string_value(self.tree);
                Value::Bool(haystack.contains(&needle))
            }
            "substring-before" => {
                let haystack = arg(0)?.string_value(self.tree);
                let needle = arg(1)?.string_value(self.tree);
                Value::Str(match haystack.find(&needle) {
                    Some(at) => haystack[..at].to_string(),
                    None => String::new(),
                })
            }
            "substring-after" => {
                let haystack = arg(0)?.string_value(self.tree);
                let needle = arg(1)?.string_value(self.tree);
                Value::Str(match haystack.find(&needle) {
                    Some(at) => haystack[at + needle.len()..].to_string(),
                    None => String::new(),
                })
            }
            "substring" => {
                let text: Vec<char> = arg(0)?.string_value(self.tree).chars().collect();
                let start = arg(1)?.number_value(self.tree);
                let length = if args.len() > 2 {
                    Some(arg(2)?.number_value(self.tree))
                } else {
                    None
                };
                let start = if start.is_nan() {
                    f64::NAN
                } else {
                    start.round()
                };
                let from = if start.is_nan() || start < 1.0 {
                    0
                } else {
                    start as usize - 1
                };
                let to = match length {
                    Some(length) if length.is_nan() => from,
                    Some(length) => {
                        let end = start + length.round().max(0.0);
                        if end.is_nan() || end < 1.0 {
                            0
                        } else {
                            (end as usize).saturating_sub(1)
                        }
                    }
                    None => text.len(),
                };
                let from = from.min(text.len());
                let to = to.clamp(from, text.len());
                Value::Str(text[from..to].iter().collect())
            }
            "string-length" => {
                let text = match args.len() {
                    0 => self.tree.string_value(self.node),
                    _ => arg(0)?.string_value(self.tree),
                };
                Value::Num(text.chars().count() as f64)
            }
            "normalize-space" => {
                let text = match args.len() {
                    0 => self.tree.string_value(self.node),
                    _ => arg(0)?.string_value(self.tree),
                };
                Value::Str(text.split_whitespace().collect::<Vec<_>>().join(" "))
            }
            "translate" => {
                let text: Vec<char> = arg(0)?.string_value(self.tree).chars().collect();
                let from: Vec<char> = arg(1)?.string_value(self.tree).chars().collect();
                let to: Vec<char> = arg(2)?.string_value(self.tree).chars().collect();
                let mut out = String::new();
                for c in text {
                    match from.iter().position(|f| *f == c) {
                        Some(at) if at < to.len() => out.push(to[at]),
                        Some(_) => {}
                        None => out.push(c),
                    }
                }
                Value::Str(out)
            }
            "sum" => match arg(0)? {
                Value::Nodes(nodes) => {
                    let mut total = 0.0;
                    for node in nodes {
                        total += self
                            .tree
                            .string_value(node)
                            .trim()
                            .parse::<f64>()
                            .unwrap_or(f64::NAN);
                    }
                    Value::Num(total)
                }
                _ => return Err(invalid_expression()),
            },
            // `id()` finds elements by a DTD's ID attributes, which a
            // PostgreSQL value does not carry.
            "id" => Value::Nodes(Vec::new()),
            "floor" => Value::Num(arg(0)?.number_value(self.tree).floor()),
            "ceiling" => Value::Num(arg(0)?.number_value(self.tree).ceil()),
            "round" => {
                let number = arg(0)?.number_value(self.tree);
                Value::Num((number + 0.5).floor())
            }
            _ => return Err(invalid_expression()),
        })
    }
}

/// An XPath comparison, with the node-set rules: a comparison over a
/// node-set holds when any of its nodes makes it hold.
fn compare(op: CmpOp, left: &Value, right: &Value, tree: &Tree) -> bool {
    let pairs: (Vec<Value>, Vec<Value>) = match (left, right) {
        (Value::Nodes(left), Value::Nodes(right)) => (
            left.iter()
                .map(|&n| Value::Str(tree.string_value(n)))
                .collect(),
            right
                .iter()
                .map(|&n| Value::Str(tree.string_value(n)))
                .collect(),
        ),
        (Value::Nodes(left), other) => (
            left.iter()
                .map(|&n| Value::Str(tree.string_value(n)))
                .collect(),
            vec![clone_value(other)],
        ),
        (other, Value::Nodes(right)) => (
            vec![clone_value(other)],
            right
                .iter()
                .map(|&n| Value::Str(tree.string_value(n)))
                .collect(),
        ),
        (left, right) => (vec![clone_value(left)], vec![clone_value(right)]),
    };
    for left in &pairs.0 {
        for right in &pairs.1 {
            if compare_one(op, left, right, tree) {
                return true;
            }
        }
    }
    false
}

fn clone_value(value: &Value) -> Value {
    match value {
        Value::Str(text) => Value::Str(text.clone()),
        Value::Num(number) => Value::Num(*number),
        Value::Bool(b) => Value::Bool(*b),
        Value::Nodes(nodes) => Value::Nodes(nodes.clone()),
    }
}

fn compare_one(op: CmpOp, left: &Value, right: &Value, tree: &Tree) -> bool {
    // A boolean or numeric operand compares as numbers, otherwise both sides
    // are strings (`<` never compares a string and a number as text).
    if matches!(left, Value::Bool(_)) || matches!(right, Value::Bool(_)) {
        let left = left.bool_value();
        let right = right.bool_value();
        return match op {
            CmpOp::Eq => left == right,
            CmpOp::Ne => left != right,
            CmpOp::Lt => !left & right,
            CmpOp::Le => !left | right,
            CmpOp::Gt => left & !right,
            CmpOp::Ge => left | !right,
        };
    }
    if matches!(left, Value::Num(_)) || matches!(right, Value::Num(_)) {
        let left = left.number_value(tree);
        let right = right.number_value(tree);
        return match op {
            CmpOp::Eq => left == right,
            CmpOp::Ne => left != right,
            CmpOp::Lt => left < right,
            CmpOp::Le => left <= right,
            CmpOp::Gt => left > right,
            CmpOp::Ge => left >= right,
        };
    }
    let left = left.string_value(tree);
    let right = right.string_value(tree);
    match op {
        CmpOp::Eq => left == right,
        CmpOp::Ne => left != right,
        CmpOp::Lt => left < right,
        CmpOp::Le => left <= right,
        CmpOp::Gt => left > right,
        CmpOp::Ge => left >= right,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::xml::Mode;

    fn document(text: &str) -> Document {
        crate::xml::parse(text, Mode::Document, true).expect("well-formed")
    }

    fn selected(expression: &str, text: &str) -> String {
        match xpath(expression, &document(text), &[]) {
            Ok(nodes) => format!("{{{}}}", nodes.join(",")),
            Err(error) => format!("error: {}", error.message),
        }
    }

    fn with_ns(expression: &str, text: &str, ns: &[(&str, &str)]) -> String {
        let ns: Vec<(String, String)> = ns
            .iter()
            .map(|(p, u)| (p.to_string(), u.to_string()))
            .collect();
        match xpath(expression, &document(text), &ns) {
            Ok(nodes) => format!("{{{}}}", nodes.join(",")),
            Err(error) => format!("error: {}", error.message),
        }
    }

    #[test]
    fn paths_match_postgresql() {
        assert_eq!(selected("/a", "<a>1</a>"), "{<a>1</a>}");
        assert_eq!(
            selected("/a/b", "<a><b>1</b><b>2</b></a>"),
            "{<b>1</b>,<b>2</b>}"
        );
        assert_eq!(
            selected("//b", "<a><b>1</b><c><b>2</b></c></a>"),
            "{<b>1</b>,<b>2</b>}"
        );
        assert_eq!(selected("//q", "<a/>"), "{}");
        assert_eq!(selected("/a/b/text()", "<a><b>1</b><c>x</c></a>"), "{1}");
        assert_eq!(selected("/a/@id", "<a id=\"5\"/>"), "{5}");
        assert_eq!(selected("//@*", "<a x=\"1\" y=\"2\"/>"), "{1,2}");
        assert_eq!(selected("/a/*", "<a><b/><c/></a>"), "{<b/>,<c/>}");
        // The context node is the document node: relative paths find
        // nothing, `.` is the document, and a text node is its escaped text.
        assert_eq!(selected("b", "<a><b>1</b></a>"), "{}");
        assert_eq!(selected("@id", "<a id=\"7\"/>"), "{}");
        assert_eq!(
            selected("/a/..", "<a/>"),
            "{<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<a/>\n}"
        );
        assert_eq!(selected("//text()", "<a>x<b>y</b></a>"), "{x,y}");
        assert_eq!(selected("//text()", "<a>a&amp;b</a>"), "{a&amp;b}");
        assert_eq!(selected("//b/../@id", "<a id=\"3\"><b/></a>"), "{3}");
        assert_eq!(selected("/a/descendant::b", "<a><c><b/></c></a>"), "{<b/>}");
        assert_eq!(
            selected("/a/comment()", "<a><!-- c --></a>"),
            "{<!-- c -->}"
        );
        assert_eq!(selected("node()", "<a>x<b/>y</a>"), "{<a>x<b/>y</a>}");
        assert_eq!(selected("//b|//a", "<a><b/></a>"), "{<a><b/></a>,<b/>}");
    }

    #[test]
    fn predicates_and_expressions_match_postgresql() {
        assert_eq!(selected("/a/b[1]", "<a><b>1</b><b>2</b></a>"), "{<b>1</b>}");
        assert_eq!(
            selected("/a/b[last()]", "<a><b>1</b><b>2</b></a>"),
            "{<b>2</b>}"
        );
        assert_eq!(
            selected("/a/b[@x=\"1\"]", "<a><b x=\"1\">y</b><b x=\"2\">z</b></a>"),
            "{<b x=\"1\">y</b>}"
        );
        assert_eq!(
            selected("/a/b[position() > 1]", "<a><b>1</b><b>2</b></a>"),
            "{<b>2</b>}"
        );
        assert_eq!(
            selected("//b[. = \"1\"]", "<a><b>1</b><b>2</b></a>"),
            "{<b>1</b>}"
        );
        assert_eq!(
            selected("//b[contains(., \"2\")]", "<a><b>1</b><b>2</b></a>"),
            "{<b>2</b>}"
        );
        assert_eq!(selected("count(/a/b)", "<a><b>1</b><b>2</b></a>"), "{2}");
        assert_eq!(selected("sum(//b)", "<a><b>1</b><b>2</b></a>"), "{3}");
        assert_eq!(selected("string(/a/b)", "<a><b>1</b></a>"), "{1}");
        assert_eq!(selected("string(//b)", "<a><b>1<c/>2</b></a>"), "{12}");
        assert_eq!(
            selected("/a/b | /a/c", "<a><b>1</b><c>2</c></a>"),
            "{<b>1</b>,<c>2</c>}"
        );
        assert_eq!(selected("1 + 2", "<a/>"), "{3}");
        assert_eq!(selected("1 div 2", "<a/>"), "{0.5}");
        assert_eq!(selected("1 = 1", "<a/>"), "{true}");
        assert_eq!(selected("not(/a)", "<a/>"), "{false}");
        assert_eq!(selected("true()", "<a/>"), "{true}");
        assert_eq!(selected("concat('a', 'b')", "<a/>"), "{ab}");
        assert_eq!(selected("string-length('abc')", "<a/>"), "{3}");
        assert_eq!(selected("substring('12345', 2, 3)", "<a/>"), "{234}");
        assert_eq!(selected("normalize-space('  a  b ')", "<a/>"), "{a b}");
        assert_eq!(selected("floor(1.7)", "<a/>"), "{1}");
        assert_eq!(selected("number('12')", "<a/>"), "{12}");
        assert_eq!(selected("name(//b)", "<a><b/></a>"), "{b}");
        assert_eq!(selected("local-name(//b)", "<a><b/></a>"), "{b}");
        assert_eq!(selected("boolean(//b)", "<a><b/></a>"), "{true}");
        assert_eq!(
            selected("//b/following-sibling::c", "<a><b/><c/></a>"),
            "{<c/>}"
        );
        assert_eq!(
            selected("//b/ancestor::a", "<a><c><b/></c></a>"),
            "{<a><c><b/></c></a>}"
        );
        assert_eq!(selected("id('x')", "<a id=\"x\"/>"), "{}");
    }

    #[test]
    fn namespaces_match_postgresql() {
        assert_eq!(
            with_ns("/x:a", "<x:a xmlns:x=\"urn:x\"/>", &[("x", "urn:x")]),
            "{<x:a xmlns:x=\"urn:x\"/>}"
        );
        // A prefix comes from the array alone, never from the document.
        assert_eq!(
            selected("/x:a", "<x:a xmlns:x=\"urn:x\"/>"),
            "error: could not create XPath object"
        );
        // An unprefixed name matches no namespace, so a default-namespaced
        // element is not one.
        assert_eq!(selected("/a", "<a xmlns=\"urn:u\"/>"), "{}");
    }

    #[test]
    fn bad_expressions_match_postgresql() {
        assert_eq!(selected("", "<a/>"), "error: empty XPath expression");
        assert_eq!(selected("bad[", "<a/>"), "error: invalid XPath expression");
        assert_eq!(
            selected("//b/string()", "<a/>"),
            "error: invalid XPath expression"
        );
    }
}
