//! Geometric types: point, lseg, box, path, polygon, line, and circle.
//! Values are canonical text typed by the declared column type, as the other
//! structured types are.

use crate::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Point,
    Lseg,
    Box,
    Path,
    Polygon,
    Line,
    Circle,
}

/// Every geometric type, in OID order, for the catalogs.
pub(crate) const KINDS: [Kind; 7] = [
    Kind::Point,
    Kind::Lseg,
    Kind::Path,
    Kind::Box,
    Kind::Polygon,
    Kind::Line,
    Kind::Circle,
];

impl Kind {
    /// The geometric type a declared type names (`point`, `pg_catalog.box`).
    pub(crate) fn of(data_type: &str) -> Option<Kind> {
        let upper = data_type.trim().to_ascii_uppercase();
        let name = upper.rsplit_once('.').map_or(upper.as_str(), |(_, n)| n);
        Some(match name.trim().trim_matches('"') {
            "POINT" => Kind::Point,
            "LSEG" => Kind::Lseg,
            "BOX" => Kind::Box,
            "PATH" => Kind::Path,
            "POLYGON" => Kind::Polygon,
            "LINE" => Kind::Line,
            "CIRCLE" => Kind::Circle,
            _ => return None,
        })
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Kind::Point => "point",
            Kind::Lseg => "lseg",
            Kind::Box => "box",
            Kind::Path => "path",
            Kind::Polygon => "polygon",
            Kind::Line => "line",
            Kind::Circle => "circle",
        }
    }

    pub(crate) fn oid(self) -> i64 {
        match self {
            Kind::Point => 600,
            Kind::Lseg => 601,
            Kind::Path => 602,
            Kind::Box => 603,
            Kind::Polygon => 604,
            Kind::Line => 628,
            Kind::Circle => 718,
        }
    }

    pub(crate) fn array_oid(self) -> i64 {
        match self {
            Kind::Point => 1017,
            Kind::Lseg => 1018,
            Kind::Path => 1019,
            Kind::Box => 1020,
            Kind::Polygon => 1027,
            Kind::Line => 629,
            Kind::Circle => 719,
        }
    }

    pub(crate) fn of_oid(oid: i64) -> Option<Kind> {
        KINDS.into_iter().find(|kind| kind.oid() == oid)
    }

    pub(crate) fn of_array_oid(oid: i64) -> Option<Kind> {
        KINDS.into_iter().find(|kind| kind.array_oid() == oid)
    }

    /// `pg_type.typlen`, as PostgreSQL's fixed-size types declare it.
    pub(crate) fn typlen(self) -> i64 {
        match self {
            Kind::Point => 16,
            Kind::Lseg | Kind::Box => 32,
            Kind::Line | Kind::Circle => 24,
            Kind::Path | Kind::Polygon => -1,
        }
    }

    /// `pg_type.typstorage` (`p`lain for the fixed-size types).
    pub(crate) fn tystorage(self) -> &'static str {
        match self {
            Kind::Point | Kind::Lseg | Kind::Box | Kind::Line | Kind::Circle => "p",
            Kind::Path | Kind::Polygon => "x",
        }
    }
}

/// Whether a declared type names a geometric type.
pub(crate) fn is_geometric_type(data_type: &str) -> bool {
    Kind::of(data_type).is_some()
}

/// A point, the building block of every geometric shape.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Pt {
    pub x: f64,
    pub y: f64,
}

impl Pt {
    fn add(self, o: Pt) -> Pt {
        Pt {
            x: self.x + o.x,
            y: self.y + o.y,
        }
    }
    fn sub(self, o: Pt) -> Pt {
        Pt {
            x: self.x - o.x,
            y: self.y - o.y,
        }
    }
    /// The complex product, as PostgreSQL multiplies points.
    fn mul(self, o: Pt) -> Pt {
        Pt {
            x: self.x * o.x - self.y * o.y,
            y: self.x * o.y + self.y * o.x,
        }
    }
    /// The complex quotient.
    fn div(self, o: Pt) -> Pt {
        let denom = o.x * o.x + o.y * o.y;
        Pt {
            x: (self.x * o.x + self.y * o.y) / denom,
            y: (self.y * o.x - self.x * o.y) / denom,
        }
    }
    fn distance(self, o: Pt) -> f64 {
        ((self.x - o.x).powi(2) + (self.y - o.y).powi(2)).sqrt()
    }
    /// Whether PostgreSQL considers the points equal (`~=`): each coordinate
    /// within `EPSILON` (1e-6), exactly when a NaN is involved.
    fn close(self, o: Pt) -> bool {
        let close = |a: f64, b: f64| {
            if a.is_nan() || b.is_nan() {
                a == b
            } else {
                a == b || (a - b).abs() <= 1e-6
            }
        };
        close(self.x, o.x) && close(self.y, o.y)
    }
}

/// A parsed geometric value.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Shape {
    Point(Pt),
    Lseg(Pt, Pt),
    /// Canonical: `high` is the upper-right corner.
    Box {
        high: Pt,
        low: Pt,
    },
    Path {
        closed: bool,
        points: Vec<Pt>,
    },
    Polygon(Vec<Pt>),
    /// `A x + B y + C = 0`, normalized so `A > 0`, or `A == 0` and `B > 0`.
    Line {
        a: f64,
        b: f64,
        c: f64,
    },
    Circle {
        center: Pt,
        radius: f64,
    },
}

/// A number as PostgreSQL writes a float8.
fn num(value: f64) -> String {
    crate::value::float_text(value)
}

fn point_text(p: Pt) -> String {
    format!("({},{})", num(p.x), num(p.y))
}

/// The value as PostgreSQL writes it.
pub(crate) fn format(shape: &Shape) -> String {
    match shape {
        Shape::Point(p) => point_text(*p),
        Shape::Lseg(a, b) => format!("[{},{}]", point_text(*a), point_text(*b)),
        Shape::Box { high, low } => format!("{},{}", point_text(*high), point_text(*low)),
        Shape::Path { closed, points } => {
            let body: Vec<String> = points.iter().map(|p| point_text(*p)).collect();
            if *closed {
                format!("({})", body.join(","))
            } else {
                format!("[{}]", body.join(","))
            }
        }
        Shape::Polygon(points) => {
            let body: Vec<String> = points.iter().map(|p| point_text(*p)).collect();
            format!("({})", body.join(","))
        }
        Shape::Line { a, b, c } => format!("{{{},{},{}}}", num(*a), num(*b), num(*c)),
        Shape::Circle { center, radius } => {
            format!("<{},{}>", point_text(*center), num(*radius))
        }
    }
}

fn malformed(kind: Kind, text: &str) -> String {
    crate::error_fields::DbError::new(format!(
        "invalid input syntax for type {}: \"{text}\"",
        kind.name()
    ))
    .code("22P02")
    .into_text()
}

/// A cursor over a literal's characters.
struct Cursor {
    chars: Vec<char>,
    i: usize,
}

impl Cursor {
    fn new(text: &str) -> Self {
        Cursor {
            chars: text.chars().collect(),
            i: 0,
        }
    }
    fn skip_ws(&mut self) {
        while self.i < self.chars.len() && self.chars[self.i].is_whitespace() {
            self.i += 1;
        }
    }
    fn peek(&self) -> Option<char> {
        self.chars.get(self.i).copied()
    }
    fn eat(&mut self, c: char) -> bool {
        self.skip_ws();
        if self.peek() == Some(c) {
            self.i += 1;
            return true;
        }
        false
    }
    fn at_end(&mut self) -> bool {
        self.skip_ws();
        self.i >= self.chars.len()
    }
    /// A number: PostgreSQL's float8 input forms.
    fn number(&mut self) -> Option<f64> {
        self.skip_ws();
        let start = self.i;
        while let Some(c) = self.peek() {
            if c.is_ascii_digit() || matches!(c, '.' | '+' | '-' | 'e' | 'E') || c.is_alphabetic() {
                self.i += 1;
            } else {
                break;
            }
        }
        let text: String = self.chars[start..self.i].iter().collect();
        crate::value::parse_float_text(&text, false).ok()
    }
    fn point(&mut self) -> Option<Pt> {
        if !self.eat('(') {
            return None;
        }
        let x = self.number()?;
        if !self.eat(',') {
            return None;
        }
        let y = self.number()?;
        if !self.eat(')') {
            return None;
        }
        Some(Pt { x, y })
    }
    /// A comma-separated list of points, ended by `close`.
    fn points(&mut self, close: char) -> Option<Vec<Pt>> {
        let mut points = vec![self.point()?];
        loop {
            if self.eat(close) {
                return Some(points);
            }
            if !self.eat(',') {
                return None;
            }
            points.push(self.point()?);
        }
    }
}

/// Parses canonical text back into its shape.
pub(crate) fn parse(kind: Kind, text: &str) -> Result<Shape, String> {
    let trimmed = text.trim();
    if kind == Kind::Line
        && let Ok(Shape::Line { a, b, .. }) = parse_line_loose(trimmed)
        && a == 0.0
        && b == 0.0
    {
        return Err(crate::error_fields::DbError::new(
            "invalid line specification: A and B cannot both be zero",
        )
        .code("22P02")
        .into_text());
    }
    let parsed = match kind {
        Kind::Point => {
            let mut c = Cursor::new(trimmed);
            match c.point() {
                Some(p) if c.at_end() => Some(Shape::Point(p)),
                _ => None,
            }
        }
        Kind::Lseg => {
            // Both `[(x,y),(x,y)]` and `((x,y),(x,y))` are accepted.
            let pair = |text: &str, open: char, close: char| -> Option<(Pt, Pt)> {
                let mut c = Cursor::new(text);
                if !c.eat(open) {
                    return None;
                }
                let a = c.point()?;
                if !c.eat(',') {
                    return None;
                }
                let b = c.point()?;
                if !c.eat(close) || !c.at_end() {
                    return None;
                }
                Some((a, b))
            };
            pair(trimmed, '[', ']')
                .or_else(|| pair(trimmed, '(', ')'))
                .map(|(a, b)| Shape::Lseg(a, b))
        }
        Kind::Box => {
            // Both `(x,y),(x,y)` and `((x,y),(x,y))` are accepted.
            let points = |text: &str| -> Option<(Pt, Pt)> {
                let mut c = Cursor::new(text);
                let a = c.point()?;
                if !c.eat(',') {
                    return None;
                }
                let b = c.point()?;
                if !c.at_end() {
                    return None;
                }
                Some((a, b))
            };
            let parenthesized = |text: &str| -> Option<(Pt, Pt)> {
                let mut c = Cursor::new(text);
                if !c.eat('(') {
                    return None;
                }
                let a = c.point()?;
                if !c.eat(',') {
                    return None;
                }
                let b = c.point()?;
                if !c.eat(')') || !c.at_end() {
                    return None;
                }
                Some((a, b))
            };
            let pair = points(trimmed).or_else(|| parenthesized(trimmed));
            pair.map(|(a, b)| boxed(a, b))
        }
        Kind::Path => {
            let mut c = Cursor::new(trimmed);
            let closed = if c.eat('[') {
                false
            } else if c.eat('(') {
                true
            } else {
                return Err(malformed(kind, text));
            };
            let close = if closed { ')' } else { ']' };
            match c.points(close) {
                Some(points) if c.at_end() => Some(Shape::Path { closed, points }),
                _ => None,
            }
        }
        Kind::Polygon => {
            let mut c = Cursor::new(trimmed);
            if !c.eat('(') {
                None
            } else {
                c.points(')').map(Shape::Polygon)
            }
        }
        Kind::Line => {
            let mut c = Cursor::new(trimmed);
            if !c.eat('{') {
                None
            } else {
                match (c.number(), c.eat(','), c.number(), c.eat(','), c.number()) {
                    (Some(a), true, Some(b), true, Some(cc))
                        if c.eat('}') && c.at_end() && !(a == 0.0 && b == 0.0) =>
                    {
                        Some(Shape::Line { a, b, c: cc })
                    }
                    _ => None,
                }
            }
        }
        Kind::Circle => {
            let mut c = Cursor::new(trimmed);
            if !c.eat('<') {
                None
            } else {
                let center = c.point();
                if center.is_none() || !c.eat(',') {
                    None
                } else {
                    let radius = c.number();
                    match (center, radius) {
                        (Some(center), Some(radius)) if c.eat('>') && c.at_end() => {
                            Some(Shape::Circle {
                                center,
                                radius: radius.abs(),
                            })
                        }
                        _ => None,
                    }
                }
            }
        }
    };
    parsed.ok_or_else(|| malformed(kind, text))
}

/// A literal as the type takes it: parsed and canonical. A circle's radius
/// must not be negative in a literal (its constructor allows one).
pub(crate) fn from_literal(kind: Kind, text: &str) -> Result<String, String> {
    if kind == Kind::Circle
        && let Ok(Shape::Circle { radius, .. }) = parse(Kind::Circle, text)
        && text.trim_start().contains(",-")
        && radius != 0.0
    {
        return Err(malformed(kind, text));
    }
    let shape = parse(kind, text).map_err(|error| {
        // A line's own message when both coefficients are zero.
        if kind == Kind::Line
            && let Ok(Shape::Line { a, b, .. }) = parse_line_loose(text)
            && a == 0.0
            && b == 0.0
        {
            return crate::error_fields::DbError::new(
                "invalid line specification: A and B cannot both be zero",
            )
            .code("22P02")
            .into_text();
        }
        error
    })?;
    Ok(format(&canonicalize(shape)))
}

/// A line literal without the zero check, for the error message.
fn parse_line_loose(text: &str) -> Result<Shape, String> {
    let mut c = Cursor::new(text.trim());
    if !c.eat('{') {
        return Err(String::new());
    }
    match (c.number(), c.eat(','), c.number(), c.eat(','), c.number()) {
        (Some(a), true, Some(b), true, Some(cc)) if c.eat('}') => Ok(Shape::Line { a, b, c: cc }),
        _ => Err(String::new()),
    }
}

/// A box's corners in PostgreSQL's order: the upper right first.
pub(crate) fn boxed(a: Pt, b: Pt) -> Shape {
    Shape::Box {
        high: Pt {
            x: a.x.max(b.x),
            y: a.y.max(b.y),
        },
        low: Pt {
            x: a.x.min(b.x),
            y: a.y.min(b.y),
        },
    }
}

/// The shape in its canonical written form: a literal is kept as written.
fn canonicalize(shape: Shape) -> Shape {
    shape
}

/// `point(x, y)`.
pub(crate) fn point(x: f64, y: f64) -> Value {
    Value::Text(point_text(Pt { x, y }))
}

fn points_of(shape: &Shape) -> Vec<Pt> {
    match shape {
        Shape::Point(p) => vec![*p],
        Shape::Lseg(a, b) => vec![*a, *b],
        Shape::Box { high, low } => vec![*high, *low],
        Shape::Path { points, .. } | Shape::Polygon(points) => points.clone(),
        Shape::Line { .. } | Shape::Circle { .. } => Vec::new(),
    }
}

/// The bounding box of a shape.
pub(crate) fn bound_box(shape: &Shape) -> Option<Shape> {
    if let Shape::Circle { center, radius } = shape {
        return Some(boxed(
            Pt {
                x: center.x + radius,
                y: center.y + radius,
            },
            Pt {
                x: center.x - radius,
                y: center.y - radius,
            },
        ));
    }
    let points = points_of(shape);
    let mut iter = points.iter();
    let first = *iter.next()?;
    let mut high = first;
    let mut low = first;
    for p in iter {
        high = Pt {
            x: high.x.max(p.x),
            y: high.y.max(p.y),
        };
        low = Pt {
            x: low.x.min(p.x),
            y: low.y.min(p.y),
        };
    }
    Some(boxed(low, high))
}

/// `center` and `@@`: a shape's centre.
pub(crate) fn center(shape: &Shape) -> Option<Pt> {
    match shape {
        Shape::Point(p) => Some(*p),
        Shape::Lseg(a, b) => Some(Pt {
            x: (a.x + b.x) / 2.0,
            y: (a.y + b.y) / 2.0,
        }),
        Shape::Box { high, low } => Some(Pt {
            x: (high.x + low.x) / 2.0,
            y: (high.y + low.y) / 2.0,
        }),
        Shape::Circle { center, .. } => Some(*center),
        Shape::Path { points, .. } | Shape::Polygon(points) => {
            let n = points.len() as f64;
            if n == 0.0 {
                return None;
            }
            Some(Pt {
                x: points.iter().map(|p| p.x).sum::<f64>() / n,
                y: points.iter().map(|p| p.y).sum::<f64>() / n,
            })
        }
        Shape::Line { .. } => None,
    }
}

/// The length of an lseg or a path's segments.
pub(crate) fn length(shape: &Shape) -> f64 {
    match shape {
        Shape::Lseg(a, b) => a.distance(*b),
        Shape::Path { closed, points } => {
            let mut total = 0.0;
            for pair in points.windows(2) {
                total += pair[0].distance(pair[1]);
            }
            if *closed && let (Some(first), Some(last)) = (points.first(), points.last()) {
                total += first.distance(*last);
            }
            total
        }
        _ => 0.0,
    }
}

/// PostgreSQL's comparison of two areas, lengths, or counts: `FPlt` adds
/// `EPSILON` (1e-6) before comparing.
fn fuzzy_cmp(a: f64, b: f64) -> std::cmp::Ordering {
    if a + 1e-6 < b {
        std::cmp::Ordering::Less
    } else if a > b + 1e-6 {
        std::cmp::Ordering::Greater
    } else {
        std::cmp::Ordering::Equal
    }
}

/// The comparison PostgreSQL's `<`, `<=`, `>`, and `>=` use for a type: boxes
/// and circles by area, lsegs by length, paths by their point count and then
/// their points.
pub(crate) fn cmp(kind: Kind, a: &Shape, b: &Shape) -> std::cmp::Ordering {
    match kind {
        Kind::Box => fuzzy_cmp(area(a), area(b)),
        Kind::Circle => fuzzy_cmp(area(a), area(b)),
        Kind::Lseg => fuzzy_cmp(length(a), length(b)),
        Kind::Path => {
            let (Shape::Path { points: pa, .. }, Shape::Path { points: pb, .. }) = (a, b) else {
                return std::cmp::Ordering::Equal;
            };
            pa.len().cmp(&pb.len()).then_with(|| cmp_points(pa, pb))
        }
        _ => std::cmp::Ordering::Equal,
    }
}

fn cmp_point(a: Pt, b: Pt) -> std::cmp::Ordering {
    cmp_f64(a.x, b.x).then_with(|| cmp_f64(a.y, b.y))
}

fn cmp_f64(a: f64, b: f64) -> std::cmp::Ordering {
    a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal)
}

fn cmp_points(a: &[Pt], b: &[Pt]) -> std::cmp::Ordering {
    for (x, y) in a.iter().zip(b) {
        let ord = cmp_point(*x, *y);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

/// `=`: by area for boxes and circles, pointwise for lsegs and paths, and by
/// proportional coefficients for lines.
fn eq(kind: Kind, a: &Shape, b: &Shape) -> bool {
    match (a, b) {
        (Shape::Point(p), Shape::Point(q)) => p == q,
        (Shape::Lseg(a1, a2), Shape::Lseg(b1, b2)) => a1 == b1 && a2 == b2,
        (Shape::Box { .. }, Shape::Box { .. }) => (area(a) - area(b)).abs() <= 1e-6,
        (
            Shape::Path {
                closed: ca,
                points: pa,
            },
            Shape::Path {
                closed: cb,
                points: pb,
            },
        ) => ca == cb && pa == pb,
        (
            Shape::Line {
                a: a1,
                b: b1,
                c: c1,
            },
            Shape::Line {
                a: a2,
                b: b2,
                c: c2,
            },
        ) => {
            let _ = kind;
            // Proportional coefficients describe the same line.
            let scale = [a1, b1, c1]
                .into_iter()
                .zip([a2, b2, c2])
                .find_map(|(x, y)| if *y != 0.0 { Some(x / y) } else { None });
            match scale {
                Some(s) => {
                    (a1 - s * a2).abs() <= 1e-6 * s.abs().max(1.0)
                        && (b1 - s * b2).abs() <= 1e-6 * s.abs().max(1.0)
                        && (c1 - s * c2).abs() <= 1e-6 * s.abs().max(1.0)
                }
                None => {
                    [a1, b1, c1].iter().all(|v| **v == 0.0)
                        && [a2, b2, c2].iter().all(|v| **v == 0.0)
                }
            }
        }
        (Shape::Circle { .. }, Shape::Circle { .. }) => (area(a) - area(b)).abs() <= 1e-6,
        _ => false,
    }
}

/// `~=`: PostgreSQL's approximate equality.
fn approx_eq(a: &Shape, b: &Shape) -> bool {
    match (a, b) {
        (Shape::Point(p), Shape::Point(q)) => p.close(*q),
        (
            Shape::Circle {
                center: ca,
                radius: ra,
            },
            Shape::Circle {
                center: cb,
                radius: rb,
            },
        ) => ca.close(*cb) && (ra - rb).abs() <= 1e-6 * ra.abs().max(rb.abs()).max(1.0),
        (Shape::Box { high: ha, low: la }, Shape::Box { high: hb, low: lb }) => {
            (ha.x - hb.x).abs() <= eps(*ha, *hb)
                && (ha.y - hb.y).abs() <= eps(*ha, *hb)
                && (la.x - lb.x).abs() <= eps(*la, *lb)
                && (la.y - lb.y).abs() <= eps(*la, *lb)
        }
        (Shape::Polygon(pa), Shape::Polygon(pb)) => {
            pa.len() == pb.len()
                && pa.iter().zip(pb).all(|(p, q)| {
                    (p.x - q.x).abs() <= eps(*p, *q) && (p.y - q.y).abs() <= eps(*p, *q)
                })
        }
        _ => false,
    }
}

fn eps(a: Pt, b: Pt) -> f64 {
    1e-6 * a
        .x
        .abs()
        .max(b.x.abs())
        .max(a.y.abs())
        .max(b.y.abs())
        .max(1.0)
}

/// A point's distance to a segment.
fn point_lseg_distance(p: Pt, a: Pt, b: Pt) -> f64 {
    let (dx, dy) = (b.x - a.x, b.y - a.y);
    let len2 = dx * dx + dy * dy;
    if len2 == 0.0 {
        return p.distance(a);
    }
    let t = (((p.x - a.x) * dx + (p.y - a.y) * dy) / len2).clamp(0.0, 1.0);
    p.distance(Pt {
        x: a.x + t * dx,
        y: a.y + t * dy,
    })
}

/// The point on a segment closest to `p`.
fn closest_on_lseg(p: Pt, a: Pt, b: Pt) -> Pt {
    let (dx, dy) = (b.x - a.x, b.y - a.y);
    let len2 = dx * dx + dy * dy;
    if len2 == 0.0 {
        return a;
    }
    let t = (((p.x - a.x) * dx + (p.y - a.y) * dy) / len2).clamp(0.0, 1.0);
    Pt {
        x: a.x + t * dx,
        y: a.y + t * dy,
    }
}

fn point_in_box(p: Pt, high: Pt, low: Pt) -> bool {
    p.x >= low.x && p.x <= high.x && p.y >= low.y && p.y <= high.y
}

/// Whether a point is inside a polygon, or on its boundary (PostgreSQL's
/// `poly_contain_pt` includes the outline).
fn point_in_polygon(p: Pt, points: &[Pt]) -> bool {
    for (a, b) in segments(points, true) {
        if point_lseg_distance(p, a, b) <= 1e-9 {
            return true;
        }
    }
    let mut inside = false;
    let n = points.len();
    for i in 0..n {
        let a = points[i];
        let b = points[(i + n - 1) % n];
        if (a.y > p.y) != (b.y > p.y) {
            let x = (b.x - a.x) * (p.y - a.y) / (b.y - a.y) + a.x;
            if p.x < x {
                inside = !inside;
            }
        }
    }
    inside
}

/// A path or polygon's segments.
fn segments(points: &[Pt], closed: bool) -> Vec<(Pt, Pt)> {
    let mut out: Vec<(Pt, Pt)> = points.windows(2).map(|w| (w[0], w[1])).collect();
    if closed
        && let (Some(first), Some(last)) = (points.first(), points.last())
        && points.len() > 2
    {
        out.push((*last, *first));
    }
    out
}

/// The distance between two shapes' points, as PostgreSQL measures them.
pub(crate) fn distance(a: &Shape, b: &Shape) -> f64 {
    let a_points = points_of(a);
    let b_points = points_of(b);
    // Circle distance takes the radii into account, and a point inside a
    // polygon or path is at distance zero.
    match (a, b) {
        (
            Shape::Circle {
                center: ca,
                radius: ra,
            },
            Shape::Circle {
                center: cb,
                radius: rb,
            },
        ) => return (ca.distance(*cb) - ra - rb).max(0.0),
        (
            Shape::Circle {
                center: c,
                radius: r,
            },
            Shape::Point(p),
        )
        | (
            Shape::Point(p),
            Shape::Circle {
                center: c,
                radius: r,
            },
        ) => {
            let gap = p.distance(*c) - r;
            return if gap > 0.0 { gap } else { 0.0 };
        }
        (Shape::Point(p), Shape::Polygon(poly)) | (Shape::Polygon(poly), Shape::Point(p))
            if point_in_polygon(*p, poly) =>
        {
            return 0.0;
        }
        (Shape::Point(p), Shape::Box { high, low })
        | (Shape::Box { high, low }, Shape::Point(p))
            if point_in_box(*p, *high, *low) =>
        {
            return 0.0;
        }
        _ => {}
    }
    let mut best = f64::INFINITY;
    // Byte paths measure the same way; a single point pair is the plain
    // distance.
    let a_segments = segments(
        &a_points,
        matches!(a, Shape::Path { closed: true, .. } | Shape::Polygon(_)),
    );
    let b_segments = segments(
        &b_points,
        matches!(b, Shape::Path { closed: true, .. } | Shape::Polygon(_)),
    );
    if a_segments.is_empty() && b_segments.is_empty() {
        for p in &a_points {
            for q in &b_points {
                best = best.min(p.distance(*q));
            }
        }
        return best;
    }
    for (p1, p2) in &a_segments {
        for q in &b_points {
            best = best.min(point_lseg_distance(*q, *p1, *p2));
        }
    }
    for (q1, q2) in &b_segments {
        for p in &a_points {
            best = best.min(point_lseg_distance(*p, *q1, *q2));
        }
    }
    for (p1, p2) in &a_segments {
        for (q1, q2) in &b_segments {
            // Segments that cross are at distance zero.
            if segments_intersect(*p1, *p2, *q1, *q2) {
                return 0.0;
            }
            best = best.min(point_lseg_distance(*p1, *q1, *q2));
            best = best.min(point_lseg_distance(*p2, *q1, *q2));
            best = best.min(point_lseg_distance(*q1, *p1, *p2));
            best = best.min(point_lseg_distance(*q2, *p1, *p2));
        }
    }
    best
}

/// Whether two segments cross (strictly or at an endpoint).
fn segments_intersect(p1: Pt, p2: Pt, q1: Pt, q2: Pt) -> bool {
    let cross = |o: Pt, a: Pt, b: Pt| (a.x - o.x) * (b.y - o.y) - (a.y - o.y) * (b.x - o.x);
    let (d1, d2) = (cross(q1, q2, p1), cross(q1, q2, p2));
    let (d3, d4) = (cross(p1, p2, q1), cross(p1, p2, q2));
    if ((d1 > 0.0 && d2 < 0.0) || (d1 < 0.0 && d2 > 0.0))
        && ((d3 > 0.0 && d4 < 0.0) || (d3 < 0.0 && d4 > 0.0))
    {
        return true;
    }
    let on = |a: Pt, b: Pt, p: Pt| {
        p.x >= a.x.min(b.x) && p.x <= a.x.max(b.x) && p.y >= a.y.min(b.y) && p.y <= a.y.max(b.y)
    };
    (d1 == 0.0 && on(q1, q2, p1))
        || (d2 == 0.0 && on(q1, q2, p2))
        || (d3 == 0.0 && on(p1, p2, q1))
        || (d4 == 0.0 && on(p1, p2, q2))
}

/// The intersection of two segments, if they have exactly one.
fn lseg_intersection(p1: Pt, p2: Pt, q1: Pt, q2: Pt) -> Option<Pt> {
    let r = (p2.x - p1.x, p2.y - p1.y);
    let s = (q2.x - q1.x, q2.y - q1.y);
    let denom = r.0 * s.1 - r.1 * s.0;
    if denom == 0.0 {
        return None;
    }
    let t = ((q1.x - p1.x) * s.1 - (q1.y - p1.y) * s.0) / denom;
    let x = p1.x + t * r.0;
    let y = p1.y + t * r.1;
    Some(Pt { x, y })
}

/// `@>` containment.
fn contains(a: &Shape, b: &Shape) -> bool {
    match (a, b) {
        (Shape::Box { high: ha, low: la }, Shape::Point(p)) => point_in_box(*p, *ha, *la),
        (Shape::Box { high: ha, low: la }, Shape::Box { high: hb, low: lb }) => {
            ha.x >= hb.x && ha.y >= hb.y && la.x <= lb.x && la.y <= lb.y
        }
        (Shape::Box { high: ha, low: la }, Shape::Lseg(p, q)) => {
            point_in_box(*p, *ha, *la) && point_in_box(*q, *ha, *la)
        }
        (
            Shape::Circle {
                center: ca,
                radius: ra,
            },
            Shape::Point(p),
        ) => ca.distance(*p) <= *ra,
        (
            Shape::Circle {
                center: ca,
                radius: ra,
            },
            Shape::Circle {
                center: cb,
                radius: rb,
            },
        ) => ca.distance(*cb) + rb <= *ra,
        (Shape::Polygon(poly), Shape::Point(p)) => point_in_polygon(*p, poly),
        (Shape::Polygon(poly), Shape::Polygon(other)) => {
            other.iter().all(|p| point_in_polygon(*p, poly))
        }
        (Shape::Path { points, .. }, Shape::Point(p)) => point_in_polygon(*p, points),
        (
            Shape::Line {
                a: la,
                b: lb,
                c: lc,
            },
            Shape::Point(p),
        ) => (la * p.x + lb * p.y + lc).abs() < 1e-9,
        (
            Shape::Line {
                a: la,
                b: lb,
                c: lc,
            },
            Shape::Lseg(p, q),
        ) => (la * p.x + lb * p.y + lc).abs() < 1e-9 && (la * q.x + lb * q.y + lc).abs() < 1e-9,
        _ => false,
    }
}

/// Whether the first shape is inside the second.
fn contained_by(a: &Shape, b: &Shape) -> bool {
    contains(b, a)
}

/// `&&` overlap for the shapes that have it.
fn overlaps(a: &Shape, b: &Shape) -> bool {
    match (a, b) {
        (Shape::Box { high: ha, low: la }, Shape::Box { high: hb, low: lb }) => {
            !(la.x > hb.x || lb.x > ha.x || la.y > hb.y || lb.y > ha.y)
        }
        (
            Shape::Circle {
                center: ca,
                radius: ra,
            },
            Shape::Circle {
                center: cb,
                radius: rb,
            },
        ) => ca.distance(*cb) <= ra + rb,
        (Shape::Polygon(pa), Shape::Polygon(pb)) => {
            // Their bounding boxes must overlap, and one must peek into the
            // other; PostgreSQL first tests the bounding boxes.
            let (Some(box_a), Some(box_b)) = (bound_box(a), bound_box(b)) else {
                return false;
            };
            if !overlaps(&box_a, &box_b) {
                return false;
            }
            let segments_a = segments(pa, true);
            let segments_b = segments(pb, true);
            if segments_a.iter().any(|(p1, p2)| {
                segments_b
                    .iter()
                    .any(|(q1, q2)| segments_intersect(*p1, *p2, *q1, *q2))
            }) {
                return true;
            }
            pa.first().is_some_and(|p| point_in_polygon(*p, pb))
                || pb.first().is_some_and(|p| point_in_polygon(*p, pa))
        }
        _ => false,
    }
}

/// `<<`, `>>`, `<<|`, `|>>`, `&<`, `&>`, `&<|`, `|&>`: the positional
/// operators, which compare bounding boxes.
fn position(op: &str, a: &Shape, b: &Shape) -> Option<bool> {
    let box_a = bound_box(a)?;
    let box_b = bound_box(b)?;
    let (Shape::Box { high: ha, low: la }, Shape::Box { high: hb, low: lb }) = (&box_a, &box_b)
    else {
        return None;
    };
    Some(match op {
        "<<" => ha.x < lb.x,
        ">>" => la.x > hb.x,
        "&<" => ha.x <= hb.x,
        "&>" => la.x >= lb.x,
        "<<|" => ha.y < lb.y,
        "|>>" => la.y > hb.y,
        "&<|" => ha.y <= hb.y,
        "|&>" => la.y >= lb.y,
        "<^" => ha.y < lb.y,
        ">^" => la.y > hb.y,
        _ => return None,
    })
}

/// `?-` and `?|`: is a shape horizontal or vertical?
fn horizontal_or_vertical(op: &str, a: &Shape, b: &Shape) -> bool {
    let horizontal = |p: Pt, q: Pt| p.y == q.y;
    let vertical = |p: Pt, q: Pt| p.x == q.x;
    let test = |p: Pt, q: Pt| {
        if op == "?-" {
            horizontal(p, q)
        } else {
            vertical(p, q)
        }
    };
    match (a, b) {
        (Shape::Point(p), Shape::Point(q)) => test(*p, *q),
        (Shape::Lseg(p, q), Shape::Lseg(r, s)) => test(*p, *q) && test(*r, *s),
        (Shape::Line { a: la, b: lb, .. }, Shape::Line { a: ra, b: rb, .. }) => {
            if op == "?-" {
                la == &0.0 && ra == &0.0
            } else {
                lb == &0.0 && rb == &0.0
            }
        }
        _ => false,
    }
}

/// `?-|` and `?||`: are the shapes perpendicular or parallel?
fn perpendicular_or_parallel(op: &str, a: &Shape, b: &Shape) -> bool {
    let direction = |s: &Shape| match s {
        Shape::Lseg(p, q) => Some((q.x - p.x, q.y - p.y)),
        Shape::Line { a, b, .. } => Some((-b, *a)),
        _ => None,
    };
    let (Some((ax, ay)), Some((bx, by))) = (direction(a), direction(b)) else {
        return false;
    };
    if op == "?||" {
        ax * by - ay * bx == 0.0
    } else {
        ax * bx + ay * by == 0.0
    }
}

/// `?#`: do the shapes intersect?
fn intersects(a: &Shape, b: &Shape) -> bool {
    let a_segments: Vec<(Pt, Pt)> = match a {
        Shape::Lseg(p, q) => vec![(*p, *q)],
        Shape::Box { high, low } => vec![
            (
                *high,
                Pt {
                    x: low.x,
                    y: high.y,
                },
            ),
            (
                Pt {
                    x: low.x,
                    y: high.y,
                },
                *low,
            ),
            (
                *low,
                Pt {
                    x: high.x,
                    y: low.y,
                },
            ),
            (
                Pt {
                    x: high.x,
                    y: low.y,
                },
                *high,
            ),
        ],
        Shape::Path { points, closed } => segments(points, *closed),
        Shape::Line { .. } => Vec::new(),
        _ => Vec::new(),
    };
    let b_segments: Vec<(Pt, Pt)> = match b {
        Shape::Lseg(p, q) => vec![(*p, *q)],
        Shape::Box { high, low } => vec![
            (
                *high,
                Pt {
                    x: low.x,
                    y: high.y,
                },
            ),
            (
                Pt {
                    x: low.x,
                    y: high.y,
                },
                *low,
            ),
            (
                *low,
                Pt {
                    x: high.x,
                    y: low.y,
                },
            ),
            (
                Pt {
                    x: high.x,
                    y: low.y,
                },
                *high,
            ),
        ],
        Shape::Path { points, closed } => segments(points, *closed),
        Shape::Line { .. } => Vec::new(),
        _ => Vec::new(),
    };
    // A line crosses a segment when its endpoints fall on either side (or on
    // it).
    let line_crosses = |line: &Shape, p: Pt, q: Pt| {
        if let Shape::Line { a, b, c } = line {
            let f = |pt: Pt| a * pt.x + b * pt.y + c;
            let (fp, fq) = (f(p), f(q));
            fp == 0.0 || fq == 0.0 || (fp < 0.0) != (fq < 0.0)
        } else {
            false
        }
    };
    for (p1, p2) in &a_segments {
        if matches!(b, Shape::Line { .. }) && line_crosses(b, *p1, *p2) {
            return true;
        }
        for (q1, q2) in &b_segments {
            if segments_intersect(*p1, *p2, *q1, *q2) {
                return true;
            }
        }
    }
    if matches!(a, Shape::Line { .. }) {
        for (q1, q2) in &b_segments {
            if line_crosses(a, *q1, *q2) {
                return true;
            }
        }
    }
    // Two lines intersect unless they are parallel, and a line meets a box
    // the same way.
    match (a, b) {
        (Shape::Line { a: a1, b: b1, .. }, Shape::Line { a: a2, b: b2, .. }) => {
            a1 * b2 - b1 * a2 != 0.0
        }
        (Shape::Line { .. }, Shape::Box { high, low })
        | (Shape::Box { high, low }, Shape::Line { .. }) => {
            let corners = [
                *high,
                Pt {
                    x: low.x,
                    y: high.y,
                },
                *low,
                Pt {
                    x: high.x,
                    y: low.y,
                },
            ];
            let line = if matches!(a, Shape::Line { .. }) {
                a
            } else {
                b
            };
            (0..4).any(|i| line_crosses(line, corners[i], corners[(i + 1) % 4]))
        }
        _ => false,
    }
}

/// `#`: the intersection of two lsegs or lines, and the point count of a
/// path or polygon.
fn intersection_point(a: &Shape, b: &Shape) -> Option<Pt> {
    let as_segment = |s: &Shape| match s {
        Shape::Lseg(p, q) => Some((*p, *q)),
        _ => None,
    };
    match (as_segment(a), as_segment(b)) {
        (Some((p1, p2)), Some((q1, q2))) => lseg_intersection(p1, p2, q1, q2),
        _ => {
            // A line against a segment or another line: solve for the common
            // point.
            let line_of = |s: &Shape| match s {
                Shape::Line { a, b, c } => Some((*a, *b, *c)),
                Shape::Lseg(p, q) => {
                    let (a, b, c) = line_through(*p, *q);
                    Some((a, b, c))
                }
                _ => None,
            };
            let (Some((a1, b1, c1)), Some((a2, b2, c2))) = (line_of(a), line_of(b)) else {
                return None;
            };
            let denom = a1 * b2 - a2 * b1;
            if denom == 0.0 {
                return None;
            }
            Some(Pt {
                x: (b1 * c2 - b2 * c1) / denom,
                y: (c1 * a2 - c2 * a1) / denom,
            })
        }
    }
}

/// The coefficients of the line through two points, as PostgreSQL's
/// `line_construct` builds them: from the slope.
fn line_through(p: Pt, q: Pt) -> (f64, f64, f64) {
    let m = (q.y - p.y) / (q.x - p.x);
    let c = p.y - m * p.x;
    if m.is_infinite() {
        // Vertical: `-1 x + 0 y + x0 = 0`.
        (-1.0, 0.0, p.x)
    } else if m == 0.0 {
        // Horizontal: `0 x - 1 y + y0 = 0`.
        (0.0, -1.0, p.y)
    } else {
        (m, -1.0, if c == 0.0 { 0.0 } else { c })
    }
}

/// `##`: the point of `b` closest to `a`.
fn closest_point(a: &Shape, b: &Shape) -> Option<Pt> {
    match (a, b) {
        (Shape::Point(p), Shape::Point(q)) => Some(*q),
        (Shape::Point(p), Shape::Lseg(q1, q2)) => Some(closest_on_lseg(*p, *q1, *q2)),
        (Shape::Point(p), Shape::Box { high, low }) => Some(Pt {
            x: p.x.clamp(low.x, high.x),
            y: p.y.clamp(low.y, high.y),
        }),
        (Shape::Point(p), Shape::Line { a, b, c }) => {
            let denom = a * a + b * b;
            let t = (a * p.x + b * p.y + c) / denom;
            Some(Pt {
                x: p.x - a * t,
                y: p.y - b * t,
            })
        }
        (Shape::Lseg(p1, p2), Shape::Box { high, low }) => {
            let corner = |p: Pt| Pt {
                x: p.x.clamp(low.x, high.x),
                y: p.y.clamp(low.y, high.y),
            };
            Some(if point_in_box(*p1, *high, *low) {
                *p1
            } else if point_in_box(*p2, *high, *low) {
                *p2
            } else {
                let c1 = closest_on_lseg(*p1, *p1, *p2);
                let _ = c1;
                // The closest point of the box to the segment: the corner
                // nearest to either end.
                let candidates = [
                    corner(*p1),
                    corner(*p2),
                    Pt {
                        x: low.x,
                        y: p1.y.clamp(low.y, high.y),
                    },
                    Pt {
                        x: high.x,
                        y: p1.y.clamp(low.y, high.y),
                    },
                    Pt {
                        x: p1.x.clamp(low.x, high.x),
                        y: low.y,
                    },
                    Pt {
                        x: p1.x.clamp(low.x, high.x),
                        y: high.y,
                    },
                ];
                *candidates
                    .iter()
                    .min_by(|x, y| {
                        let dx = distance(&Shape::Point(**x), &Shape::Lseg(*p1, *p2));
                        let dy = distance(&Shape::Point(**y), &Shape::Lseg(*p1, *p2));
                        cmp_f64(dx, dy)
                    })
                    .expect("candidates are non-empty")
            })
        }
        _ => None,
    }
}

/// `#`(box, box): the boxes' intersection.
fn box_intersection(a: &Shape, b: &Shape) -> Option<Shape> {
    let (Shape::Box { high: ha, low: la }, Shape::Box { high: hb, low: lb }) = (a, b) else {
        return None;
    };
    let high_pt = Pt {
        x: ha.x.min(hb.x),
        y: ha.y.min(hb.y),
    };
    let low_pt = Pt {
        x: la.x.max(lb.x),
        y: la.y.max(lb.y),
    };
    Some(boxed(high_pt, low_pt))
}

/// The area of a box, circle, or path.
pub(crate) fn area(shape: &Shape) -> f64 {
    match shape {
        Shape::Box { high, low } => (high.x - low.x) * (high.y - low.y),
        Shape::Circle { radius, .. } => std::f64::consts::PI * radius * radius,
        Shape::Polygon(points) => polygon_area(points),
        Shape::Path { points, .. } => polygon_area(points),
        _ => 0.0,
    }
}

/// The shoelace area of a polygon.
fn polygon_area(points: &[Pt]) -> f64 {
    if points.len() < 3 {
        return 0.0;
    }
    let mut total = 0.0;
    for i in 0..points.len() {
        let a = points[i];
        let b = points[(i + 1) % points.len()];
        total += a.x * b.y - b.x * a.y;
    }
    (total / 2.0).abs()
}

/// A geometric operator's result. `right_kind` is the right operand's
/// declared geometric type, when it has one.
pub(crate) fn operator(
    op: &str,
    kind: Kind,
    left: &str,
    right: &str,
    right_kind: Option<Kind>,
) -> Result<Value, String> {
    let a = parse(kind, left)?;
    let b = match right_kind {
        Some(right_kind) => Some(parse(right_kind, right)?),
        None => None,
    };
    let as_point = |shape: &Shape| match shape {
        Shape::Point(p) => Some(*p),
        _ => None,
    };
    match op {
        "+" | "-" | "*" | "/" => {
            // Point arithmetic, and the same operation applied to every point
            // of a shape.
            let other = match b.as_ref().and_then(as_point) {
                Some(p) => p,
                None => return Err(format!("unsupported geometric operator {op}")),
            };
            let map_point = |p: Pt| match op {
                "+" => p.add(other),
                "-" => p.sub(other),
                "*" => p.mul(other),
                _ => p.div(other),
            };
            let shape = match &a {
                Shape::Point(p) => Shape::Point(map_point(*p)),
                Shape::Box { high, low } => Shape::Box {
                    high: map_point(*high),
                    low: map_point(*low),
                },
                Shape::Circle { center, radius } => Shape::Circle {
                    center: map_point(*center),
                    radius: match op {
                        "*" => radius * (other.x * other.x + other.y * other.y).sqrt(),
                        "/" => radius / (other.x * other.x + other.y * other.y).sqrt(),
                        _ => *radius,
                    },
                },
                Shape::Path { closed, points } => Shape::Path {
                    closed: *closed,
                    points: points.iter().map(|p| map_point(*p)).collect(),
                },
                _ => return Err(format!("unsupported geometric operator {op}")),
            };
            Ok(Value::Text(format(&shape)))
        }
        "<->" => {
            let Some(b) = b.as_ref() else {
                return Err("unsupported geometric operator <->".to_string());
            };
            Ok(Value::Float(distance(&a, b)))
        }
        "~=" => {
            let Some(b) = b.as_ref() else {
                return Err("unsupported geometric operator ~=".to_string());
            };
            Ok(Value::Bool(approx_eq(&a, &b)))
        }
        "@>" | "<@" => {
            let Some(b) = b.as_ref() else {
                return Err(format!("unsupported geometric operator {op}"));
            };
            let holds = if op == "@>" {
                contains(&a, b)
            } else {
                contained_by(&a, b)
            };
            Ok(Value::Bool(holds))
        }
        "&&" => {
            let Some(b) = b.as_ref() else {
                return Err("unsupported geometric operator &&".to_string());
            };
            Ok(Value::Bool(overlaps(&a, &b)))
        }
        "<<" | ">>" | "&<" | "&>" | "<<|" | "|>>" | "&<|" | "|&>" | "<^" | ">^" => {
            let Some(b) = b.as_ref() else {
                return Err(format!("unsupported geometric operator {op}"));
            };
            match position(op, &a, b) {
                Some(holds) => Ok(Value::Bool(holds)),
                None => Err(format!("unsupported geometric operator {op}")),
            }
        }
        "?#" => {
            let Some(b) = b.as_ref() else {
                return Err("unsupported geometric operator ?#".to_string());
            };
            Ok(Value::Bool(intersects(&a, b)))
        }
        "?-|" | "?||" => {
            let Some(b) = b.as_ref() else {
                return Err(format!("unsupported geometric operator {op}"));
            };
            Ok(Value::Bool(perpendicular_or_parallel(op, &a, b)))
        }
        "?-" | "?|" => {
            let Some(b) = b.as_ref() else {
                return Err(format!("unsupported geometric operator {op}"));
            };
            Ok(Value::Bool(horizontal_or_vertical(op, &a, b)))
        }
        "@@" => match center(&a) {
            Some(p) => Ok(Value::Text(point_text(p))),
            None => Err("unsupported geometric operator @@".to_string()),
        },
        "@-@" => Ok(Value::Float(length(&a))),
        "#" => {
            if let Some(b) = b.as_ref()
                && matches!(a, Shape::Box { .. })
                && matches!(b, Shape::Box { .. })
            {
                return Ok(box_intersection(&a, b)
                    .map(|shape| Value::Text(format(&shape)))
                    .unwrap_or(Value::Null));
            }
            if let Some(b) = b.as_ref() {
                return Ok(intersection_point(&a, b)
                    .map(|p| Value::Text(point_text(p)))
                    .unwrap_or(Value::Null));
            }
            match &a {
                Shape::Path { points, .. } | Shape::Polygon(points) => {
                    Ok(Value::Int(points.len() as i64))
                }
                _ => Err("unsupported geometric operator #".to_string()),
            }
        }
        "##" => {
            let Some(b) = b.as_ref() else {
                return Err("unsupported geometric operator ##".to_string());
            };
            Ok(closest_point(&a, b)
                .map(|p| Value::Text(point_text(p)))
                .unwrap_or(Value::Null))
        }
        "=" | "<>" | "<" | ">" | "<=" | ">=" => {
            let Some(b) = b.as_ref() else {
                return Err(format!("unsupported geometric operator {op}"));
            };
            if op == "=" || op == "<>" {
                let holds = eq(kind, &a, b);
                return Ok(Value::Bool(if op == "=" { holds } else { !holds }));
            }
            let ord = cmp(kind, &a, b);
            use std::cmp::Ordering::*;
            Ok(Value::Bool(match op {
                "<" => ord == Less,
                ">" => ord == Greater,
                "<=" => ord != Greater,
                _ => ord != Less,
            }))
        }
        other => Err(format!("unsupported geometric operator {other}")),
    }
}

/// A value of any geometric type, by the shape of its text.
pub(crate) fn parse_any(text: &str) -> Option<Shape> {
    let trimmed = text.trim();
    let candidates: [Kind; 7] = [
        Kind::Circle,
        Kind::Line,
        Kind::Lseg,
        Kind::Point,
        Kind::Box,
        Kind::Polygon,
        Kind::Path,
    ];
    for kind in candidates {
        if let Ok(shape) = parse(kind, trimmed) {
            // A box's canonical text has no outer parentheses; trying it
            // before the polygon keeps `(1,2),(3,4)` a box.
            return Some(shape);
        }
    }
    None
}

/// A unary geometric operator's result.
pub(crate) fn unary(op: &str, kind: Kind, text: &str) -> Result<Value, String> {
    let shape = parse(kind, text)?;
    match op {
        "@@" => match center(&shape) {
            Some(p) => Ok(Value::Text(point_text(p))),
            None => Err(format!("operator does not exist: @@ {}", kind.name())),
        },
        "@-@" => Ok(Value::Float(length(&shape))),
        "#" => match &shape {
            Shape::Path { points, .. } | Shape::Polygon(points) => {
                Ok(Value::Int(points.len() as i64))
            }
            _ => Err(format!("operator does not exist: # {}", kind.name())),
        },
        "?-" | "?|" => Ok(Value::Bool(match &shape {
            Shape::Lseg(p, q) => {
                if op == "?-" {
                    p.y == q.y
                } else {
                    p.x == q.x
                }
            }
            Shape::Line { a, b, .. } => {
                if op == "?-" {
                    *a == 0.0
                } else {
                    *b == 0.0
                }
            }
            _ => false,
        })),
        other => Err(format!("operator does not exist: {other} {}", kind.name())),
    }
}

/// A geometric function or constructor.
pub(crate) fn call(name: &str, kind: Kind, args: &[Value]) -> Option<Result<Value, String>> {
    let number = |i: usize| -> Option<f64> {
        match args.get(i) {
            Some(Value::Int(v)) => Some(*v as f64),
            Some(Value::Float(v)) => Some(*v),
            Some(Value::Numeric(v)) => Some(crate::value::decimal_to_f64(v)),
            _ => None,
        }
    };
    let shape = |i: usize, kind: Kind| -> Option<Shape> {
        let text = crate::render(args.get(i)?);
        if i == 0 {
            // The first argument reads as its declared type first.
            if let Ok(shape) = parse(kind, &text) {
                return Some(shape);
            }
        }
        parse_any(&text)
    };
    let point = |i: usize| -> Option<Pt> {
        match shape(i, Kind::Point)? {
            Shape::Point(p) => Some(p),
            _ => None,
        }
    };
    let result = match name {
        "POINT" if args.len() == 2 => match (number(0), number(1)) {
            (Some(x), Some(y)) => Ok(Value::Text(point_text(Pt { x, y }))),
            _ => Err("unsupported".to_string()),
        },
        "POINT" if args.len() == 1 => match shape(0, kind)? {
            Shape::Box { high, low } => Ok(Value::Text(point_text(Pt {
                x: (high.x + low.x) / 2.0,
                y: (high.y + low.y) / 2.0,
            }))),
            Shape::Circle { center, .. } => Ok(Value::Text(point_text(center))),
            Shape::Lseg(p, q) => Ok(Value::Text(point_text(Pt {
                x: (p.x + q.x) / 2.0,
                y: (p.y + q.y) / 2.0,
            }))),
            Shape::Polygon(points) => match center(&Shape::Polygon(points)) {
                Some(p) => Ok(Value::Text(point_text(p))),
                None => Err("unsupported".to_string()),
            },
            _ => Err("unsupported".to_string()),
        },
        "LSEG" if args.len() == 2 => match (point(0), point(1)) {
            (Some(p), Some(q)) => Ok(Value::Text(format(&Shape::Lseg(p, q)))),
            _ => Err("unsupported".to_string()),
        },
        "LSEG" if args.len() == 1 => match shape(0, kind)? {
            Shape::Box { high, low } => Ok(Value::Text(format(&Shape::Lseg(high, low)))),
            _ => Err("unsupported".to_string()),
        },
        "BOX" if args.len() == 2 => match (point(0), point(1)) {
            (Some(p), Some(q)) => Ok(Value::Text(format(&boxed(p, q)))),
            _ => Err("unsupported".to_string()),
        },
        "BOX" | "BOUND_BOX" if args.len() == 1 || args.len() == 2 => {
            let first = shape(0, kind)?;
            let shape = if args.len() == 2 {
                let second = shape(1, kind)?;
                let points: Vec<Pt> = points_of(&first)
                    .into_iter()
                    .chain(points_of(&second))
                    .collect();
                Shape::Polygon(points)
            } else {
                first
            };
            match bound_box(&shape) {
                Some(shape) => Ok(Value::Text(format(&shape))),
                None => Err("unsupported".to_string()),
            }
        }
        "CIRCLE" if args.len() == 2 => match (point(0), number(1)) {
            (Some(center), Some(radius)) => {
                Ok(Value::Text(format(&Shape::Circle { center, radius })))
            }
            _ => Err("unsupported".to_string()),
        },
        "CIRCLE" if args.len() == 1 => match shape(0, kind)? {
            Shape::Box { high, low } => Ok(Value::Text(format(&Shape::Circle {
                center: Pt {
                    x: (high.x + low.x) / 2.0,
                    y: (high.y + low.y) / 2.0,
                },
                radius: high.distance(low) / 2.0,
            }))),
            Shape::Polygon(points) => {
                let sphere = match bound_box(&Shape::Polygon(points)) {
                    Some(Shape::Box { high, low }) => Shape::Circle {
                        center: Pt {
                            x: (high.x + low.x) / 2.0,
                            y: (high.y + low.y) / 2.0,
                        },
                        radius: high.distance(low) / 2.0,
                    },
                    _ => return Some(Err("unsupported".to_string())),
                };
                Ok(Value::Text(format(&sphere)))
            }
            _ => Err("unsupported".to_string()),
        },
        "PATH" if args.len() == 1 => match shape(0, kind)? {
            Shape::Polygon(points) => Ok(Value::Text(format(&Shape::Path {
                closed: true,
                points,
            }))),
            _ => Err("unsupported".to_string()),
        },
        "POLYGON" if (1..=2).contains(&args.len()) => {
            let shape = shape(0, kind)?;
            let sides = number(1).unwrap_or(12.0).max(1.0) as usize;
            match shape {
                Shape::Path { points, .. } => Ok(Value::Text(format(&Shape::Polygon(points)))),
                Shape::Box { high, low } => Ok(Value::Text(format(&Shape::Polygon(vec![
                    low,
                    Pt {
                        x: low.x,
                        y: high.y,
                    },
                    high,
                    Pt {
                        x: high.x,
                        y: low.y,
                    },
                ])))),
                Shape::Circle { center, radius } => {
                    let points = (0..sides)
                        .map(|i| {
                            let angle = 2.0 * std::f64::consts::PI * (i as f64) / (sides as f64);
                            Pt {
                                x: center.x - radius * angle.cos(),
                                y: center.y + radius * angle.sin(),
                            }
                        })
                        .collect();
                    Ok(Value::Text(format(&Shape::Polygon(points))))
                }
                _ => Err("unsupported".to_string()),
            }
        }
        "LINE" if args.len() == 2 => match (point(0), point(1)) {
            (Some(p), Some(q)) => {
                if p == q {
                    return Some(Err(crate::error_fields::DbError::new(
                        "invalid line specification: must be two distinct points",
                    )
                    .code("22P02")
                    .into_text()));
                }
                let (a, b, c) = line_through(p, q);
                Ok(Value::Text(format(&Shape::Line { a, b, c })))
            }
            _ => Err("unsupported".to_string()),
        },
        "CENTER" if args.len() == 1 => match center(&shape(0, kind)?) {
            Some(p) => Ok(Value::Text(point_text(p))),
            None => Err("unsupported".to_string()),
        },
        "RADIUS" if args.len() == 1 => match shape(0, kind)? {
            Shape::Circle { radius, .. } => Ok(Value::Float(radius)),
            _ => Err("unsupported".to_string()),
        },
        "DIAMETER" if args.len() == 1 => match shape(0, kind)? {
            Shape::Circle { radius, .. } => Ok(Value::Float(radius * 2.0)),
            _ => Err("unsupported".to_string()),
        },
        "HEIGHT" | "WIDTH" if args.len() == 1 => match shape(0, kind)? {
            Shape::Box { high, low } => Ok(Value::Float(if name == "HEIGHT" {
                high.y - low.y
            } else {
                high.x - low.x
            })),
            _ => Err("unsupported".to_string()),
        },
        "DIAGONAL" if args.len() == 1 => match shape(0, kind)? {
            Shape::Box { high, low } => Ok(Value::Text(format(&Shape::Lseg(high, low)))),
            _ => Err("unsupported".to_string()),
        },
        "AREA" if args.len() == 1 => Ok(Value::Float(area(&shape(0, kind)?))),
        "NPOINTS" if args.len() == 1 => match shape(0, kind)? {
            Shape::Path { points, .. } | Shape::Polygon(points) => {
                Ok(Value::Int(points.len() as i64))
            }
            _ => Err("unsupported".to_string()),
        },
        "ISCLOSED" | "ISOPEN" if args.len() == 1 => match shape(0, kind)? {
            Shape::Path { closed, .. } => Ok(Value::Bool(if name == "ISCLOSED" {
                closed
            } else {
                !closed
            })),
            _ => Err("unsupported".to_string()),
        },
        "PCLOSE" | "POPEN" if args.len() == 1 => match shape(0, kind)? {
            Shape::Path { closed, points } => Ok(Value::Text(format(&Shape::Path {
                closed: name == "PCLOSE",
                points,
            }))),
            _ => Err("unsupported".to_string()),
        },
        _ => return None,
    };
    Some(result)
}

/// A cast between two geometric types.
pub(crate) fn cast_between(from: Kind, to: Kind, text: &str) -> Result<Value, String> {
    let shape = parse(from, text)?;
    let converted = match (from, to) {
        (Kind::Point, Kind::Box) => {
            let Shape::Point(p) = shape else {
                return Err(bad_cast(from, to));
            };
            boxed(p, p)
        }
        (Kind::Path, Kind::Polygon) => {
            let Shape::Path { points, .. } = shape else {
                return Err(bad_cast(from, to));
            };
            Shape::Polygon(points)
        }
        (Kind::Box, Kind::Polygon) => {
            let Shape::Box { high, low } = shape else {
                return Err(bad_cast(from, to));
            };
            Shape::Polygon(vec![
                low,
                Pt {
                    x: low.x,
                    y: high.y,
                },
                high,
                Pt {
                    x: high.x,
                    y: low.y,
                },
            ])
        }
        (Kind::Lseg, Kind::Point) => {
            let Shape::Lseg(p, q) = shape else {
                return Err(bad_cast(from, to));
            };
            Shape::Point(Pt {
                x: (p.x + q.x) / 2.0,
                y: (p.y + q.y) / 2.0,
            })
        }
        (Kind::Box, Kind::Point) => {
            let Shape::Box { high, low } = shape else {
                return Err(bad_cast(from, to));
            };
            Shape::Point(Pt {
                x: (high.x + low.x) / 2.0,
                y: (high.y + low.y) / 2.0,
            })
        }
        (Kind::Box, Kind::Lseg) => {
            let Shape::Box { high, low } = shape else {
                return Err(bad_cast(from, to));
            };
            Shape::Lseg(high, low)
        }
        (Kind::Box, Kind::Circle) => {
            let Shape::Box { high, low } = shape else {
                return Err(bad_cast(from, to));
            };
            Shape::Circle {
                center: Pt {
                    x: (high.x + low.x) / 2.0,
                    y: (high.y + low.y) / 2.0,
                },
                radius: high.distance(low) / 2.0,
            }
        }
        (Kind::Polygon, Kind::Point) => match center(&shape) {
            Some(p) => Shape::Point(p),
            None => return Err(bad_cast(from, to)),
        },
        (Kind::Polygon, Kind::Box) => match bound_box(&shape) {
            Some(b) => b,
            None => return Err(bad_cast(from, to)),
        },
        (Kind::Polygon, Kind::Circle) | (Kind::Polygon, Kind::Path) => {
            if to == Kind::Path {
                let Shape::Polygon(points) = shape else {
                    return Err(bad_cast(from, to));
                };
                Shape::Path {
                    closed: true,
                    points,
                }
            } else {
                match bound_box(&shape) {
                    Some(Shape::Box { high, low }) => Shape::Circle {
                        center: Pt {
                            x: (high.x + low.x) / 2.0,
                            y: (high.y + low.y) / 2.0,
                        },
                        radius: high.distance(low) / 2.0,
                    },
                    _ => return Err(bad_cast(from, to)),
                }
            }
        }
        (Kind::Circle, Kind::Point) => {
            let Shape::Circle { center, .. } = shape else {
                return Err(bad_cast(from, to));
            };
            Shape::Point(center)
        }
        (Kind::Circle, Kind::Box) => {
            let Shape::Circle { center, radius } = shape else {
                return Err(bad_cast(from, to));
            };
            // The box's corners lie on the circle.
            let half = radius / std::f64::consts::SQRT_2;
            boxed(
                Pt {
                    x: center.x + half,
                    y: center.y + half,
                },
                Pt {
                    x: center.x - half,
                    y: center.y - half,
                },
            )
        }
        (Kind::Circle, Kind::Polygon) => {
            let Shape::Circle { center, radius } = shape else {
                return Err(bad_cast(from, to));
            };
            let points = (0..12)
                .map(|i| {
                    let angle = 2.0 * std::f64::consts::PI * (i as f64) / 12.0;
                    Pt {
                        x: center.x - radius * angle.cos(),
                        y: center.y + radius * angle.sin(),
                    }
                })
                .collect();
            Shape::Polygon(points)
        }
        _ => return Err(bad_cast(from, to)),
    };
    Ok(Value::Text(format(&converted)))
}

fn bad_cast(from: Kind, to: Kind) -> String {
    crate::error_fields::DbError::new(format!("cannot cast type {} to {}", from.name(), to.name()))
        .code("42846")
        .into_text()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lit(kind: Kind, text: &str) -> String {
        from_literal(kind, text).unwrap()
    }

    #[test]
    fn literals_canonicalize_as_postgresql_does() {
        assert_eq!(lit(Kind::Point, "(1,2)"), "(1,2)");
        assert_eq!(lit(Kind::Point, "(1.5, -2.25)"), "(1.5,-2.25)");
        assert_eq!(lit(Kind::Box, "((1,2),(3,4))"), "(3,4),(1,2)");
        assert_eq!(lit(Kind::Box, "(3,4),(1,2)"), "(3,4),(1,2)");
        assert_eq!(lit(Kind::Lseg, "[(1,2),(3,4)]"), "[(1,2),(3,4)]");
        assert_eq!(lit(Kind::Lseg, "((1,2),(3,4))"), "[(1,2),(3,4)]");
        assert_eq!(lit(Kind::Path, "[(1,2),(3,4)]"), "[(1,2),(3,4)]");
        assert_eq!(lit(Kind::Path, "((1,2),(3,4))"), "((1,2),(3,4))");
        assert_eq!(
            lit(Kind::Polygon, "((1,2),(3,4),(5,6))"),
            "((1,2),(3,4),(5,6))"
        );
        assert_eq!(lit(Kind::Circle, "<(1,2),3>"), "<(1,2),3>");
        // A line keeps the coefficients it is given.
        assert_eq!(lit(Kind::Line, "{1,-2,3}"), "{1,-2,3}");
        assert_eq!(lit(Kind::Line, "{-1,1,1}"), "{-1,1,1}");
        let error = from_literal(Kind::Line, "{0,0,5}").unwrap_err();
        assert!(
            crate::error_message(&error).contains("A and B cannot both be zero"),
            "{error}"
        );
        assert!(from_literal(Kind::Circle, "<(0,0),-1>").is_err());
        assert!(from_literal(Kind::Point, "x").is_err());
        assert!(from_literal(Kind::Point, "(1)").is_err());
        assert!(from_literal(Kind::Box, "(1,)").is_err());
    }

    #[test]
    fn operators_match_postgresql() {
        let with = |op: &str, kind: Kind, a: &str, b: &str, right: Kind| {
            operator(op, kind, a, b, Some(right)).unwrap()
        };
        let same = |op: &str, kind: Kind, a: &str, b: &str| with(op, kind, a, b, kind);
        assert_eq!(
            same("<->", Kind::Point, "(1,2)", "(4,6)"),
            Value::Float(5.0)
        );
        assert_eq!(
            same("+", Kind::Point, "(1,2)", "(3,4)"),
            Value::Text("(4,6)".into())
        );
        assert_eq!(
            same("*", Kind::Point, "(3,4)", "(2,3)"),
            Value::Text("(-6,17)".into())
        );
        assert_eq!(
            same("~=", Kind::Point, "(1,2)", "(1,2.0000001)"),
            Value::Bool(true)
        );
        assert_eq!(
            same("~=", Kind::Point, "(1,2)", "(1,2.000001)"),
            Value::Bool(false)
        );
        assert_eq!(
            with("@>", Kind::Box, "(3,4),(1,2)", "(2,3)", Kind::Point),
            Value::Bool(true)
        );
        assert_eq!(
            with("@>", Kind::Box, "(3,4),(1,2)", "(1,1)", Kind::Point),
            Value::Bool(false)
        );
        assert_eq!(
            same("@>", Kind::Box, "(3,4),(1,2)", "(3,3),(2,2)"),
            Value::Bool(true)
        );
        assert_eq!(
            same("&&", Kind::Circle, "<(0,0),2>", "<(3,0),2>"),
            Value::Bool(true)
        );
        assert_eq!(
            same("<<", Kind::Box, "(1,1),(0,0)", "(3,3),(2,2)"),
            Value::Bool(true)
        );
        assert_eq!(
            same("<<|", Kind::Box, "(1,1),(0,0)", "(1,3),(0,2)"),
            Value::Bool(true)
        );
        assert_eq!(
            same("&<", Kind::Box, "(1,1),(0,0)", "(2,2),(0,0)"),
            Value::Bool(true)
        );
        assert_eq!(
            same("?-", Kind::Point, "(1,2)", "(1,3)"),
            Value::Bool(false)
        );
        assert_eq!(
            same("?|", Kind::Point, "(1,2)", "(3,2)"),
            Value::Bool(false)
        );
        assert_eq!(
            same("?#", Kind::Lseg, "[(0,0),(3,4)]", "[(0,0),(3,0)]"),
            Value::Bool(true)
        );
        assert_eq!(
            same("#", Kind::Lseg, "[(0,0),(3,4)]", "[(0,4),(3,0)]"),
            Value::Text("(1.5,2)".into())
        );
        assert_eq!(
            with("##", Kind::Point, "(1,2)", "[(0,0),(2,2)]", Kind::Lseg),
            Value::Text("(1.5,1.5)".into())
        );
        assert_eq!(
            with("+", Kind::Box, "(3,4),(1,2)", "(1,1)", Kind::Point),
            Value::Text("(4,5),(2,3)".into())
        );
        assert_eq!(
            same("=", Kind::Box, "(2,2),(0,0)", "(3,3),(1,1)"),
            Value::Bool(true)
        );
        assert_eq!(
            same("<", Kind::Lseg, "((0,0),(3,4))", "((0,0),(4,4))"),
            Value::Bool(true)
        );
    }

    #[test]
    fn functions_and_casts_match_postgresql() {
        let call_it =
            |name: &str, kind: Kind, args: &[Value]| call(name, kind, args).unwrap().unwrap();
        assert_eq!(
            call_it("CENTER", Kind::Box, &[Value::Text("(3,4),(1,2)".into())]),
            Value::Text("(2,3)".into())
        );
        assert_eq!(
            call_it("RADIUS", Kind::Circle, &[Value::Text("<(1,2),3>".into())]),
            Value::Float(3.0)
        );
        assert_eq!(
            call_it("AREA", Kind::Box, &[Value::Text("(3,4),(1,2)".into())]),
            Value::Float(4.0)
        );
        assert_eq!(
            call_it(
                "NPOINTS",
                Kind::Path,
                &[Value::Text("[(0,0),(3,4)]".into())]
            ),
            Value::Int(2)
        );
        // The circle's polygon has twelve corners, the first at 180 degrees.
        let Value::Text(polygon) =
            call_it("POLYGON", Kind::Circle, &[Value::Text("<(0,0),1>".into())])
        else {
            panic!("a polygon is text");
        };
        let Shape::Polygon(corners) = parse(Kind::Polygon, &polygon).unwrap() else {
            panic!("a polygon parses");
        };
        assert_eq!(corners.len(), 12);
        assert_eq!(corners[0], Pt { x: -1.0, y: 0.0 });
        assert!((corners[3].y - 1.0).abs() < 1e-12);
        assert_eq!(
            call_it("LSEG", Kind::Box, &[Value::Text("(3,4),(1,2)".into())]),
            Value::Text("[(3,4),(1,2)]".into())
        );
        assert_eq!(
            cast_between(Kind::Box, Kind::Polygon, "(3,4),(1,2)").unwrap(),
            Value::Text("((1,2),(1,4),(3,4),(3,2))".into())
        );
        assert_eq!(
            cast_between(Kind::Circle, Kind::Point, "<(1,2),3>").unwrap(),
            Value::Text("(1,2)".into())
        );
        assert_eq!(
            cast_between(Kind::Point, Kind::Box, "(1,2)").unwrap(),
            Value::Text("(1,2),(1,2)".into())
        );
        let error = cast_between(Kind::Point, Kind::Lseg, "(1,2)").unwrap_err();
        assert!(crate::error_message(&error).contains("cannot cast type point to lseg"));
    }
}

/// Whether PostgreSQL has the operator for the two argument types.
pub(crate) fn supported(op: &str, left: Kind, right: Kind) -> bool {
    use Kind::*;
    let same = left == right;
    let boxy = || same && matches!(left, Box | Circle | Polygon | Point);
    match op {
        "+" => (left == Path && right == Path) || right == Point,
        "-" | "*" | "/" => right == Point,
        "~=" => same && matches!(left, Box | Circle | Point | Polygon),
        "=" => same && matches!(left, Box | Circle | Line | Lseg | Path),
        "<>" => same && matches!(left, Point | Box | Circle | Line | Lseg | Path),
        "<" | ">" | "<=" | ">=" => same && matches!(left, Box | Circle | Lseg | Path),
        "@>" => matches!(
            (left, right),
            (Box, Box | Point)
                | (Circle, Circle | Point)
                | (Path, Point)
                | (Polygon, Polygon | Point)
        ),
        "<@" => matches!(
            (left, right),
            (Box, Box | Point)
                | (Circle, Circle | Point)
                | (Lseg, Box | Line)
                | (Point, Box | Circle | Line | Lseg | Path | Polygon)
                | (Polygon, Polygon)
        ),
        "&&" => boxy() && !matches!(left, Point),
        "<<" | ">>" | "<<|" | "|>>" => {
            (same && matches!(left, Box | Circle | Polygon)) || (same && left == Point)
        }
        "&<" | "&>" | "&<|" | "|&>" => boxy() && !matches!(left, Point),
        "<^" | ">^" => same && matches!(left, Box | Point),
        "?#" => matches!(
            (left, right),
            (Box, Box) | (Line, Box | Line) | (Lseg, Box | Line | Lseg) | (Path, Path)
        ),
        "?-|" | "?||" => same && matches!(left, Line | Lseg),
        "?-" | "?|" => left == Point && right == Point,
        "##" => matches!(
            (left, right),
            (Line, Lseg) | (Lseg, Box | Lseg) | (Point, Box | Line | Lseg)
        ),
        "#" => same && matches!(left, Box | Line | Lseg),
        "<->" => matches!(
            (left, right),
            (Point, Point | Lseg | Box | Line | Circle | Polygon | Path)
                | (Lseg, Point | Lseg | Box | Line)
                | (Box, Point | Lseg | Box)
                | (Line, Point | Lseg | Line)
                | (Circle, Point | Circle | Polygon)
                | (Polygon, Point | Circle | Polygon)
                | (Path, Point | Path)
        ),
        _ => false,
    }
}

/// Whether the two kinds take the operator as a *unary* operator (the
/// geometric ones sqlparser parses that way).
pub(crate) fn unary_supported(kind: Kind, op: &str) -> bool {
    use Kind::*;
    match op {
        "@@" => matches!(kind, Box | Circle | Lseg | Polygon),
        "@-@" => matches!(kind, Lseg | Path),
        "#" => matches!(kind, Path | Polygon),
        "?-" | "?|" => matches!(kind, Line | Lseg),
        _ => false,
    }
}

/// Whether PostgreSQL has the function for the argument types (the
/// constructors and accessors, by their fixed signatures).
pub(crate) fn function_supported(name: &str, kind: Kind, arg_kinds: &[Option<Kind>]) -> bool {
    use Kind::*;
    let first = arg_kinds.first().copied().flatten();
    match name {
        "POINT" => arg_kinds.len() == 2 || matches!(first, Some(Box | Circle | Lseg | Polygon)),
        "LSEG" => arg_kinds.len() == 2 || first == Some(Box),
        "BOX" => arg_kinds.len() == 2 || matches!(first, Some(Point | Polygon | Circle)),
        "PATH" => first == Some(Polygon),
        "POLYGON" => {
            matches!(first, Some(Box | Path | Circle))
                || (arg_kinds.len() == 2
                    && arg_kinds.first().copied().flatten().is_none()
                    && arg_kinds.get(1).copied().flatten() == Some(Circle))
        }
        "LINE" => arg_kinds.len() == 2,
        "CIRCLE" => arg_kinds.len() == 2 || matches!(first, Some(Box | Polygon)),
        "CENTER" => matches!(kind, Box | Circle),
        "RADIUS" | "DIAMETER" => kind == Circle,
        "HEIGHT" | "WIDTH" | "DIAGONAL" => kind == Box,
        "AREA" => matches!(kind, Box | Circle | Path),
        "NPOINTS" => matches!(kind, Path | Polygon),
        "ISCLOSED" | "ISOPEN" | "PCLOSE" | "POPEN" => kind == Path,
        "BOUND_BOX" => arg_kinds.len() == 2 && first == Some(Box),
        _ => false,
    }
}

/// The declared type of a geometric function's result, for `pg_typeof` and
/// the wire.
pub(crate) fn return_type(name: &str, kind: &str) -> Option<String> {
    let kind = Kind::of(kind)?;
    Some(
        match name {
            "CENTER" | "POINT" => "point",
            "RADIUS" | "DIAMETER" | "HEIGHT" | "WIDTH" | "AREA" => "double precision",
            "DIAGONAL" => "lseg",
            "NPOINTS" => "integer",
            "ISCLOSED" | "ISOPEN" => "boolean",
            "PCLOSE" | "POPEN" => "path",
            "BOUND_BOX" => "box",
            // A constructor or conversion keeps its own type.
            "POINT" | "LSEG" | "BOX" | "PATH" | "POLYGON" | "LINE" | "CIRCLE" => kind.name(),
            _ => return None,
        }
        .to_string(),
    )
}
