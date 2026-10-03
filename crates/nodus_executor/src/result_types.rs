//! Result types determined from expressions and declared columns, independent
//! of result rows. Describe (LIMIT 0) and Execute must publish the same types.

use crate::{AggregateOp, ProjectionItem, ScalarBinaryOp, ScalarExpr, ScalarUnaryOp, Value};

fn aggregate_type(op: &AggregateOp, input: Option<String>) -> Option<String> {
    match op {
        AggregateOp::Count => Some("BIGINT".into()),
        AggregateOp::Min | AggregateOp::Max => input,
        AggregateOp::Sum => input.map(|ty| match ty.to_ascii_uppercase().as_str() {
            "INT" | "INTEGER" | "INT4" | "SMALLINT" | "INT2" => "BIGINT".into(),
            "BIGINT" | "INT8" => "NUMERIC".into(),
            _ => ty,
        }),
        AggregateOp::Avg
        | AggregateOp::StddevSamp
        | AggregateOp::StddevPop
        | AggregateOp::VarSamp
        | AggregateOp::VarPop => input.map(|ty| match ty.to_ascii_uppercase().as_str() {
            "REAL" | "FLOAT4" | "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" => {
                "DOUBLE PRECISION".into()
            }
            "INTERVAL" if *op == AggregateOp::Avg => ty,
            _ => "NUMERIC".into(),
        }),
        AggregateOp::StringAgg => Some(match input {
            Some(t) if crate::value::is_bytea_type(&t) => "BYTEA".into(),
            _ => "TEXT".into(),
        }),
        AggregateOp::ArrayAgg => input.map(|ty| format!("{ty}[]")),
        AggregateOp::BoolAnd | AggregateOp::BoolOr => Some("BOOLEAN".into()),
        AggregateOp::JsonAgg | AggregateOp::JsonObjectAgg => Some("JSON".into()),
        AggregateOp::JsonbAgg | AggregateOp::JsonbObjectAgg => Some("JSONB".into()),
        AggregateOp::BitAnd | AggregateOp::BitOr | AggregateOp::BitXor => input,
        AggregateOp::PercentileCont => match input {
            Some(t)
                if crate::datetime::Kind::of_type(&t) == Some(crate::datetime::Kind::Interval) =>
            {
                Some(t)
            }
            _ => Some("DOUBLE PRECISION".into()),
        },
        AggregateOp::PercentileDisc | AggregateOp::Mode | AggregateOp::AnyValue => input,
        AggregateOp::HypotheticalRank
        | AggregateOp::HypotheticalDenseRank
        | AggregateOp::RegrCount => Some("BIGINT".into()),
        AggregateOp::HypotheticalPercentRank
        | AggregateOp::HypotheticalCumeDist
        | AggregateOp::Corr
        | AggregateOp::CovarPop
        | AggregateOp::CovarSamp
        | AggregateOp::RegrSlope
        | AggregateOp::RegrIntercept
        | AggregateOp::RegrR2
        | AggregateOp::RegrAvgX
        | AggregateOp::RegrAvgY
        | AggregateOp::RegrSxx
        | AggregateOp::RegrSyy
        | AggregateOp::RegrSxy => Some("DOUBLE PRECISION".into()),
        // `range_agg` of ranges collects a multirange; of multiranges keeps
        // it. `range_intersect_agg` keeps the argument's shape.
        AggregateOp::RangeAgg => input.map(|ty| {
            crate::ranges::Kind::of(&ty)
                .map_or(ty.clone(), |kind| kind.multirange_name().to_string())
        }),
        AggregateOp::RangeIntersectAgg => input,
    }
}

fn literal_type(value: &Value) -> Option<String> {
    Some(
        match value {
            Value::Int(i) if i32::try_from(*i).is_ok() => "INTEGER",
            Value::Int(_) => "BIGINT",
            Value::Float(_) => "DOUBLE PRECISION",
            Value::Numeric(_) => "NUMERIC",
            Value::Bool(_) => "BOOLEAN",
            Value::Text(_) => "TEXT",
            Value::Jsonb(_) => "JSONB",
            Value::Json(_) => "JSON",
            Value::Bytea(_) => "BYTEA",
            Value::Array(items) => {
                return items
                    .iter()
                    .find_map(literal_type)
                    .map(|element| format!("{element}[]"));
            }
            _ => return None,
        }
        .into(),
    )
}

fn scalar_type(expr: &ScalarExpr, column: &impl Fn(&str) -> Option<String>) -> Option<String> {
    match expr {
        ScalarExpr::Column(name) => column(name),
        ScalarExpr::Window(call) => {
            let input = || call.args.first().and_then(|a| scalar_type(a, column));
            match call.func.as_str() {
                "ROW_NUMBER" | "RANK" | "DENSE_RANK" => Some("BIGINT".into()),
                "NTILE" => Some("INTEGER".into()),
                "PERCENT_RANK" | "CUME_DIST" => Some("DOUBLE PRECISION".into()),
                "LAG" | "LEAD" | "FIRST_VALUE" | "LAST_VALUE" | "NTH_VALUE" => input(),
                name => {
                    crate::planner::aggregate_op(name).and_then(|op| aggregate_type(&op, input()))
                }
            }
        }
        ScalarExpr::Literal(value) => literal_type(value),
        ScalarExpr::Cast { target, .. } => Some(target.clone()),
        // Operators take a domain's values as its base type's.
        ScalarExpr::Binary { op, left, right } => binary_type(
            *op,
            scalar_type(left, column).map(|t| crate::user_types::base_type(&t)),
            scalar_type(right, column).map(|t| crate::user_types::base_type(&t)),
        ),
        ScalarExpr::Unary {
            op: ScalarUnaryOp::Neg,
            expr,
        } => scalar_type(expr, column),
        ScalarExpr::Unary {
            op: ScalarUnaryOp::Not,
            ..
        }
        | ScalarExpr::IsNull { .. }
        | ScalarExpr::IsBool { .. }
        | ScalarExpr::IsDistinctFrom { .. }
        | ScalarExpr::PatternMatch { .. }
        | ScalarExpr::InList { .. }
        | ScalarExpr::Quantified { .. } => Some("BOOLEAN".into()),
        ScalarExpr::Case {
            branches,
            else_result,
            ..
        } => branches
            .iter()
            .map(|(_, result)| result)
            .chain(else_result.as_deref())
            .find_map(|result| scalar_type(result, column)),
        ScalarExpr::Extract { .. } => Some("NUMERIC".into()),
        // A date/time operator, by its operands' types.
        ScalarExpr::Function { name, args } if name == DATETIME_OP => {
            let text = |i: usize| match args.get(i) {
                Some(ScalarExpr::Literal(Value::Text(t))) => Some(t.as_str()),
                _ => None,
            };
            let kind = |i: usize| text(i).and_then(crate::datetime::Kind::of_type);
            crate::datetime::result_kind(text(0)?, kind(3), kind(4)).map(Into::into)
        }
        ScalarExpr::DateOffset { base, .. } => {
            match scalar_type(base, column).and_then(|t| crate::datetime::Kind::of_type(&t)) {
                Some(crate::datetime::Kind::TimestampTz) => Some("TIMESTAMPTZ".into()),
                Some(crate::datetime::Kind::Time) => Some("TIME".into()),
                _ => Some("TIMESTAMP".into()),
            }
        }
        // A network-family operator's result: the address or money type for
        // the set operations, and a boolean for the predicates.
        ScalarExpr::Function { name, args } if name == NET_OP => {
            match args.first() {
                Some(ScalarExpr::Literal(Value::Text(op)))
                    if matches!(op.as_str(), "+" | "-" | "*" | "/" | "&" | "|" | "~") =>
                {
                    // `money / money` is a ratio.
                    if op == "/"
                        && matches!(args.get(4), Some(ScalarExpr::Literal(Value::Bool(false))))
                        && matches!(
                            args.get(3),
                            Some(ScalarExpr::Literal(Value::Text(k))) if k == "money"
                        )
                    {
                        return Some("FLOAT8".into());
                    }
                    match args.get(3) {
                        Some(ScalarExpr::Literal(Value::Text(kind))) => Some(kind.clone()),
                        _ => None,
                    }
                }
                _ => Some("BOOLEAN".into()),
            }
        }
        ScalarExpr::Function { name, args } if name == NET_CAST => match args.get(1) {
            Some(ScalarExpr::Literal(Value::Text(to))) => Some(to.clone()),
            _ => None,
        },
        // A range operator's result: the range for the set operations, and
        // a boolean for the predicates.
        ScalarExpr::Function { name, args } if name == RANGE_OP => match args.first() {
            Some(ScalarExpr::Literal(Value::Text(op)))
                if matches!(op.as_str(), "+" | "-" | "*") =>
            {
                match args.get(3) {
                    Some(ScalarExpr::Literal(Value::Text(kind))) => Some(kind.clone()),
                    _ => None,
                }
            }
            _ => Some("BOOLEAN".into()),
        },
        // A field of a composite value, by the value's type.
        ScalarExpr::Function { name, args } if name == crate::user_types::FIELD => {
            match args.as_slice() {
                [value, ScalarExpr::Literal(Value::Text(field)), ..] => {
                    crate::user_types::field_type(&scalar_type(value, column)?, field)
                }
                _ => None,
            }
        }
        // The geometric operators, casts, and functions carry their result
        // types by name.
        ScalarExpr::Function { name, args } if name == GEO_OP => {
            let op = args.first().and_then(|arg| match arg {
                ScalarExpr::Literal(Value::Text(op)) => Some(op.as_str()),
                _ => None,
            })?;
            return Some(
                match op {
                    "<->" | "@-@" => "DOUBLE PRECISION",
                    "@@" => "POINT",
                    "#" => {
                        let left = args.get(1).and_then(|arg| {
                            scalar_type(arg, column).and_then(|t| crate::geometric::Kind::of(&t))
                        });
                        if left == Some(crate::geometric::Kind::Box) {
                            "BOX"
                        } else {
                            "POINT"
                        }
                    }
                    "##" => "POINT",
                    _ => "BOOLEAN",
                }
                .to_string(),
            );
        }
        ScalarExpr::Function { name, args } if name == GEO_CAST => {
            return args.get(1).and_then(|arg| match arg {
                ScalarExpr::Literal(Value::Text(kind)) => Some(kind.clone()),
                _ => None,
            });
        }
        ScalarExpr::Function { name, args } if name == GEO_FN => {
            let literal = |i: usize| match args.get(i) {
                Some(ScalarExpr::Literal(Value::Text(text))) => Some(text.as_str()),
                _ => None,
            };
            return crate::geometric::return_type(&literal(0)?.to_ascii_uppercase(), literal(1)?)
                .or_else(|| Some(literal(1)?.to_string()));
        }
        ScalarExpr::Function { name, args } if name == GEO_UNARY => {
            let op = args.first().and_then(|arg| match arg {
                ScalarExpr::Literal(Value::Text(op)) => Some(op.as_str()),
                _ => None,
            })?;
            return Some(
                match op {
                    "@-@" => "DOUBLE PRECISION",
                    "@@" => "POINT",
                    "#" => "INTEGER",
                    _ => "BOOLEAN",
                }
                .to_string(),
            );
        }
        // `greatest`/`least` of a range family keep its type.
        ScalarExpr::Function { name, args } if name == RANGE_GREATEST => {
            return args.get(1).and_then(|arg| match arg {
                ScalarExpr::Literal(Value::Text(kind)) => Some(kind.clone()),
                _ => None,
            });
        }
        // The multirange constructors carry their subtype's name as a
        // literal.
        ScalarExpr::Function { name, args } if name == MULTIRANGE_BUILD => {
            return args.first().and_then(|arg| match arg {
                ScalarExpr::Literal(Value::Text(kind)) => crate::ranges::Kind::of(kind)
                    .or_else(|| crate::multiranges::kind_of(kind))
                    .map(|kind| kind.multirange_name().to_string()),
                _ => None,
            });
        }
        ScalarExpr::Function { name, args } => crate::functions::return_type(
            name,
            &args
                .iter()
                .map(|arg| scalar_type(arg, column))
                .collect::<Vec<_>>(),
        ),
        ScalarExpr::Aggregate {
            op, arg, arg_expr, ..
        } => aggregate_type(
            op,
            arg_expr
                .as_ref()
                .and_then(|expr| scalar_type(expr, column))
                .or_else(|| column(arg)),
        ),
        _ => None,
    }
}

/// Integer types by width, for arithmetic result types.
fn integer_rank(ty: &str) -> Option<u8> {
    match ty.to_ascii_uppercase().as_str() {
        "SMALLINT" | "INT2" => Some(1),
        "INT" | "INTEGER" | "INT4" | "SERIAL" => Some(2),
        "BIGINT" | "INT8" | "BIGSERIAL" => Some(3),
        _ => None,
    }
}

fn is_float(ty: &str) -> bool {
    matches!(
        ty.to_ascii_uppercase().as_str(),
        "REAL" | "FLOAT4" | "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" | "FLOAT"
    )
}

fn is_numeric(ty: &str) -> bool {
    let upper = ty.to_ascii_uppercase();
    upper.starts_with("NUMERIC") || upper.starts_with("DECIMAL")
}

/// The result type of a binary operator, where the operand types decide it.
fn binary_type(op: ScalarBinaryOp, left: Option<String>, right: Option<String>) -> Option<String> {
    use ScalarBinaryOp as Op;
    match op {
        Op::Eq
        | Op::NotEq
        | Op::Lt
        | Op::LtEq
        | Op::Gt
        | Op::GtEq
        | Op::And
        | Op::Or
        | Op::JsonHasKey
        | Op::JsonHasAnyKey
        | Op::JsonHasAllKeys
        | Op::Contains
        | Op::ContainedBy
        | Op::JsonPathExists
        | Op::JsonPathMatch
        | Op::Overlap => Some("BOOLEAN".into()),
        Op::JsonGetText | Op::JsonPathText => Some("TEXT".into()),
        Op::JsonGet | Op::JsonPath => left,
        Op::Concat => match (&left, &right) {
            (Some(l), _) if l.ends_with("[]") => left,
            (_, Some(r)) if r.ends_with("[]") => right,
            (Some(t), _) | (_, Some(t)) if crate::value::is_bytea_type(t) => Some("BYTEA".into()),
            (Some(l), Some(r))
                if crate::bits::bit_type(l).is_some() && crate::bits::bit_type(r).is_some() =>
            {
                Some("VARBIT".into())
            }
            _ => Some("TEXT".into()),
        },
        Op::Add | Op::Sub | Op::Mul | Op::Div | Op::Mod => {
            // Date/time arithmetic has its own result types.
            let kind = |t: &Option<String>| t.as_deref().and_then(crate::datetime::Kind::of_type);
            if kind(&left).is_some() || kind(&right).is_some() {
                return crate::datetime::result_kind(arith_symbol(op), kind(&left), kind(&right))
                    .map(Into::into);
            }
            let (left, right) = (left?, right?);
            if let (Some(l), Some(r)) = (integer_rank(&left), integer_rank(&right)) {
                // Integer arithmetic is in the wider operand's type.
                Some(
                    match l.max(r) {
                        1 => "SMALLINT",
                        2 => "INTEGER",
                        _ => "BIGINT",
                    }
                    .into(),
                )
            } else if is_float(&left) || is_float(&right) {
                Some("DOUBLE PRECISION".into())
            } else if (is_numeric(&left) || integer_rank(&left).is_some())
                && (is_numeric(&right) || integer_rank(&right).is_some())
            {
                Some("NUMERIC".into())
            } else {
                None
            }
        }
    }
}

/// An arithmetic operator's symbol, as the date/time operators name it.
fn arith_symbol(op: ScalarBinaryOp) -> &'static str {
    match op {
        ScalarBinaryOp::Add => "+",
        ScalarBinaryOp::Sub => "-",
        ScalarBinaryOp::Mul => "*",
        ScalarBinaryOp::Div => "/",
        _ => "%",
    }
}

/// The type of a window function's result.
fn window_type(
    func_name: &str,
    args: &[String],
    column: &impl Fn(&str) -> Option<String>,
) -> Option<String> {
    let input = || args.first().and_then(|arg| column(arg));
    match func_name.to_ascii_uppercase().as_str() {
        "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "NTILE" | "COUNT" => Some("BIGINT".into()),
        "PERCENT_RANK" | "CUME_DIST" => Some("DOUBLE PRECISION".into()),
        "SUM" => aggregate_type(&AggregateOp::Sum, input()),
        "AVG" => aggregate_type(&AggregateOp::Avg, input()),
        "MIN" | "MAX" | "LAG" | "LEAD" | "FIRST_VALUE" | "LAST_VALUE" | "NTH_VALUE" => input(),
        _ => None,
    }
}

/// `expr` with each integer arithmetic whose result is `integer` or
/// `smallint` checked against that type's range, as PostgreSQL computes it
/// in that type (`2147483647 + 1` fails rather than widening). `column`
/// gives the declared types of the columns it names.
pub(crate) fn check_integer_ranges(
    expr: &ScalarExpr,
    column: &impl Fn(&str) -> Option<String>,
) -> ScalarExpr {
    let checked = expr.map_children(&mut |e| check_integer_ranges(e, column));
    if let Some(ordered) = enum_order(&checked, column) {
        return ordered;
    }
    // A field of a composite value learns the value's type.
    if let ScalarExpr::Function { name, args } = &checked
        && name == crate::user_types::FIELD
        && let [value, field] = args.as_slice()
        && let Some(ty) = scalar_type(value, column)
    {
        return ScalarExpr::Function {
            name: name.clone(),
            args: vec![
                value.clone(),
                field.clone(),
                ScalarExpr::Literal(Value::Text(ty)),
            ],
        };
    }
    // A range operator takes its subtype from the operand that has one
    // (`'[1,5)'::int4range @> 3`), and a cast between two range types is
    // refused as PostgreSQL refuses it.
    // A range family: a range type, or the multirange over one. The flag
    // marks the multirange, whose type names differ.
    let range_side = |e: &ScalarExpr| -> Option<(crate::ranges::Kind, bool)> {
        let data_type = scalar_type(e, column)?;
        if let Some(kind) = crate::ranges::Kind::of(&data_type) {
            return Some((kind, false));
        }
        crate::multiranges::kind_of(&data_type).map(|kind| (kind, true))
    };
    // The type name of a side, as PostgreSQL's messages write it.
    let side_name = |(kind, multirange): (crate::ranges::Kind, bool)| {
        if multirange {
            kind.multirange_name().to_string()
        } else {
            kind.name().to_string()
        }
    };
    let range_kind = |e: &ScalarExpr| range_side(e).map(|(kind, _)| kind);
    // A geometric type, whose operators and functions take it along.
    let geo_kind =
        |e: &ScalarExpr| scalar_type(e, column).and_then(|t| crate::geometric::Kind::of(&t));
    let geo_call = |op: &str, l: &ScalarExpr, r: &ScalarExpr, kind: crate::geometric::Kind| {
        let right_kind = geo_kind(r)
            .map(|kind| kind.name().to_string())
            .unwrap_or_default();
        ScalarExpr::Function {
            name: GEO_OP.to_string(),
            args: vec![
                ScalarExpr::Literal(Value::Text(op.to_string())),
                l.clone(),
                r.clone(),
                ScalarExpr::Literal(Value::Text(kind.name().to_string())),
                ScalarExpr::Literal(Value::Text(right_kind)),
            ],
        }
    };
    // An element bound to a range operator must be of its subtype; an
    // unknown or string literal is read as one.
    let element_fits = |kind: crate::ranges::Kind, e: &ScalarExpr| -> bool {
        let Some(data_type) = scalar_type(e, column) else {
            return true;
        };
        let data_type = data_type.trim().to_ascii_uppercase();
        if data_type.is_empty() || data_type == "UNKNOWN" || is_text_type(&data_type) {
            return true;
        }
        match kind {
            crate::ranges::Kind::Int4 | crate::ranges::Kind::Int8 => {
                integer_rank(&data_type).is_some()
            }
            crate::ranges::Kind::Numeric => {
                integer_rank(&data_type).is_some() || is_float(&data_type) || is_numeric(&data_type)
            }
            crate::ranges::Kind::Date => data_type == "DATE",
            crate::ranges::Kind::Timestamp => data_type.starts_with("TIMESTAMP"),
            crate::ranges::Kind::TimestampTz => {
                data_type.starts_with("TIMESTAMPTZ")
                    || data_type.starts_with("TIMESTAMP WITH TIME ZONE")
            }
        }
    };
    // The same for the network-family types and money.
    let net_kind = |e: &ScalarExpr| scalar_type(e, column).and_then(|t| crate::net::Kind::of(&t));
    let number = |e: &ScalarExpr| {
        scalar_type(e, column)
            .is_some_and(|t| integer_rank(&t).is_some() || is_float(&t) || is_numeric(&t))
    };
    let net_call =
        |op: &str, l: &ScalarExpr, r: &ScalarExpr, kind: crate::net::Kind, number: bool| {
            net_operator(op, l, r, kind, number)
        };
    // Two different network families have no common operator either.
    let mixed_net = |l: &ScalarExpr, r: &ScalarExpr, symbol: &str| -> Option<ScalarExpr> {
        match (net_kind(l), net_kind(r)) {
            (Some(a), Some(b)) if a != b => Some(bad_operator(a.name(), symbol, b.name())),
            _ => None,
        }
    };
    let range_call = range_operator;
    // Two different range types have no common operator, and the two
    // families mix only for the positional operators (a range and a
    // multirange of one subtype compare, but do not compare as a whole or
    // combine).
    let mixed = |l: &ScalarExpr, r: &ScalarExpr, symbol: &str| -> Option<ScalarExpr> {
        match (range_side(l), range_side(r)) {
            (Some(a), Some(b)) if a.0 != b.0 => {
                Some(bad_operator(&side_name(a), symbol, &side_name(b)))
            }
            (Some(a), Some(b))
                if a.1 != b.1
                    && matches!(
                        symbol,
                        "=" | "<>" | "<" | ">" | "<=" | ">=" | "+" | "-" | "*"
                    ) =>
            {
                Some(bad_operator(&side_name(a), symbol, &side_name(b)))
            }
            _ => None,
        }
    };
    match &checked {
        // A cast between two network-family types converts (`inet` to `cidr`
        // zeroes the host bits), and money does not cast to a number.
        ScalarExpr::Cast {
            expr: inner,
            target,
        } if let (Some(from), Some(to)) = (net_kind(inner), crate::net::Kind::of(target))
            && from != to =>
        {
            return ScalarExpr::Function {
                name: NET_CAST.to_string(),
                args: vec![
                    ScalarExpr::Literal(Value::Text(from.name().to_string())),
                    ScalarExpr::Literal(Value::Text(to.name().to_string())),
                    (**inner).clone(),
                ],
            };
        }
        ScalarExpr::Cast {
            expr: inner,
            target,
        } if net_kind(inner) == Some(crate::net::Kind::Money)
            && (integer_rank(target).is_some() || is_float(target)) =>
        {
            return ScalarExpr::Function {
                name: BAD_RANGE_CAST.to_string(),
                args: vec![
                    ScalarExpr::Literal(Value::Text("money".to_string())),
                    ScalarExpr::Literal(Value::Text(operator_type_name(target))),
                ],
            };
        }
        // Casts between the range family: only a range to the multirange
        // over its subtype exists; any other pairing errors.
        ScalarExpr::Cast {
            expr: inner,
            target,
        } if let Some(from) = range_side(inner) => {
            let to = crate::ranges::Kind::of(target)
                .map(|kind| (kind, false))
                .or_else(|| crate::multiranges::kind_of(target).map(|kind| (kind, true)));
            match to {
                Some(to) if to == from => {}
                Some((to_kind, true)) if to_kind == from.0 && !from.1 => {
                    // A range wraps into the one-element multirange.
                    return ScalarExpr::Function {
                        name: MULTIRANGE_CAST.to_string(),
                        args: vec![
                            ScalarExpr::Literal(Value::Text(
                                from.0.multirange_name().to_string(),
                            )),
                            (**inner).clone(),
                        ],
                    };
                }
                Some(to) => {
                    return ScalarExpr::Function {
                        name: BAD_RANGE_CAST.to_string(),
                        args: vec![
                            ScalarExpr::Literal(Value::Text(side_name(from))),
                            ScalarExpr::Literal(Value::Text(side_name(to))),
                        ],
                    };
                }
                None => {}
            }
        }
        // A jsonpath casts to text-family types only; anything else
        // (`jsonpath::jsonb`, `jsonpath::integer`) has no cast.
        ScalarExpr::Cast { expr: inner, target }
            if scalar_type(inner, column).is_some_and(|t| crate::jsonpath::is_type(&t))
                && !is_text_type(target)
                && !crate::jsonpath::is_type(target) =>
        {
            return ScalarExpr::Function {
                name: BAD_RANGE_CAST.to_string(),
                args: vec![
                    ScalarExpr::Literal(Value::Text("jsonpath".to_string())),
                    ScalarExpr::Literal(Value::Text(operator_type_name(target))),
                ],
            };
        }
        // Casts between geometric types: only PostgreSQL's casts exist.
        ScalarExpr::Cast { expr: inner, target }
            if let (Some(from), Some(to)) = (geo_kind(inner), crate::geometric::Kind::of(target)) =>
        {
            return ScalarExpr::Function {
                name: GEO_CAST.to_string(),
                args: vec![
                    ScalarExpr::Literal(Value::Text(from.name().to_string())),
                    ScalarExpr::Literal(Value::Text(to.name().to_string())),
                    (**inner).clone(),
                ],
            };
        }
        // A geometric function or constructor learns its type, and its
        // first argument's when that argument is geometric too.
        ScalarExpr::Function { name, args }
            if crate::geometric::Kind::of(name).is_some()
                || (matches!(
                    name.as_str(),
                    "CENTER"
                        | "RADIUS"
                        | "DIAMETER"
                        | "HEIGHT"
                        | "WIDTH"
                        | "DIAGONAL"
                        | "AREA"
                        | "NPOINTS"
                        | "ISCLOSED"
                        | "ISOPEN"
                        | "PCLOSE"
                        | "POPEN"
                        | "BOUND_BOX"
                ) && args.iter().any(|arg| geo_kind(arg).is_some())) =>
        {
            // `point('(1,2)')` reads the text as the type's input.
            if let Some(kind) = crate::geometric::Kind::of(name)
                && let [only] = args.as_slice()
                && scalar_type(only, column).is_none_or(|t| {
                    is_text_type(&t) || t.trim().is_empty() || t.eq_ignore_ascii_case("UNKNOWN")
                })
            {
                let _ = kind;
                return ScalarExpr::Cast {
                    expr: Box::new(only.clone()),
                    target: name.clone(),
                };
            }
            let result_kind = crate::geometric::Kind::of(name)
                .or_else(|| args.iter().find_map(|arg| geo_kind(arg)));
            let Some(result_kind) = result_kind else {
                return checked.clone();
            };
            let arg_kinds: Vec<Option<crate::geometric::Kind>> =
                args.iter().map(geo_kind).collect();
            if !crate::geometric::function_supported(
                &name.to_ascii_uppercase(),
                result_kind,
                &arg_kinds,
            ) {
                let types: Vec<String> = args
                    .iter()
                    .map(|arg| {
                        geo_kind(arg).map_or_else(
                            || operator_type_name(&scalar_type(arg, column).unwrap_or_default()),
                            |kind| kind.name().to_string(),
                        )
                    })
                    .collect();
                return bad_function_args(&name.to_ascii_lowercase(), &types);
            }
            let argument_kind = geo_kind(args.first().unwrap_or(&ScalarExpr::Literal(Value::Null)))
                .map(|kind| kind.name().to_string())
                .unwrap_or_default();
            let mut with_kind = vec![
                ScalarExpr::Literal(Value::Text(name.to_ascii_lowercase())),
                ScalarExpr::Literal(Value::Text(result_kind.name().to_string())),
                ScalarExpr::Literal(Value::Text(argument_kind)),
            ];
            with_kind.extend(args.iter().cloned());
            return ScalarExpr::Function {
                name: GEO_FN.to_string(),
                args: with_kind,
            };
        }
        // A unary geometric operator.
        ScalarExpr::Function { name, args }
            if name == GEO_UNARY
                && let [ScalarExpr::Literal(Value::Text(op)), value] = args.as_slice() =>
        {
            if let Some(kind) = geo_kind(value)
                && crate::geometric::unary_supported(kind, op)
            {
                return ScalarExpr::Function {
                    name: GEO_UNARY.to_string(),
                    args: vec![
                        ScalarExpr::Literal(Value::Text(op.to_string())),
                        (*value).clone(),
                        ScalarExpr::Literal(Value::Text(kind.name().to_string())),
                    ],
                };
            }
            let other = operator_type_name(&scalar_type(value, column).unwrap_or_default());
            return bad_operator("", op, &other);
        }
        // `#` of two geometric shapes is their intersection or point count,
        // not the bitwise exclusive or.
        ScalarExpr::Function { name, args }
            if name == BIT_XOR
                && let [l, r] = args.as_slice()
                && let Some(kind) = geo_kind(l).or_else(|| geo_kind(r)) =>
        {
            return geo_call("#", l, r, kind);
        }
        // `&`, `|`, and `~` of addresses are the address operations, not the
        // integer ones.
        ScalarExpr::Function { name, args }
            if matches!(name.as_str(), BIT_AND | BIT_OR)
                && let [l, r] = args.as_slice()
                && let Some(kind) = net_kind(l).or_else(|| net_kind(r)) =>
        {
            let op = if name == BIT_AND { "&" } else { "|" };
            return net_call(op, l, r, kind, false);
        }
        ScalarExpr::Function { name, args }
            if name == BIT_NOT
                && let [value] = args.as_slice()
                && let Some(kind) = net_kind(value) =>
        {
            return net_call("~", value, value, kind, false);
        }
        // `x << y` and `x >> y` shadow the bit shifts for ranges and the
        // geometric shapes.
        ScalarExpr::Function { name, args }
            if matches!(name.as_str(), SHIFT_LEFT | SHIFT_RIGHT)
                && let [l, r] = args.as_slice()
                && let (Some(left_kind), Some(right_kind)) = (geo_kind(l), geo_kind(r)) =>
        {
            let op = if name == SHIFT_LEFT { "<<" } else { ">>" };
            if !crate::geometric::supported(op, left_kind, right_kind) {
                return bad_operator(left_kind.name(), op, right_kind.name());
            }
            return geo_call(op, l, r, left_kind);
        }
        ScalarExpr::Function { name, args }
            if matches!(name.as_str(), SHIFT_LEFT | SHIFT_RIGHT)
                && let [l, r] = args.as_slice()
                && let Some(kind) = range_kind(l).or_else(|| range_kind(r)) =>
        {
            let op = if name == SHIFT_LEFT { "<<" } else { ">>" };
            return range_call(op, l, r, kind, false);
        }
        // The multirange constructors learn their subtype.
        ScalarExpr::Function { name, args }
            if let Some(kind) = crate::ranges::Kind::of_multirange(name) =>
        {
            let mut with_kind = vec![ScalarExpr::Literal(Value::Text(
                kind.name().to_string(),
            ))];
            with_kind.extend(args.iter().cloned());
            return ScalarExpr::Function {
                name: MULTIRANGE_BUILD.to_string(),
                args: with_kind,
            };
        }
        // `multirange(anyrange)` takes its subtype from its argument.
        ScalarExpr::Function { name, args }
            if name == "MULTIRANGE"
                && let [arg] = args.as_slice()
                && let Some((kind, false)) = range_side(arg) =>
        {
            return ScalarExpr::Function {
                name: MULTIRANGE_BUILD.to_string(),
                args: vec![
                    ScalarExpr::Literal(Value::Text(kind.name().to_string())),
                    arg.clone(),
                ],
            };
        }
        // `multirange(x)` takes a range, not a multirange.
        ScalarExpr::Function { name, args }
            if name == "MULTIRANGE"
                && let [arg] = args.as_slice()
                && let Some((kind, true)) = range_side(arg) =>
        {
            return bad_function("multirange", kind.multirange_name());
        }
        // `range_merge` of one range does not exist; of a multirange it
        // spans its elements.
        ScalarExpr::Function { name, args }
            if name == "RANGE_MERGE"
                && let [arg] = args.as_slice()
                && let Some((kind, multirange)) = range_side(arg)
                && !multirange =>
        {
            return bad_function("range_merge", kind.name());
        }
        // `greatest`/`least` of a range family pick by its order, which
        // compares bounds rather than text.
        ScalarExpr::Function { name, args }
            if matches!(name.as_str(), "GREATEST" | "LEAST")
                && let Some(side) = args.iter().find_map(|a| range_side(a)) =>
        {
            let mut with_kind = vec![
                ScalarExpr::Literal(Value::Text(name.to_ascii_lowercase())),
                ScalarExpr::Literal(Value::Text(side_name(side))),
            ];
            with_kind.extend(args.iter().cloned());
            return ScalarExpr::Function {
                name: RANGE_GREATEST.to_string(),
                args: with_kind,
            };
        }
        // `money(x)` is the cast into money.
        ScalarExpr::Function { name, args } if name == "MONEY" && args.len() == 1 => {
            return ScalarExpr::Cast {
                expr: Box::new(args[0].clone()),
                target: "MONEY".to_string(),
            };
        }
        // `text(money)` shows it with its symbol.
        ScalarExpr::Function { name, args }
            if name == "TEXT"
                && let [value] = args.as_slice()
                && net_kind(value) == Some(crate::net::Kind::Money) =>
        {
            return ScalarExpr::Function {
                name: MONEY_TEXT.to_string(),
                args: vec![value.clone()],
            };
        }
        // An argument of a function that takes any value (`to_json`,
        // `quote_literal`, ...) is what money shows.
        ScalarExpr::Function { name, args }
            if matches!(
                name.as_str(),
                "TO_JSON"
                    | "TO_JSONB"
                    | "JSON_BUILD_ARRAY"
                    | "JSONB_BUILD_ARRAY"
                    | "JSON_BUILD_OBJECT"
                    | "JSONB_BUILD_OBJECT"
                    | "QUOTE_LITERAL"
                    | "QUOTE_NULLABLE"
                    | "ARRAY"
            ) && args
                .iter()
                .any(|arg| net_kind(arg) == Some(crate::net::Kind::Money)) =>
        {
            return ScalarExpr::Function {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|arg| {
                        if net_kind(arg) == Some(crate::net::Kind::Money) {
                            ScalarExpr::Function {
                                name: MONEY_TEXT.to_string(),
                                args: vec![arg.clone()],
                            }
                        } else {
                            arg.clone()
                        }
                    })
                    .collect(),
            };
        }
        // No other function takes money: it is not a number (`abs(money)`,
        // `round(money)`, `to_char(money, ...)` do not exist). The functions
        // that take any value (`concat`, `format`, ...) keep their argument.
        ScalarExpr::Function { name, args }
            if !matches!(
                name.as_str(),
                "CASH"
                    | "CASH_WORDS"
                    | "CASHLARGER"
                    | "CASHSMALLER"
                    | "MONEY"
                    | "CONCAT"
                    | "CONCAT_WS"
                    | "FORMAT"
                    | "GREATEST"
                    | "LEAST"
                    | "COALESCE"
                    | "NULLIF"
                    | "PG_TYPEOF"
                    | "PG_COLUMN_SIZE"
            ) && !name.starts_with("__")
                && args
                    .iter()
                    .any(|arg| net_kind(arg) == Some(crate::net::Kind::Money)) =>
        {
            let types: Vec<String> = args
                .iter()
                .map(|arg| argument_type_name(arg, column))
                .collect();
            return bad_function_args(&name.to_ascii_lowercase(), &types);
        }
        // A custom operator (`@>`, `&&`, `<@`, ...) once its subtype is
        // known; `<@` reads as `@>` with its operands swapped, and with no
        // range operand the operator keeps its former meaning (`jsonb @>`).
        ScalarExpr::Function { name, args }
            if name == RANGE_OP
                && let [
                    ScalarExpr::Literal(Value::Text(op)),
                    l,
                    r,
                    ScalarExpr::Literal(Value::Text(kind_name)),
                    _,
                ] = args.as_slice()
                && kind_name.is_empty() =>
        {
            if let Some(found) = net_kind(l).or_else(|| net_kind(r)) {
                if let Some(bad) = mixed_net(l, r, op) {
                    return bad;
                }
                return net_call(op, l, r, found, false);
            }
            if let Some(kind) = geo_kind(l).or_else(|| geo_kind(r)) {
                let (Some(left_kind), Some(right_kind)) = (geo_kind(l), geo_kind(r)) else {
                    let name = |e: &ScalarExpr| {
                        geo_kind(e).map_or_else(
                            || {
                                operator_type_name(&scalar_type(e, column).unwrap_or_default())
                            },
                            |kind| kind.name().to_string(),
                        )
                    };
                    return bad_operator(&name(l), op, &name(r));
                };
                if !crate::geometric::supported(op, left_kind, right_kind) {
                    return bad_operator(left_kind.name(), op, right_kind.name());
                }
                return geo_call(op, l, r, kind);
            }
            let Some(found) = range_kind(l).or_else(|| range_kind(r)) else {
                let op = match op.as_str() {
                    "@>" => ScalarBinaryOp::Contains,
                    "<@" => ScalarBinaryOp::ContainedBy,
                    "&&" => ScalarBinaryOp::Overlap,
                    _ => return checked.clone(),
                };
                return ScalarExpr::Binary {
                    op,
                    left: Box::new(l.clone()),
                    right: Box::new(r.clone()),
                };
            };
            if let Some(bad) = mixed(l, r, op) {
                return bad;
            }
            // An untyped literal resolves to the range's own type, as
            // PostgreSQL resolves it; anything else outside the family is an
            // element.
            let untyped = |e: &ScalarExpr| matches!(e, ScalarExpr::Literal(Value::Text(_)));
            if op == "<@" {
                let element = range_kind(l).is_none() && !untyped(l);
                if element && !element_fits(found, l) {
                    let element_name =
                        operator_type_name(&scalar_type(l, column).unwrap_or_default());
                    let range_name = range_side(r).map(side_name).unwrap_or_default();
                    return bad_operator(&range_name, "@>", &element_name);
                }
                return range_call("@>", r, l, found, element);
            }
            let element = op == "@>" && range_kind(r).is_none() && !untyped(r);
            if element && !element_fits(found, r) {
                let element_name =
                    operator_type_name(&scalar_type(r, column).unwrap_or_default());
                let range_name = range_side(l).map(side_name).unwrap_or_default();
                return bad_operator(&range_name, op, &element_name);
            }
            return range_call(op, l, r, found, element);
        }
        // A window aggregate over an address or money carries its type the
        // same way the plain one does; macaddr has no `min`/`max`.
        ScalarExpr::Window(call) => {
            let kind = call.args.first().and_then(net_kind);
            let func = call.func.to_ascii_uppercase();
            if let (Some(kind), Some(op)) = (kind, crate::planner::aggregate_op(&func)) {
                // Behind `count`, `min`, and `max`, only money sums.
                let allowed = match kind {
                    crate::net::Kind::Money => matches!(
                        op,
                        AggregateOp::Count
                            | AggregateOp::Min
                            | AggregateOp::Max
                            | AggregateOp::Sum
                    ),
                    _ => matches!(
                        op,
                        AggregateOp::Count | AggregateOp::Min | AggregateOp::Max
                    ),
                };
                if !allowed {
                    return bad_function(&func.to_ascii_lowercase(), kind.name());
                }
            }
            // macaddr has neither `min` nor `max`.
            if matches!(func.as_str(), "MIN" | "MAX")
                && matches!(
                    kind,
                    Some(crate::net::Kind::MacAddr | crate::net::Kind::MacAddr8)
                )
            {
                let kind = kind.expect("guarded by the match");
                return bad_function(&func.to_ascii_lowercase(), kind.name());
            }
            let extra = match (func.as_str(), kind) {
                ("SUM", Some(crate::net::Kind::Money)) => Some("money"),
                ("MIN" | "MAX", Some(kind)) => Some(kind.name()),
                _ => None,
            };
            if let Some(extra) = extra
                && call.args.len() == 1
            {
                let mut call = (**call).clone();
                call.args
                    .push(ScalarExpr::Literal(Value::Text(extra.to_string())));
                return ScalarExpr::Window(Box::new(call));
            }
        }
        // `range_agg` and `range_intersect_agg` carry their subtype and the
        // input's shape wherever they are nested.
        ScalarExpr::Aggregate {
            op: op @ (AggregateOp::RangeAgg | AggregateOp::RangeIntersectAgg),
            arg,
            arg_expr,
            distinct,
            extra_args,
            filter,
            order_by,
        } if extra_args.is_empty()
            && let Some((kind, multirange)) = arg_expr
                .as_deref()
                .and_then(|e| range_side(e))
                .or_else(|| range_side(&ScalarExpr::Column(arg.clone()))) =>
        {
            return ScalarExpr::Aggregate {
                op: op.clone(),
                arg: arg.clone(),
                arg_expr: arg_expr.clone(),
                distinct: *distinct,
                extra_args: vec![
                    ScalarExpr::Literal(Value::Text(kind.name().to_string())),
                    ScalarExpr::Literal(Value::Bool(multirange)),
                ],
                filter: filter.clone(),
                order_by: order_by.clone(),
            };
        }
        // `sum` of money adds cents wherever it is nested (in `HAVING`, a
        // cast, or arithmetic); the type rides along as an extra argument.
        ScalarExpr::Aggregate {
            op: AggregateOp::Sum,
            arg,
            arg_expr,
            distinct,
            extra_args,
            filter,
            order_by,
        } if extra_args.is_empty()
            && arg_expr
                .as_deref()
                .and_then(net_kind)
                .or_else(|| net_kind(&ScalarExpr::Column(arg.clone())))
                == Some(crate::net::Kind::Money) =>
        {
            return ScalarExpr::Aggregate {
                op: AggregateOp::Sum,
                arg: arg.clone(),
                arg_expr: arg_expr.clone(),
                distinct: *distinct,
                extra_args: vec![ScalarExpr::Literal(Value::Text("money".to_string()))],
                filter: filter.clone(),
                order_by: order_by.clone(),
            };
        }
        // Only `count`, `array_agg`, and the JSON collectors take a
        // geometric value.
        ScalarExpr::Aggregate {
            op, arg, arg_expr, ..
        } if !matches!(
            op,
            AggregateOp::Count
                | AggregateOp::ArrayAgg
                | AggregateOp::JsonAgg
                | AggregateOp::JsonbAgg
                | AggregateOp::JsonObjectAgg
                | AggregateOp::JsonbObjectAgg
        ) && arg_expr
            .as_deref()
            .and_then(|e| geo_kind(e))
            .or_else(|| geo_kind(&ScalarExpr::Column(arg.clone())))
            .is_some() =>
        {
            let kind = arg_expr
                .as_deref()
                .and_then(|e| geo_kind(e))
                .or_else(|| geo_kind(&ScalarExpr::Column(arg.clone())))
                .expect("checked by the guard");
            return bad_function(&op.sql_name().to_ascii_lowercase(), kind.name());
        }
        // `greatest`/`least` need a comparison function, which no geometric
        // type has.
        ScalarExpr::Function { name, args }
            if matches!(name.as_str(), "GREATEST" | "LEAST")
                && let Some(kind) = args.iter().find_map(|arg| geo_kind(arg)) =>
        {
            return ScalarExpr::Function {
                name: BAD_COMPARISON.to_string(),
                args: vec![ScalarExpr::Literal(Value::Text(kind.name().to_string()))],
            };
        }
        // Only `count`, the collectors, and the range builders take a range
        // or multirange; `min`, `sum`, and the like have no overload.
        ScalarExpr::Aggregate {
            op, arg, arg_expr, ..
        } if !matches!(
            op,
            AggregateOp::Count
                | AggregateOp::ArrayAgg
                | AggregateOp::JsonAgg
                | AggregateOp::JsonbAgg
                | AggregateOp::JsonObjectAgg
                | AggregateOp::JsonbObjectAgg
                | AggregateOp::RangeAgg
                | AggregateOp::RangeIntersectAgg
        ) && arg_expr
            .as_deref()
            .and_then(|e| range_side(e))
            .or_else(|| range_side(&ScalarExpr::Column(arg.clone())))
            .is_some() =>
        {
            let side = arg_expr
                .as_deref()
                .and_then(|e| range_side(e))
                .or_else(|| range_side(&ScalarExpr::Column(arg.clone())))
                .expect("checked by the guard");
            return bad_function(&op.sql_name().to_ascii_lowercase(), &side_name(side));
        }
        // A jsonpath has no equality, so `IN`, `= ANY`, and `IS DISTINCT
        // FROM` are all the missing `=` operator.
        ScalarExpr::IsDistinctFrom { left, right, .. }
            if is_jsonpath_type(left, column) || is_jsonpath_type(right, column) =>
        {
            return bad_operator(
                &argument_type_name(left, column),
                "=",
                &argument_type_name(right, column),
            );
        }
        ScalarExpr::InList { expr, list, .. } if is_jsonpath_type(expr, column) => {
            let other = list
                .first()
                .map(|e| argument_type_name(e, column))
                .unwrap_or_else(|| "unknown".to_string());
            return bad_operator(&argument_type_name(expr, column), "=", &other);
        }
        ScalarExpr::Quantified { left, .. } if is_jsonpath_type(left, column) => {
            return bad_operator(&argument_type_name(left, column), "=", "jsonpath");
        }
        ScalarExpr::Binary { op, left, right } => {
            // A jsonpath has no comparison operator in PostgreSQL.
            if is_jsonpath_type(left, column) || is_jsonpath_type(right, column) {
                let symbol = match op {
                    ScalarBinaryOp::Eq => Some("="),
                    ScalarBinaryOp::NotEq => Some("<>"),
                    ScalarBinaryOp::Lt => Some("<"),
                    ScalarBinaryOp::LtEq => Some("<="),
                    ScalarBinaryOp::Gt => Some(">"),
                    ScalarBinaryOp::GtEq => Some(">="),
                    _ => None,
                };
                if let Some(symbol) = symbol {
                    return bad_operator(
                        &argument_type_name(left, column),
                        symbol,
                        &argument_type_name(right, column),
                    );
                }
            }
            // `jsonb @? jsonpath` and `jsonb @@ jsonpath`: left `jsonb`, right
            // `jsonpath`; an untyped literal is read as either.
            if matches!(
                op,
                ScalarBinaryOp::JsonPathExists | ScalarBinaryOp::JsonPathMatch
            ) {
                let untyped = |e: &ScalarExpr| {
                    matches!(e, ScalarExpr::Literal(Value::Text(_)))
                        || scalar_type(e, column)
                            .is_none_or(|t| t.trim().is_empty() || t.eq_ignore_ascii_case("UNKNOWN"))
                };
                let is_jsonb = |e: &ScalarExpr| {
                    untyped(e)
                        || scalar_type(e, column).is_some_and(|t| crate::value::is_jsonb_type(&t))
                };
                let is_path = |e: &ScalarExpr| {
                    untyped(e)
                        || scalar_type(e, column).is_some_and(|t| crate::jsonpath::is_type(&t))
                };
                if !is_jsonb(left) || !is_path(right) {
                    let symbol = if *op == ScalarBinaryOp::JsonPathExists {
                        "@?"
                    } else {
                        "@@"
                    };
                    return bad_operator(
                        &argument_type_name(left, column),
                        symbol,
                        &argument_type_name(right, column),
                    );
                }
                // A literal path is the jsonpath it names, in canonical form.
                let right = match right.as_ref() {
                    ScalarExpr::Literal(Value::Text(text)) => {
                        Box::new(jsonpath_literal(text))
                    }
                    other => Box::new(other.clone()),
                };
                return ScalarExpr::Binary {
                    op: *op,
                    left: left.clone(),
                    right,
                };
            }
            if *op == ScalarBinaryOp::JsonHasAnyKey
                && let (Some(left_kind), Some(right_kind)) = (geo_kind(left), geo_kind(right))
            {
                if !crate::geometric::supported("?|", left_kind, right_kind) {
                    return bad_operator(left_kind.name(), "?|", right_kind.name());
                }
                return geo_call("?|", left, right, left_kind);
            }
            // `range || x` has an operator only where `x` is text, and
            // `range || range` has none at all.
            if *op == ScalarBinaryOp::Concat
                && let Some(kind) = range_kind(left).or_else(|| range_kind(right))
            {
                let text_side = |e: &ScalarExpr| {
                    scalar_type(e, column).is_none_or(|t| {
                        is_text_type(&t) || t.trim().is_empty() || t.eq_ignore_ascii_case("UNKNOWN")
                    })
                };
                // The other side is the one that decides: text concatenates,
                // anything else has no operator.
                let other = if range_kind(left).is_some() {
                    right
                } else {
                    left
                };
                if !text_side(other) {
                    let range_name = kind.name().to_string();
                    let other_name =
                        operator_type_name(&scalar_type(other, column).unwrap_or_default());
                    return if range_kind(left).is_some() {
                        bad_operator(&range_name, "||", &other_name)
                    } else {
                        bad_operator(&other_name, "||", &range_name)
                    };
                }
            }
            if let Some(kind) = geo_kind(left).or_else(|| geo_kind(right)) {
                let symbol = match op {
                    ScalarBinaryOp::Eq => Some("="),
                    ScalarBinaryOp::NotEq => Some("<>"),
                    ScalarBinaryOp::Lt => Some("<"),
                    ScalarBinaryOp::Gt => Some(">"),
                    ScalarBinaryOp::LtEq => Some("<="),
                    ScalarBinaryOp::GtEq => Some(">="),
                    ScalarBinaryOp::Add => Some("+"),
                    ScalarBinaryOp::Sub => Some("-"),
                    ScalarBinaryOp::Mul => Some("*"),
                    ScalarBinaryOp::Div => Some("/"),
                    _ => None,
                };
                if let Some(symbol) = symbol {
                    // A geometric operand needs the other side's kind for the
                    // operator to exist.
                    let (Some(left_kind), Some(right_kind)) = (geo_kind(left), geo_kind(right))
                    else {
                        let name = |e: &ScalarExpr| {
                            geo_kind(e).map_or_else(
                                || {
                                    operator_type_name(
                                        &scalar_type(e, column).unwrap_or_default(),
                                    )
                                },
                                |kind| kind.name().to_string(),
                            )
                        };
                        return bad_operator(&name(left), symbol, &name(right));
                    };
                    if !crate::geometric::supported(symbol, left_kind, right_kind) {
                        return bad_operator(left_kind.name(), symbol, right_kind.name());
                    }
                    return geo_call(symbol, left, right, kind);
                }
            }
            if let Some(kind) = net_kind(left).or_else(|| net_kind(right)) {
                let symbol = match op {
                    ScalarBinaryOp::Eq => Some("="),
                    ScalarBinaryOp::NotEq => Some("<>"),
                    ScalarBinaryOp::Lt => Some("<"),
                    ScalarBinaryOp::Gt => Some(">"),
                    ScalarBinaryOp::LtEq => Some("<="),
                    ScalarBinaryOp::GtEq => Some(">="),
                    ScalarBinaryOp::Add => Some("+"),
                    ScalarBinaryOp::Sub => Some("-"),
                    ScalarBinaryOp::Mul if kind == crate::net::Kind::Money => Some("*"),
                    ScalarBinaryOp::Div if kind == crate::net::Kind::Money => Some("/"),
                    _ => None,
                };
                let right_number = number(right)
                    || (kind == crate::net::Kind::Money
                        && scalar_type(right, column).is_none());
                if let Some(symbol) = symbol {
                    // `inet` has `+` with a number and `-` with a number or
                    // another address; money takes money on either side.
                    let monetary = kind == crate::net::Kind::Money;
                    let right_kind = net_kind(right);
                    // An untyped string literal reads as PostgreSQL's
                    // `unknown` and resolves to whatever the operator takes.
                    let unknown = |e: &ScalarExpr| {
                        scalar_type(e, column).is_none()
                            || matches!(e, ScalarExpr::Literal(Value::Text(_)))
                    };
                    // A number money scales by: an integer, a float, or a
                    // numeric (which PostgreSQL casts to a float), but not
                    // another money.
                    let number_operand = |e: &ScalarExpr| {
                        scalar_type(e, column).is_some_and(|t| {
                            integer_rank(&t).is_some() || is_float(&t) || is_numeric(&t)
                        })
                    };
                    let left_money = net_kind(left) == Some(crate::net::Kind::Money);
                    let right_money = right_kind == Some(crate::net::Kind::Money);
                    let ok = if monetary {
                        let other = if left_money { right } else { left };
                        match symbol {
                            // Money compares with money (an unknown literal
                            // resolves to money).
                            "=" | "<>" | "<" | ">" | "<=" | ">=" => {
                                left_money && right_money || unknown(other)
                            }
                            // Money adds to money, and to an unknown literal.
                            "+" | "-" => left_money && right_money || unknown(other),
                            // Money scales by a number on either side, but
                            // not by money.
                            "*" => {
                                (left_money
                                    && !right_money
                                    && (number_operand(right) || unknown(right)))
                                    || (right_money
                                        && (number_operand(left) || unknown(left)))
                            }
                            // `money / money` is a ratio; otherwise money is
                            // the dividend.
                            "/" => {
                                left_money
                                    && (right_money
                                        || number_operand(right)
                                        || unknown(right))
                            }
                            _ => false,
                        }
                    } else {
                        match symbol {
                            "+" => right_number || unknown(right),
                            "-" => right_kind.is_some() || right_number || unknown(right),
                            _ => true,
                        }
                    };
                    if ok {
                        // `money + money` (and `-`) reads an unknown literal
                        // as money too; `*` and `/` take the number as one.
                        let number = match (kind, symbol) {
                            (crate::net::Kind::Money, "+" | "-") => false,
                            _ => right_kind.is_none(),
                        };
                        return net_call(symbol, left, right, kind, number);
                    }
                    // No overload: name both types as PostgreSQL does.
                    let other = if net_kind(left).is_some() { right } else { left };
                    let other_name =
                        operator_type_name(&scalar_type(other, column).unwrap_or_default());
                    return if net_kind(left).is_some() {
                        bad_operator(kind.name(), symbol, &other_name)
                    } else {
                        bad_operator(&other_name, symbol, kind.name())
                    };
                }
            }
            if let Some(kind) = range_kind(left).or_else(|| range_kind(right)) {
                let symbol = match op {
                    ScalarBinaryOp::Eq => Some("="),
                    ScalarBinaryOp::NotEq => Some("<>"),
                    ScalarBinaryOp::Lt => Some("<"),
                    ScalarBinaryOp::Gt => Some(">"),
                    ScalarBinaryOp::LtEq => Some("<="),
                    ScalarBinaryOp::GtEq => Some(">="),
                    ScalarBinaryOp::Add => Some("+"),
                    ScalarBinaryOp::Sub => Some("-"),
                    ScalarBinaryOp::Mul => Some("*"),
                    _ => None,
                };
                // `+`, `-`, `*`, and the comparisons are for two ranges or
                // two multiranges; with an element (or across the families)
                // PostgreSQL has no such operator. An untyped literal takes
                // the other side's type, as PostgreSQL resolves it.
                let untyped = |e: &ScalarExpr| {
                    matches!(
                        e,
                        ScalarExpr::Literal(Value::Text(_)) | ScalarExpr::Literal(Value::Null)
                    )
                };
                let both_ranges = range_side(left).is_some_and(|(_, m)| !m)
                    && (range_side(right).is_some_and(|(_, m)| !m) || untyped(right))
                    || untyped(left) && range_side(right).is_some_and(|(_, m)| !m);
                let both_multiranges = range_side(left).is_some_and(|(_, m)| m)
                    && (range_side(right).is_some_and(|(_, m)| m) || untyped(right))
                    || untyped(left) && range_side(right).is_some_and(|(_, m)| m);
                let set_operation = matches!(
                    op,
                    ScalarBinaryOp::Add | ScalarBinaryOp::Sub | ScalarBinaryOp::Mul
                );
                if let Some(symbol) = symbol {
                    if let Some(bad) = mixed(left, right, symbol) {
                        return bad;
                    }
                    if !both_ranges && !both_multiranges {
                        // One side is an element (or the other family): name
                        // both as PostgreSQL does.
                        let name = |e: &ScalarExpr| {
                            range_side(e).map_or_else(
                                || {
                                    operator_type_name(
                                        &scalar_type(e, column).unwrap_or_default(),
                                    )
                                },
                                side_name,
                            )
                        };
                        return bad_operator(&name(left), symbol, &name(right));
                    }
                    return range_call(symbol, left, right, kind, false);
                }
            }
        }
        // An address function learns its argument's type
        // (`set_masklen` masks a `cidr` but keeps an `inet`'s address).
        ScalarExpr::Function { name, args }
            if matches!(
                name.as_str(),
                "HOST" | "NETMASK" | "HOSTMASK" | "BROADCAST" | "NETWORK" | "MASKLEN"
                    | "SET_MASKLEN" | "ABBREV" | "FAMILY" | "INET_SAME_FAMILY" | "INET_MERGE"
                    | "MACADDR8_SET7BIT"
            ) && let Some(kind) = args.first().and_then(|a| net_kind(a))
                && args.last().is_none_or(|a| !matches!(a, ScalarExpr::Literal(Value::Text(t)) if crate::net::Kind::of(t).is_some())) =>
        {
            let mut args = args.clone();
            args.push(ScalarExpr::Literal(Value::Text(kind.name().to_string())));
            return ScalarExpr::Function {
                name: name.clone(),
                args,
            };
        }
        // `trunc` of a MAC address zeroes its low bytes.
        ScalarExpr::Function { name, args }
            if name == "TRUNC"
                && let [arg] = args.as_slice()
                && let Some(kind @ (crate::net::Kind::MacAddr | crate::net::Kind::MacAddr8)) =
                    net_kind(arg) =>
        {
            return ScalarExpr::Function {
                name: "__MAC_TRUNC__".to_string(),
                args: vec![
                    arg.clone(),
                    ScalarExpr::Literal(Value::Text(kind.name().to_string())),
                ],
            };
        }
        // `lower` and `upper` of a range read its bounds, where the string
        // functions would fold its text's case.
        ScalarExpr::Function { name, args }
            if matches!(name.as_str(), "LOWER" | "UPPER")
                && let [arg] = args.as_slice()
                && range_kind(arg).is_some() =>
        {
            let subtype = range_kind(arg).expect("checked by the guard").subtype();
            return ScalarExpr::Function {
                name: if name == "LOWER" {
                    RANGE_LOWER
                } else {
                    RANGE_UPPER
                }
                .to_string(),
                args: vec![
                    arg.clone(),
                    ScalarExpr::Literal(Value::Text(subtype.to_string())),
                ],
            };
        }
        _ => {}
    }
    // Date/time arithmetic takes its operators from its operands' types:
    // `time + interval` wraps at midnight, `date + interval` is a timestamp.
    let kind =
        |e: &ScalarExpr| scalar_type(e, column).and_then(|t| crate::datetime::Kind::of_type(&t));
    let kind_name = |k: Option<crate::datetime::Kind>| {
        ScalarExpr::Literal(Value::Text(k.map_or("", |k| k.type_name()).to_string()))
    };
    match &checked {
        ScalarExpr::Binary {
            op:
                op @ (ScalarBinaryOp::Add
                | ScalarBinaryOp::Sub
                | ScalarBinaryOp::Mul
                | ScalarBinaryOp::Div),
            left,
            right,
        } if kind(left).is_some() || kind(right).is_some() => {
            return ScalarExpr::Function {
                name: DATETIME_OP.to_string(),
                args: vec![
                    ScalarExpr::Literal(Value::Text(arith_symbol(*op).to_string())),
                    (**left).clone(),
                    (**right).clone(),
                    kind_name(kind(left)),
                    kind_name(kind(right)),
                ],
            };
        }
        ScalarExpr::Unary {
            op: ScalarUnaryOp::Neg,
            expr: inner,
        } if kind(inner) == Some(crate::datetime::Kind::Interval) => {
            return ScalarExpr::Function {
                name: DATETIME_OP.to_string(),
                args: vec![
                    ScalarExpr::Literal(Value::Text("neg".to_string())),
                    (**inner).clone(),
                    ScalarExpr::Literal(Value::Null),
                    kind_name(kind(inner)),
                    kind_name(None),
                ],
            };
        }
        _ => {}
    }
    // Intervals order (and partition) by their length of time, as sort keys
    // of an aggregate's or a window's; their text would not.
    let interval = |e: &ScalarExpr| kind(e) == Some(crate::datetime::Kind::Interval);
    let span = |e: &ScalarExpr| {
        if interval(e) {
            ScalarExpr::Function {
                name: INTERVAL_SPAN.to_string(),
                args: vec![e.clone()],
            }
        } else {
            e.clone()
        }
    };
    match &checked {
        ScalarExpr::Aggregate {
            op,
            arg,
            arg_expr,
            distinct,
            extra_args,
            filter,
            order_by,
        } if order_by.iter().any(|(e, _, _)| interval(e)) => {
            return ScalarExpr::Aggregate {
                op: op.clone(),
                arg: arg.clone(),
                arg_expr: arg_expr.clone(),
                distinct: *distinct,
                extra_args: extra_args.clone(),
                filter: filter.clone(),
                order_by: order_by
                    .iter()
                    .map(|(e, asc, nulls)| (span(e), *asc, *nulls))
                    .collect(),
            };
        }
        // A RANGE frame's offsets measure the key itself.
        ScalarExpr::Window(call)
            if (call.order_by.iter().any(|(e, _, _)| interval(e))
                || call.partition_by.iter().any(interval))
                && !call.frame.as_ref().is_some_and(|f| {
                    f.units == crate::plan_types::WindowFrameUnits::Range
                        && [&f.start, &f.end].iter().any(|b| {
                            matches!(
                                b,
                                crate::plan_types::FrameBound::Preceding(_)
                                    | crate::plan_types::FrameBound::Following(_)
                            )
                        })
                }) =>
        {
            let mut call = (**call).clone();
            call.partition_by = call.partition_by.iter().map(span).collect();
            call.order_by = call
                .order_by
                .iter()
                .map(|(e, asc, nulls)| (span(e), *asc, *nulls))
                .collect();
            return ScalarExpr::Window(Box::new(call));
        }
        _ => {}
    }
    // A zoned timestamp is kept in UTC: its local date, time, or timestamp
    // is in the session's zone, and so is its text.
    let zoned = |e: &ScalarExpr| kind(e) == Some(crate::datetime::Kind::TimestampTz);
    let session_text = |e: &ScalarExpr| ScalarExpr::Function {
        name: TZ_TEXT.to_string(),
        args: vec![e.clone()],
    };
    // A `real` is kept as a double; its text and its numeric value are the
    // `real`'s (`0.1::real::text` is `0.1`).
    let real =
        |e: &ScalarExpr| scalar_type(e, column).is_some_and(|t| crate::value::is_real_type(&t));
    let money = |e: &ScalarExpr| net_kind(e) == Some(crate::net::Kind::Money);
    let call = |name: &str, e: &ScalarExpr| ScalarExpr::Function {
        name: name.to_string(),
        args: vec![e.clone()],
    };
    let textual = |e: &ScalarExpr| zoned(e) || real(e) || money(e);
    let text_form = |e: &ScalarExpr| {
        if zoned(e) {
            session_text(e)
        } else if real(e) {
            call(REAL_TEXT, e)
        } else if money(e) {
            call(MONEY_TEXT, e)
        } else {
            e.clone()
        }
    };
    // A bit string as an integer is its bits, and the bit string
    // functions read bits rather than text.
    let bit_typed = |e: &ScalarExpr| {
        scalar_type(e, column).is_some_and(|t| crate::bits::bit_type(&t).is_some())
    };
    if let ScalarExpr::Cast {
        expr: inner,
        target,
    } = &checked
        && bit_typed(inner)
        && let Some(rank) = integer_rank(target)
    {
        return ScalarExpr::Cast {
            expr: Box::new(ScalarExpr::Function {
                name: BITS.to_string(),
                args: vec![
                    ScalarExpr::Literal(Value::Text("int".into())),
                    (**inner).clone(),
                    ScalarExpr::Literal(Value::Int(if rank == 3 { 64 } else { 32 })),
                ],
            }),
            target: target.clone(),
        };
    }
    if let ScalarExpr::Function { name, args } = &checked
        && matches!(
            name.as_str(),
            "GET_BIT" | "SET_BIT" | "BIT_COUNT" | "OCTET_LENGTH" | "BIT_LENGTH"
        )
        && args.first().is_some_and(bit_typed)
    {
        return ScalarExpr::Function {
            name: BITS.to_string(),
            args: [ScalarExpr::Literal(Value::Text(name.to_ascii_lowercase()))]
                .into_iter()
                .chain(args.iter().cloned())
                .collect(),
        };
    }
    // An integer as `bytea` is its bytes at its type's width.
    if let ScalarExpr::Cast {
        expr: inner,
        target,
    } = &checked
        && crate::value::is_bytea_type(target)
        && let Some(rank) = scalar_type(inner, column).as_deref().and_then(integer_rank)
    {
        return ScalarExpr::Function {
            name: INT_BYTEA.to_string(),
            args: vec![
                (**inner).clone(),
                ScalarExpr::Literal(Value::Int(match rank {
                    1 => 2,
                    2 => 4,
                    _ => 8,
                })),
            ],
        };
    }
    if let ScalarExpr::Cast {
        expr: inner,
        target,
    } = &checked
        && money(inner)
        && is_text_type(target)
    {
        return call(MONEY_TEXT, inner);
    }
    if let ScalarExpr::Cast {
        expr: inner,
        target,
    } = &checked
        && real(inner)
    {
        if is_text_type(target) {
            return call(REAL_TEXT, inner);
        }
        if is_numeric(target) {
            return ScalarExpr::Cast {
                expr: Box::new(call(REAL_NUMERIC, inner)),
                target: target.clone(),
            };
        }
    }
    match &checked {
        ScalarExpr::Cast {
            expr: inner,
            target,
        } if zoned(inner) => {
            use crate::datetime::Kind;
            if matches!(
                Kind::of_type(target),
                Some(Kind::Date | Kind::Timestamp | Kind::Time)
            ) {
                let local = ScalarExpr::Function {
                    name: "TIMEZONE".to_string(),
                    args: vec![
                        ScalarExpr::Function {
                            name: "CURRENT_SETTING".to_string(),
                            args: vec![ScalarExpr::Literal(Value::Text("TimeZone".to_string()))],
                        },
                        (**inner).clone(),
                        ScalarExpr::Literal(Value::Text(String::new())),
                        ScalarExpr::Literal(Value::Text("TIMESTAMPTZ".to_string())),
                    ],
                };
                return ScalarExpr::Cast {
                    expr: Box::new(local),
                    target: target.clone(),
                };
            }
            if is_text_type(target) {
                return session_text(inner);
            }
        }
        ScalarExpr::Binary {
            op: ScalarBinaryOp::Concat,
            left,
            right,
        } if textual(left) || textual(right) => {
            return ScalarExpr::Binary {
                op: ScalarBinaryOp::Concat,
                left: Box::new(text_form(left)),
                right: Box::new(text_form(right)),
            };
        }
        // Text-building functions take a zoned timestamp's text.
        ScalarExpr::Function { name, args }
            if matches!(name.as_str(), "CONCAT" | "CONCAT_WS" | "FORMAT")
                && args.iter().any(textual) =>
        {
            return ScalarExpr::Function {
                name: name.clone(),
                args: args.iter().map(text_form).collect(),
            };
        }
        // A timestamp is JSON in ISO 8601 form (`2024-07-01T12:00:00`).
        ScalarExpr::Function { name, args }
            if matches!(
                name.as_str(),
                "TO_JSON"
                    | "TO_JSONB"
                    | "JSON_BUILD_OBJECT"
                    | "JSONB_BUILD_OBJECT"
                    | "JSON_BUILD_ARRAY"
                    | "JSONB_BUILD_ARRAY"
            ) && args.iter().any(|a| {
                matches!(
                    kind(a),
                    Some(crate::datetime::Kind::Timestamp | crate::datetime::Kind::TimestampTz)
                )
            }) =>
        {
            let iso = |a: &ScalarExpr| match kind(a) {
                Some(
                    k @ (crate::datetime::Kind::Timestamp | crate::datetime::Kind::TimestampTz),
                ) => ScalarExpr::Function {
                    name: JSON_TIME.to_string(),
                    args: vec![
                        a.clone(),
                        ScalarExpr::Literal(Value::Text(k.type_name().to_string())),
                    ],
                },
                _ => a.clone(),
            };
            return ScalarExpr::Function {
                name: name.clone(),
                args: args.iter().map(iso).collect(),
            };
        }
        _ => {}
    }
    // `timezone(zone, value)` reads its arguments by their types: an
    // interval zone is east of UTC, and a time becomes a zoned time.
    // So does OVERLAPS, whose ends may be times or intervals.
    if let ScalarExpr::Function { name, args } = &checked
        && ((name == "TIMEZONE" && args.len() == 2) || (name == "OVERLAPS" && args.len() == 4))
    {
        // An unknown type is empty, as a NULL argument would make the
        // call NULL.
        let type_name = |e: &ScalarExpr| {
            ScalarExpr::Literal(Value::Text(scalar_type(e, column).unwrap_or_default()))
        };
        let types: Vec<ScalarExpr> = args.iter().map(type_name).collect();
        return ScalarExpr::Function {
            name: name.clone(),
            args: args.iter().cloned().chain(types).collect(),
        };
    }
    // `to_hex` of an `integer` shows 32 bits, and `pg_column_size` measures
    // a value by its type.
    if let ScalarExpr::Function { name, args } = &checked
        && matches!(
            name.as_str(),
            "TO_HEX" | "TO_BIN" | "TO_OCT" | "PG_COLUMN_SIZE"
        )
        && let [arg] = args.as_slice()
    {
        let ty = scalar_type(arg, column)
            .unwrap_or_default()
            .to_ascii_uppercase();
        return ScalarExpr::Function {
            name: name.clone(),
            args: vec![arg.clone(), ScalarExpr::Literal(Value::Text(ty))],
        };
    }
    // A `char(n)` value is kept without its padding, which the functions
    // that read its output form and LIKE see.
    let padded = |e: &ScalarExpr| -> ScalarExpr {
        match scalar_type(e, column).and_then(|t| crate::value::character_limit(&t)) {
            Some((length, true)) => ScalarExpr::Function {
                name: BPCHAR_PAD.to_string(),
                args: vec![e.clone(), ScalarExpr::Literal(Value::Int(length as i64))],
            },
            _ => e.clone(),
        }
    };
    match &checked {
        ScalarExpr::Function { name, args }
            if matches!(
                name.as_str(),
                "CONCAT" | "CONCAT_WS" | "FORMAT" | "OCTET_LENGTH" | "PG_COLUMN_SIZE"
            ) =>
        {
            let padded_args: Vec<ScalarExpr> = args.iter().map(padded).collect();
            if padded_args
                .iter()
                .any(|a| matches!(a, ScalarExpr::Function { name, .. } if name == BPCHAR_PAD))
            {
                return ScalarExpr::Function {
                    name: name.clone(),
                    args: padded_args,
                };
            }
        }
        ScalarExpr::PatternMatch {
            expr,
            pattern,
            kind,
            case_insensitive,
            negated,
            escape,
        } if matches!(padded(expr), ScalarExpr::Function { ref name, .. } if name == BPCHAR_PAD) => {
            return ScalarExpr::PatternMatch {
                expr: Box::new(padded(expr)),
                pattern: pattern.clone(),
                kind: *kind,
                case_insensitive: *case_insensitive,
                negated: *negated,
                escape: *escape,
            };
        }
        _ => {}
    }
    // A `char(n)` value compared with a literal ignores the literal's
    // trailing blanks, as it does its own.
    if let ScalarExpr::Binary { op, left, right } = &checked
        && matches!(
            op,
            ScalarBinaryOp::Eq
                | ScalarBinaryOp::NotEq
                | ScalarBinaryOp::Lt
                | ScalarBinaryOp::LtEq
                | ScalarBinaryOp::Gt
                | ScalarBinaryOp::GtEq
        )
    {
        let is_padded = |e: &ScalarExpr| {
            scalar_type(e, column)
                .and_then(|t| crate::value::character_limit(&t))
                .is_some_and(|(_, padded)| padded)
        };
        let trimmed = |e: &ScalarExpr| match e {
            ScalarExpr::Literal(Value::Text(t)) if t.ends_with(' ') => Some(ScalarExpr::Literal(
                Value::Text(t.trim_end_matches(' ').to_string()),
            )),
            _ => None,
        };
        if is_padded(left)
            && let Some(r) = trimmed(right)
        {
            return ScalarExpr::Binary {
                op: *op,
                left: left.clone(),
                right: Box::new(r),
            };
        }
        if is_padded(right)
            && let Some(l) = trimmed(left)
        {
            return ScalarExpr::Binary {
                op: *op,
                left: Box::new(l),
                right: right.clone(),
            };
        }
    }
    // A bitwise operator computes in its result type (`1 << 31` wraps in
    // `integer`).
    if let ScalarExpr::Function { name, args } = &checked
        && bitwise_arity(name) == Some(args.len())
    {
        let ty = scalar_type(&checked, column).unwrap_or_default();
        return ScalarExpr::Function {
            name: name.clone(),
            args: args
                .iter()
                .cloned()
                .chain([ScalarExpr::Literal(Value::Text(ty))])
                .collect(),
        };
    }
    // `pg_typeof` reports the argument's declared type, which a value alone
    // cannot tell (a `smallint` column holds integers too); an untyped
    // string literal is `unknown`.
    if let ScalarExpr::Function { name, args } = &checked
        && name == "PG_TYPEOF"
        && let [arg] = args.as_slice()
    {
        if matches!(arg, ScalarExpr::Literal(Value::Text(_))) {
            return ScalarExpr::Literal(Value::Text("unknown".to_string()));
        }
        if let Some(ty) = scalar_type(arg, column) {
            let name = crate::functions::format_type_name(crate::MemExecutor::pg_type_oid(&ty));
            if name != "???" {
                return ScalarExpr::Literal(Value::Text(name));
            }
        }
    }
    let arithmetic = matches!(
        expr,
        ScalarExpr::Binary {
            op: ScalarBinaryOp::Add
                | ScalarBinaryOp::Sub
                | ScalarBinaryOp::Mul
                | ScalarBinaryOp::Div
                | ScalarBinaryOp::Mod,
            ..
        } | ScalarExpr::Unary {
            op: ScalarUnaryOp::Neg,
            ..
        }
    );
    match scalar_type(&checked, column).as_deref() {
        Some(ty @ ("INTEGER" | "SMALLINT")) if arithmetic => ScalarExpr::Function {
            name: INTEGER_RANGE.to_string(),
            args: vec![checked, ScalarExpr::Literal(Value::Text(ty.to_string()))],
        },
        _ => checked,
    }
}

/// The functions the bitwise operators are evaluated by: `&`, `|`, `#`,
/// `<<`, `>>`, and `~`. [`check_integer_ranges`] appends their result type,
/// which sets the width an integer shift wraps at.
pub(crate) const BIT_AND: &str = "__BITAND__";
pub(crate) const BIT_OR: &str = "__BITOR__";
pub(crate) const BIT_XOR: &str = "__BITXOR__";
pub(crate) const SHIFT_LEFT: &str = "__SHIFTLEFT__";
pub(crate) const SHIFT_RIGHT: &str = "__SHIFTRIGHT__";
pub(crate) const BIT_NOT: &str = "__BITNOT__";

/// Whether a function is one of the bitwise operators, and how many operands
/// it takes.
pub(crate) fn bitwise_arity(name: &str) -> Option<usize> {
    match name {
        BIT_AND | BIT_OR | BIT_XOR | SHIFT_LEFT | SHIFT_RIGHT => Some(2),
        BIT_NOT => Some(1),
        _ => None,
    }
}

/// The result type of a bitwise operator on operands of these types: the
/// wider integer (a shift keeps its left operand's), or a bit string.
pub(crate) fn bitwise_type(name: &str, arg_types: &[Option<String>]) -> Option<String> {
    let left = arg_types.first().cloned().flatten();
    if matches!(name, SHIFT_LEFT | SHIFT_RIGHT | BIT_NOT) {
        return left;
    }
    let right = arg_types.get(1).cloned().flatten();
    match (&left, &right) {
        (Some(l), Some(r)) => match (integer_rank(l), integer_rank(r)) {
            (Some(a), Some(b)) if b > a => right,
            _ => left,
        },
        (None, _) => right,
        _ => left,
    }
}

/// The function an interval sort key is rewritten to: its length of time,
/// as a number of microseconds (a month as 30 days).
pub(crate) const INTERVAL_SPAN: &str = "__INTERVAL_SPAN__";

/// The function a range operator is rewritten to once a subtype is known:
/// `__RANGE__(operator, left, right, subtype, right-is-an-element)`.
pub(crate) const RANGE_OP: &str = "__RANGE__";

/// The call a range operator is rewritten to.
pub(crate) fn range_operator(
    op: &str,
    left: &ScalarExpr,
    right: &ScalarExpr,
    kind: crate::ranges::Kind,
    right_is_element: bool,
) -> ScalarExpr {
    ScalarExpr::Function {
        name: RANGE_OP.to_string(),
        args: vec![
            ScalarExpr::Literal(Value::Text(op.to_string())),
            left.clone(),
            right.clone(),
            ScalarExpr::Literal(Value::Text(kind.name().to_string())),
            ScalarExpr::Literal(Value::Bool(right_is_element)),
        ],
    }
}

/// The functions a range's `lower` and `upper` are rewritten to (the string
/// `lower` would lowercase the text).
pub(crate) const RANGE_LOWER: &str = "__RANGE_LOWER__";
pub(crate) const RANGE_UPPER: &str = "__RANGE_UPPER__";

/// The function a cast between two range types is rewritten to: PostgreSQL
/// has no such cast (`cannot cast type int4range to int8range`).
pub(crate) const BAD_RANGE_CAST: &str = "__BAD_RANGE_CAST__";

/// The function a range's cast to the multirange over its subtype is
/// rewritten to: `__MULTIRANGE_FROM_RANGE__(type, value)`.
pub(crate) const MULTIRANGE_CAST: &str = "__MULTIRANGE_FROM_RANGE__";

/// The function the multirange constructors are rewritten to:
/// `__MULTIRANGE_BUILD__(subtype, range...)`.
pub(crate) const MULTIRANGE_BUILD: &str = "__MULTIRANGE_BUILD__";

/// The function a geometric operator is rewritten to once its types are
/// known: `__GEO__(operator, left, right, type, right-type)`.
pub(crate) const GEO_OP: &str = "__GEO__";

/// The function a geometric cast between two of the types is rewritten to:
/// `__GEO_CAST__(from, to, value)`.
pub(crate) const GEO_CAST: &str = "__GEO_CAST__";

/// The function a geometric function or constructor is rewritten to:
/// `__GEO_FN__(name, type, arguments...)`.
pub(crate) const GEO_FN: &str = "__GEO_FN__";

/// The function a unary geometric operator is rewritten to:
/// `__GEO_UNARY__(operator, value)`.
pub(crate) const GEO_UNARY: &str = "__GEO_UNARY__";

/// The error a call needing a comparison function is rewritten to
/// (`__BAD_COMPARISON__(type)`).
pub(crate) const BAD_COMPARISON: &str = "__BAD_COMPARISON__";

/// The `jsonb_path_*` functions (their `_tz` spellings included).
pub(crate) const JSONPATH_FUNCTIONS: &[&str] = &[
    "JSONB_PATH_EXISTS",
    "JSONB_PATH_EXISTS_TZ",
    "JSONB_PATH_MATCH",
    "JSONB_PATH_MATCH_TZ",
    "JSONB_PATH_QUERY",
    "JSONB_PATH_QUERY_TZ",
    "JSONB_PATH_QUERY_ARRAY",
    "JSONB_PATH_QUERY_ARRAY_TZ",
    "JSONB_PATH_QUERY_FIRST",
    "JSONB_PATH_QUERY_FIRST_TZ",
];

/// The function `greatest`/`least` over a range or multirange is rewritten
/// to: `__RANGE_GREATEST__(name, type, value...)`, picking by its order.
pub(crate) const RANGE_GREATEST: &str = "__RANGE_GREATEST__";

/// The function a network-family operator is rewritten to once its type is
/// known: `__NET__(operator, left, right, type, right-is-compatible)`.
pub(crate) const NET_OP: &str = "__NET__";

/// The function a cast between two network-family types is rewritten to:
/// `__NET_CAST__(from, to, value)`.
pub(crate) const NET_CAST: &str = "__NET_CAST__";

/// The function money's text is rewritten to: its display text with the
/// currency symbol and thousands separators.
pub(crate) const MONEY_TEXT: &str = "__MONEY_TEXT__";

/// The call a network-family operator is rewritten to.
pub(crate) fn net_operator(
    op: &str,
    left: &ScalarExpr,
    right: &ScalarExpr,
    kind: crate::net::Kind,
    right_is_number: bool,
) -> ScalarExpr {
    ScalarExpr::Function {
        name: NET_OP.to_string(),
        args: vec![
            ScalarExpr::Literal(Value::Text(op.to_string())),
            left.clone(),
            right.clone(),
            ScalarExpr::Literal(Value::Text(kind.name().to_string())),
            ScalarExpr::Literal(Value::Bool(right_is_number)),
        ],
    }
}

/// The function an operator PostgreSQL has no overload for is rewritten to:
/// `__BAD_OPERATOR__(left type, operator, right type)`.
pub(crate) const BAD_OPERATOR: &str = "__BAD_OPERATOR__";

/// The function a call PostgreSQL has no overload for is rewritten to:
/// `__BAD_FUNCTION__(name, argument type)`.
pub(crate) const BAD_FUNCTION: &str = "__BAD_FUNCTION__";

/// The call an aggregate with no overload for its argument is rewritten to
/// (`min(int4range)` does not exist).
pub(crate) fn bad_function(name: &str, data_type: &str) -> ScalarExpr {
    bad_function_args(name, std::slice::from_ref(&data_type.to_string()))
}

/// The same call with a list of argument types, as PostgreSQL names them
/// (`function mod(money, integer) does not exist`).
pub(crate) fn bad_function_args(name: &str, types: &[String]) -> ScalarExpr {
    let mut args = vec![ScalarExpr::Literal(Value::Text(name.to_string()))];
    args.extend(
        types
            .iter()
            .map(|t| ScalarExpr::Literal(Value::Text(t.clone()))),
    );
    ScalarExpr::Function {
        name: BAD_FUNCTION.to_string(),
        args,
    }
}

/// The name of an argument's type in a "function ... does not exist" message;
/// an untyped literal is `unknown`, as PostgreSQL reports it.
fn argument_type_name(expr: &ScalarExpr, column: &impl Fn(&str) -> Option<String>) -> String {
    if matches!(expr, ScalarExpr::Literal(Value::Text(_)))
        && scalar_type(expr, column).is_none_or(|t| is_text_type(&t) || t.trim().is_empty())
    {
        return "unknown".to_string();
    }
    match scalar_type(expr, column) {
        Some(t) if !t.trim().is_empty() && !t.eq_ignore_ascii_case("UNKNOWN") => {
            operator_type_name(&t)
        }
        _ => "unknown".to_string(),
    }
}

/// The error call for an operator PostgreSQL has no overload for.
fn bad_operator(left: &str, op: &str, right: &str) -> ScalarExpr {
    ScalarExpr::Function {
        name: BAD_OPERATOR.to_string(),
        args: vec![
            ScalarExpr::Literal(Value::Text(left.to_string())),
            ScalarExpr::Literal(Value::Text(op.to_string())),
            ScalarExpr::Literal(Value::Text(right.to_string())),
        ],
    }
}

/// The type name PostgreSQL's operator errors print for a declared type.
/// A literal jsonpath argument: the text parsed and rewritten in canonical
/// form, as a cast (an invalid path keeps its text and fails when cast, as
/// PostgreSQL raises it).
fn jsonpath_literal(text: &str) -> ScalarExpr {
    let text = crate::jsonpath::canonical(text).unwrap_or_else(|_| text.to_string());
    ScalarExpr::Cast {
        expr: Box::new(ScalarExpr::Literal(Value::Text(text))),
        target: "jsonpath".to_string(),
    }
}

/// Whether an expression's declared type is `jsonpath`.
fn is_jsonpath_type(expr: &ScalarExpr, column: &impl Fn(&str) -> Option<String>) -> bool {
    scalar_type(expr, column).is_some_and(|t| crate::jsonpath::is_type(&t))
}

fn operator_type_name(data_type: &str) -> String {
    if let Some(kind) = crate::ranges::Kind::of(data_type) {
        return kind.name().to_string();
    }
    crate::functions::format_type_name(crate::MemExecutor::pg_type_oid(data_type))
}

/// The function a `char(n)` value is padded by where its padding shows:
/// `__BPCHAR_PAD__(value, n)`.
pub(crate) const BPCHAR_PAD: &str = "__BPCHAR_PAD__";

/// The function the bit string operations that differ from text's are
/// rewritten to: `__BITS__(operation, bits, ...)`.
pub(crate) const BITS: &str = "__BITS__";

/// The function an integer cast to `bytea` is rewritten to:
/// `__INT_BYTEA__(value, width)`.
pub(crate) const INT_BYTEA: &str = "__INT_BYTEA__";

/// The functions a `real`'s text and numeric value are rewritten to.
pub(crate) const REAL_TEXT: &str = "__REAL_TEXT__";
pub(crate) const REAL_NUMERIC: &str = "__REAL_NUMERIC__";

/// The function a zoned timestamp's text is rewritten to: the value as
/// the session shows it.
pub(crate) const TZ_TEXT: &str = "__TZ_TEXT__";

/// The function a timestamp given to `to_json` is rewritten to:
/// `__JSON_TIME__(value, type)`, its ISO 8601 text.
pub(crate) const JSON_TIME: &str = "__JSON_TIME__";

/// Whether a declared type is a character string type.
fn is_text_type(data_type: &str) -> bool {
    let upper = data_type.trim().to_ascii_uppercase();
    let base = upper.split('(').next().unwrap_or_default().trim();
    matches!(
        base,
        "TEXT" | "VARCHAR" | "CHARACTER VARYING" | "CHAR" | "CHARACTER" | "BPCHAR" | "NAME"
    )
}

/// The function date/time arithmetic is rewritten to:
/// `__DATETIME__(op, left, right, left_type, right_type)`.
pub(crate) const DATETIME_OP: &str = "__DATETIME__";

/// The function [`check_integer_ranges`] wraps a result in: its first
/// argument, or an error when that is outside the type named by the second.
pub(crate) const INTEGER_RANGE: &str = "__INTEGER_RANGE__";

/// The enum an expression's value is of, by name.
fn enum_of(expr: &ScalarExpr, column: &impl Fn(&str) -> Option<String>) -> Option<String> {
    let ty = scalar_type(expr, column)?;
    crate::user_types::enum_type(&ty).map(|_| ty)
}

/// An enum value as its place in the enum's order.
fn enum_place(expr: &ScalarExpr, ty: &str) -> ScalarExpr {
    ScalarExpr::Function {
        name: crate::user_types::ENUM_SORT.to_string(),
        args: vec![
            expr.clone(),
            ScalarExpr::Literal(Value::Text(ty.to_string())),
        ],
    }
}

/// An expression over an enum as its order decides it: a comparison of
/// its values compares their places, `min` and `max` pick by place, and
/// `enum_range`, `enum_first`, and `enum_last` learn the enum.
fn enum_order(expr: &ScalarExpr, column: &impl Fn(&str) -> Option<String>) -> Option<ScalarExpr> {
    use ScalarBinaryOp as Op;
    match expr {
        ScalarExpr::Binary {
            op: op @ (Op::Eq | Op::NotEq | Op::Lt | Op::LtEq | Op::Gt | Op::GtEq),
            left,
            right,
        } => {
            let ty = enum_of(left, column).or_else(|| enum_of(right, column))?;
            Some(ScalarExpr::Binary {
                op: *op,
                left: Box::new(enum_place(left, &ty)),
                right: Box::new(enum_place(right, &ty)),
            })
        }
        ScalarExpr::Function { name, args }
            if matches!(name.as_str(), "GREATEST" | "LEAST") && !args.is_empty() =>
        {
            let ty = args.iter().find_map(|a| enum_of(a, column))?;
            Some(ScalarExpr::Function {
                name: crate::user_types::ENUM_LABEL.to_string(),
                args: vec![
                    ScalarExpr::Function {
                        name: name.clone(),
                        args: args.iter().map(|a| enum_place(a, &ty)).collect(),
                    },
                    ScalarExpr::Literal(Value::Text(ty)),
                ],
            })
        }
        ScalarExpr::Function { name, args }
            if matches!(name.as_str(), "ENUM_RANGE" | "ENUM_FIRST" | "ENUM_LAST")
                && (1..=2).contains(&args.len()) =>
        {
            let ty = args.iter().find_map(|a| enum_of(a, column))?;
            let mut args = args.clone();
            args.push(ScalarExpr::Literal(Value::Text(ty)));
            Some(ScalarExpr::Function {
                name: name.clone(),
                args,
            })
        }
        ScalarExpr::Aggregate {
            op: op @ (AggregateOp::Min | AggregateOp::Max),
            arg,
            arg_expr,
            distinct,
            extra_args,
            filter,
            order_by,
        } => {
            let input = arg_expr
                .as_deref()
                .cloned()
                .unwrap_or_else(|| ScalarExpr::Column(arg.clone()));
            let ty = enum_of(&input, column)?;
            Some(ScalarExpr::Function {
                name: crate::user_types::ENUM_LABEL.to_string(),
                args: vec![
                    ScalarExpr::Aggregate {
                        op: op.clone(),
                        arg: arg.clone(),
                        arg_expr: Some(Box::new(enum_place(&input, &ty))),
                        distinct: *distinct,
                        extra_args: extra_args.clone(),
                        filter: filter.clone(),
                        order_by: order_by.clone(),
                    },
                    ScalarExpr::Literal(Value::Text(ty)),
                ],
            })
        }
        _ => None,
    }
}

/// A sort key over an enum column, as its place in the enum's order.
pub(crate) fn enum_sort_key(
    name: &str,
    column: &impl Fn(&str) -> Option<String>,
) -> Option<ScalarExpr> {
    enum_sort_expr(&ScalarExpr::Column(name.to_string()), column)
}

/// A sort key of an enum value, as its place in the enum's order.
pub(crate) fn enum_sort_expr(
    key: &ScalarExpr,
    column: &impl Fn(&str) -> Option<String>,
) -> Option<ScalarExpr> {
    enum_of(key, column).map(|ty| enum_place(key, &ty))
}

/// A condition with [`check_integer_ranges`] applied to its expressions.
pub(crate) fn check_filter_integer_ranges(
    filter: &crate::FilterExpr,
    column: &impl Fn(&str) -> Option<String>,
) -> crate::FilterExpr {
    use crate::FilterExpr as F;
    let check = |e: &ScalarExpr| check_integer_ranges(e, column);
    let recur = |f: &F| Box::new(check_filter_integer_ranges(f, column));
    // A `char(n)` column compared with a literal ignores the literal's
    // trailing blanks, as it does its own.
    let padded = |name: &str| {
        column(name)
            .and_then(|t| crate::value::character_limit(&t))
            .is_some_and(|(_, padded)| padded)
    };
    // A range predicate reads the range, not its text (`r @> 3`).
    let range_symbol = |op: &crate::CompareOp| -> Option<&'static str> {
        use crate::CompareOp as C;
        Some(match op {
            C::Eq => "=",
            C::Ne => "<>",
            C::Lt => "<",
            C::Le => "<=",
            C::Gt => ">",
            C::Ge => ">=",
            C::Contains => "@>",
            C::ContainedBy => "<@",
            _ => return None,
        })
    };
    let range_predicate = |left: &ScalarExpr, op: &crate::CompareOp, right: &ScalarExpr| {
        // A range column, or the multirange over one.
        let kind = |e: &ScalarExpr| {
            expr_type(e, column).and_then(|t| {
                crate::ranges::Kind::of(&t).or_else(|| crate::multiranges::kind_of(&t))
            })
        };
        let kind = kind(left)?;
        let symbol = range_symbol(op)?;
        let right_is_element = expr_type(right, column)
            .and_then(|t| crate::ranges::Kind::of(&t).or_else(|| crate::multiranges::kind_of(&t)))
            .is_none();
        Some(if symbol == "<@" {
            range_operator("@>", right, left, kind, !right_is_element)
        } else {
            range_operator(
                symbol,
                left,
                right,
                kind,
                symbol == "@>" && right_is_element,
            )
        })
    };
    match filter {
        F::Predicate(crate::Predicate { left, op, right }) => {
            let operand = |operand: &crate::Operand| match operand {
                crate::Operand::Literal(v) => ScalarExpr::Literal(v.clone()),
                crate::Operand::Ident(name) => ScalarExpr::Column(name.clone()),
            };
            if let Some(scalar) =
                range_predicate(&ScalarExpr::Column(left.clone()), op, &operand(right))
            {
                return F::Scalar(scalar);
            }
        }
        F::ExprCmp { left, op, right } => {
            if let Some(scalar) = range_predicate(left, op, right) {
                return F::Scalar(scalar);
            }
        }
        _ => {}
    }
    match filter {
        // An enum column compares in the enum's order.
        F::Predicate(crate::Predicate { left, op, right })
            if matches!(
                op,
                crate::CompareOp::Eq
                    | crate::CompareOp::Ne
                    | crate::CompareOp::Lt
                    | crate::CompareOp::Le
                    | crate::CompareOp::Gt
                    | crate::CompareOp::Ge
            ) && column(left).is_some_and(|t| crate::user_types::enum_type(&t).is_some()) =>
        {
            let ty = column(left).unwrap_or_default();
            let right = match right {
                crate::Operand::Literal(v) => ScalarExpr::Literal(v.clone()),
                crate::Operand::Ident(name) => ScalarExpr::Column(name.clone()),
            };
            F::ExprCmp {
                left: enum_place(&ScalarExpr::Column(left.clone()), &ty),
                op: *op,
                right: enum_place(&right, &ty),
            }
        }
        F::Predicate(crate::Predicate {
            left,
            op,
            right: crate::Operand::Literal(Value::Text(text)),
        }) if padded(left) && text.ends_with(' ') => F::Predicate(crate::Predicate {
            left: left.clone(),
            op: *op,
            right: crate::Operand::Literal(Value::Text(text.trim_end_matches(' ').to_string())),
        }),
        F::And(a, b) => F::And(recur(a), recur(b)),
        F::Or(a, b) => F::Or(recur(a), recur(b)),
        F::Not(a) => F::Not(recur(a)),
        F::ExprCmp { left, op, right } => F::ExprCmp {
            left: check(left),
            op: op.clone(),
            right: check(right),
        },
        F::Scalar(e) => F::Scalar(check(e)),
        F::QuantifiedSubquery {
            left,
            op,
            subquery,
            all,
        } => F::QuantifiedSubquery {
            left: check(left),
            op: *op,
            subquery: subquery.clone(),
            all: *all,
        },
        other => other.clone(),
    }
}

/// The type of a scalar expression, given its columns' declared types.
pub(crate) fn expr_type(
    expr: &ScalarExpr,
    column: &impl Fn(&str) -> Option<String>,
) -> Option<String> {
    scalar_type(expr, column)
}

/// The type a value has, as a literal of it would.
pub(crate) fn value_type(value: &Value) -> String {
    literal_type(value).unwrap_or_else(|| "TEXT".to_string())
}

/// The type of a scalar expression that references no columns.
pub(crate) fn constant_expr_type(expr: &ScalarExpr) -> Option<String> {
    scalar_type(expr, &|_: &str| None)
}

pub(crate) fn projection_type(
    item: &ProjectionItem,
    column: impl Fn(&str) -> Option<String>,
) -> Option<String> {
    match item {
        ProjectionItem::Aggregate(op, arg) => aggregate_type(op, column(arg)),
        ProjectionItem::Expr { expr, .. } => scalar_type(expr, &column),
        ProjectionItem::Literal(value) | ProjectionItem::AliasedLiteral(value, _) => {
            literal_type(value)
        }
        ProjectionItem::Column(name) | ProjectionItem::AliasedColumn(name, _) => column(name),
        ProjectionItem::WindowFunction {
            func_name, args, ..
        } => window_type(func_name, args, &column),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_types_do_not_depend_on_rows() {
        assert_eq!(
            aggregate_type(&AggregateOp::Count, None).as_deref(),
            Some("BIGINT")
        );
        assert_eq!(
            aggregate_type(&AggregateOp::Sum, Some("INTEGER".into())).as_deref(),
            Some("BIGINT")
        );
        assert_eq!(
            aggregate_type(&AggregateOp::Sum, Some("BIGINT".into())).as_deref(),
            Some("NUMERIC")
        );
        assert_eq!(
            aggregate_type(&AggregateOp::Min, Some("DATE".into())).as_deref(),
            Some("DATE")
        );
    }
}
