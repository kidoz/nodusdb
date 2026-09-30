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
    let call = |name: &str, e: &ScalarExpr| ScalarExpr::Function {
        name: name.to_string(),
        args: vec![e.clone()],
    };
    let textual = |e: &ScalarExpr| zoned(e) || real(e);
    let text_form = |e: &ScalarExpr| {
        if zoned(e) {
            session_text(e)
        } else if real(e) {
            call(REAL_TEXT, e)
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
