//! Common table expressions visible while a statement runs.
//!
//! A `WITH` query's CTEs (and a query's derived tables) are computed once and
//! bound here for the rest of that query's execution, so every nested part of
//! it — subqueries, set operation branches, join sources, later CTEs — can read
//! them. A name resolves to the innermost binding first, and a CTE shadows a
//! table of the same name, as in PostgreSQL.

use std::cell::RefCell;
use std::rc::Rc;

use crate::QueryOutput;

thread_local! {
    static SCOPE: RefCell<Vec<(String, Rc<QueryOutput>)>> = const { RefCell::new(Vec::new()) };
}

/// Removes a binding (and any made after it) when its query finishes.
pub(crate) struct ScopeGuard {
    len: usize,
}

impl Drop for ScopeGuard {
    fn drop(&mut self) {
        SCOPE.with(|scope| scope.borrow_mut().truncate(self.len));
    }
}

/// Makes `output` readable as the relation `name` until the guard drops.
pub(crate) fn bind(name: &str, output: QueryOutput) -> ScopeGuard {
    SCOPE.with(|scope| {
        let mut scope = scope.borrow_mut();
        let len = scope.len();
        scope.push((key(name), Rc::new(output)));
        ScopeGuard { len }
    })
}

/// The rows bound to `name`, innermost binding first.
pub(crate) fn lookup(name: &str) -> Option<Rc<QueryOutput>> {
    let name = key(name);
    SCOPE.with(|scope| {
        scope
            .borrow()
            .iter()
            .rev()
            .find(|(bound, _)| *bound == name)
            .map(|(_, output)| output.clone())
    })
}

/// Runs `f` with no bindings visible: a view's stored query is resolved
/// against tables only, whatever CTEs the query reading it defines.
pub(crate) fn isolated<R>(f: impl FnOnce() -> R) -> R {
    let saved = SCOPE.with(|scope| std::mem::take(&mut *scope.borrow_mut()));
    let result = f();
    SCOPE.with(|scope| *scope.borrow_mut() = saved);
    result
}

fn key(name: &str) -> String {
    name.trim_matches('"').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named(column: &str) -> QueryOutput {
        QueryOutput {
            columns: vec![column.to_string()],
            ..Default::default()
        }
    }

    #[test]
    fn inner_bindings_shadow_outer_ones_until_dropped() {
        let _outer = bind("x", named("outer"));
        {
            let _inner = bind("x", named("inner"));
            assert_eq!(lookup("x").unwrap().columns, ["inner"]);
            assert!(isolated(|| lookup("x").is_none()));
        }
        assert_eq!(lookup("x").unwrap().columns, ["outer"]);
        assert!(lookup("y").is_none());
    }
}
