//! Statement-scoped runtime errors from scalar evaluation.
//!
//! Scalar evaluation returns plain values so it can run inside filters,
//! projections, sorts, and aggregates. A runtime error there (division by zero,
//! overflow, invalid input, a function domain error) is recorded here instead,
//! and the executor fails the statement once evaluation finishes, so the error
//! reaches the client rather than being silently replaced by NULL.

use std::cell::RefCell;

thread_local! {
    static EVAL_ERROR: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Records a runtime error for the current statement (the first one wins) and
/// returns NULL as the placeholder result of the failed evaluation.
pub(crate) fn raise(message: impl Into<String>) -> crate::Value {
    EVAL_ERROR.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.is_none() {
            *slot = Some(message.into());
        }
    });
    crate::Value::Null
}

/// Clears any error left behind by an earlier statement on this thread.
pub(crate) fn reset() {
    EVAL_ERROR.with(|slot| slot.borrow_mut().take());
}

/// Fails with the error recorded since the last [`reset`], if any.
pub(crate) fn check() -> anyhow::Result<()> {
    match EVAL_ERROR.with(|slot| slot.borrow_mut().take()) {
        Some(message) => Err(anyhow::anyhow!(message)),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_error_wins_and_check_clears_it() {
        reset();
        assert!(check().is_ok());
        assert_eq!(raise("division by zero"), crate::Value::Null);
        raise("a later error");
        assert_eq!(check().unwrap_err().to_string(), "division by zero");
        assert!(check().is_ok());
    }
}
