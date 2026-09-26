//! Advisory locks (`pg_advisory_lock` and friends): application-defined
//! locks on a key, held by a session (until it unlocks them or ends) or by
//! its transaction (until that ends), shared or exclusive, and counted: a
//! session takes a lock it holds again, and releases it as often.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// A lock's key: one `bigint`, or two `integer`s (a separate space).
pub(crate) type LockKey = (bool, i64);

#[derive(Default)]
struct LockState {
    /// Per session, how often it holds the lock exclusively, and shared.
    exclusive: HashMap<String, u32>,
    shared: HashMap<String, u32>,
}

impl LockState {
    fn is_free(&self) -> bool {
        self.exclusive.is_empty() && self.shared.is_empty()
    }
}

#[derive(Default)]
struct Locks {
    locks: HashMap<LockKey, LockState>,
    /// Per session, the transaction-level locks to release when its
    /// transaction ends: key and whether exclusive.
    transaction_locks: HashMap<String, Vec<(LockKey, bool)>>,
}

/// The node's advisory locks.
#[derive(Default)]
pub(crate) struct AdvisoryLocks {
    state: parking_lot::Mutex<Locks>,
    released: parking_lot::Condvar,
}

impl AdvisoryLocks {
    /// Takes the lock for `session` if no other session holds it in a
    /// conflicting mode; `transaction` locks go when its transaction ends.
    pub(crate) fn try_lock(
        &self,
        session: &str,
        key: LockKey,
        exclusive: bool,
        transaction: bool,
    ) -> bool {
        let mut state = self.state.lock();
        Self::try_lock_in(&mut state, session, key, exclusive, transaction)
    }

    fn try_lock_in(
        state: &mut Locks,
        session: &str,
        key: LockKey,
        exclusive: bool,
        transaction: bool,
    ) -> bool {
        let lock = state.locks.entry(key).or_default();
        let others = |holders: &HashMap<String, u32>| holders.keys().any(|s| s != session);
        let free = if exclusive {
            !others(&lock.exclusive) && !others(&lock.shared)
        } else {
            !others(&lock.exclusive)
        };
        if !free {
            return false;
        }
        let holders = if exclusive {
            &mut lock.exclusive
        } else {
            &mut lock.shared
        };
        *holders.entry(session.to_string()).or_default() += 1;
        if transaction {
            state
                .transaction_locks
                .entry(session.to_string())
                .or_default()
                .push((key, exclusive));
        }
        true
    }

    /// Waits for the lock, until `deadline` if one is given; `false` when it
    /// passes first.
    pub(crate) fn lock(
        &self,
        session: &str,
        key: LockKey,
        exclusive: bool,
        transaction: bool,
        deadline: Option<Instant>,
    ) -> bool {
        let mut state = self.state.lock();
        loop {
            if Self::try_lock_in(&mut state, session, key, exclusive, transaction) {
                return true;
            }
            let wait = match deadline {
                Some(deadline) => match deadline.checked_duration_since(Instant::now()) {
                    Some(left) => left.min(Duration::from_millis(100)),
                    None => return false,
                },
                None => Duration::from_millis(100),
            };
            self.released.wait_for(&mut state, wait);
        }
    }

    /// Releases one hold of a session-level lock; `false` when the session
    /// holds none.
    pub(crate) fn unlock(&self, session: &str, key: LockKey, exclusive: bool) -> bool {
        let mut state = self.state.lock();
        let Some(lock) = state.locks.get_mut(&key) else {
            return false;
        };
        let holders = if exclusive {
            &mut lock.exclusive
        } else {
            &mut lock.shared
        };
        let Some(count) = holders.get_mut(session) else {
            return false;
        };
        *count -= 1;
        if *count == 0 {
            holders.remove(session);
        }
        if lock.is_free() {
            state.locks.remove(&key);
        }
        self.released.notify_all();
        true
    }

    /// Releases every lock the session holds (`pg_advisory_unlock_all`, and
    /// when it ends).
    pub(crate) fn unlock_all(&self, session: &str) {
        let mut state = self.state.lock();
        state.locks.retain(|_, lock| {
            lock.exclusive.remove(session);
            lock.shared.remove(session);
            !lock.is_free()
        });
        state.transaction_locks.remove(session);
        self.released.notify_all();
    }

    /// Releases the locks the session's transaction took.
    pub(crate) fn end_transaction(&self, session: &str) {
        let mut state = self.state.lock();
        let Some(held) = state.transaction_locks.remove(session) else {
            return;
        };
        for (key, exclusive) in held {
            if let Some(lock) = state.locks.get_mut(&key) {
                let holders = if exclusive {
                    &mut lock.exclusive
                } else {
                    &mut lock.shared
                };
                if let Some(count) = holders.get_mut(session) {
                    *count -= 1;
                    if *count == 0 {
                        holders.remove(session);
                    }
                }
                if lock.is_free() {
                    state.locks.remove(&key);
                }
            }
        }
        self.released.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locks_conflict_by_mode_and_count_their_holds() {
        let locks = AdvisoryLocks::default();
        let key = (false, 1);
        assert!(locks.try_lock("a", key, true, false));
        assert!(locks.try_lock("a", key, true, false));
        assert!(!locks.try_lock("b", key, false, false));
        assert!(locks.unlock("a", key, true));
        assert!(!locks.try_lock("b", key, true, false));
        assert!(locks.unlock("a", key, true));
        assert!(!locks.unlock("a", key, true));
        // Shared holders exclude only exclusive ones.
        assert!(locks.try_lock("a", key, false, false));
        assert!(locks.try_lock("b", key, false, false));
        assert!(!locks.try_lock("c", key, true, false));
        locks.unlock_all("a");
        locks.unlock_all("b");
        assert!(locks.try_lock("c", key, true, true));
        locks.end_transaction("c");
        assert!(locks.try_lock("a", key, true, false));
    }

    #[test]
    fn waiting_gives_up_at_the_deadline() {
        let locks = AdvisoryLocks::default();
        assert!(locks.try_lock("a", (true, 7), true, false));
        let deadline = Instant::now() + Duration::from_millis(20);
        assert!(!locks.lock("b", (true, 7), true, false, Some(deadline)));
    }
}
