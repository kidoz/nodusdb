//! `random()` and `setseed()` with PostgreSQL's generator (xoroshiro128**,
//! seeded through splitmix64), so a session that sets a seed draws the same
//! numbers PostgreSQL would. Each session has its own generator, seeded from
//! entropy until `setseed` sets it.

use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Clone, Copy)]
pub(crate) struct State {
    s0: u64,
    s1: u64,
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut val = *state;
    val = (val ^ (val >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    val = (val ^ (val >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    val ^ (val >> 31)
}

impl State {
    pub(crate) fn seeded(mut seed: u64) -> State {
        let mut state = State {
            s0: splitmix64(&mut seed),
            s1: splitmix64(&mut seed),
        };
        // An all-zero state would only ever give zeroes.
        if state.s0 == 0 && state.s1 == 0 {
            state.s0 = 0x5851_F42D_4C95_7F2D;
            state.s1 = 0x1405_7B7E_F767_814F;
        }
        state
    }

    fn next(&mut self) -> u64 {
        let s0 = self.s0;
        let sx = self.s1 ^ s0;
        let val = s0.wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        self.s0 = s0.rotate_left(24) ^ sx ^ (sx << 16);
        self.s1 = sx.rotate_left(37);
        val
    }

    /// A float in `[0, 1)` from 52 random bits.
    pub(crate) fn double(&mut self) -> f64 {
        (self.next() >> 12) as f64 * 2f64.powi(-52)
    }

    /// An integer in `[0, range]`, by rejecting draws above it.
    fn up_to(&mut self, range: u64) -> u64 {
        if range == 0 {
            return 0;
        }
        let shift = range.leading_zeros();
        loop {
            let val = self.next() >> shift;
            if val <= range {
                return val;
            }
        }
    }
}

/// The generators of the sessions that drew from one, by session id.
static STATES: Mutex<Option<HashMap<String, State>>> = Mutex::new(None);

fn session_id() -> String {
    crate::session_env::with(|env| env.map(|e| e.session_id.clone())).unwrap_or_default()
}

fn with_state<R>(f: impl FnOnce(&mut State) -> R) -> R {
    let id = session_id();
    let mut states = STATES.lock().unwrap_or_else(|e| e.into_inner());
    let state = states
        .get_or_insert_with(HashMap::new)
        .entry(id)
        .or_insert_with(|| {
            let entropy = u64::from_le_bytes(
                uuid::Uuid::new_v4().as_bytes()[..8]
                    .try_into()
                    .unwrap_or([0; 8]),
            );
            State::seeded(entropy)
        });
    f(state)
}

/// `random()`: a float in `[0, 1)`.
pub(crate) fn uniform() -> f64 {
    with_state(State::double)
}

/// `random(lo, hi)`: an integer in `[lo, hi]`.
pub(crate) fn int_range(lo: i64, hi: i64) -> i64 {
    with_state(|s| (lo as u64).wrapping_add(s.up_to((hi as u64).wrapping_sub(lo as u64))) as i64)
}

/// A standard normal value, by the Box-Muller transform.
pub(crate) fn normal() -> f64 {
    with_state(|s| {
        let u1 = 1.0 - s.double();
        let u2 = 1.0 - s.double();
        (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).sin()
    })
}

/// `setseed(seed)` for `seed` in `[-1, 1]`.
pub(crate) fn set_seed(seed: f64) {
    let seed = (((1u64 << 52) - 1) as f64 * seed) as i64;
    let id = session_id();
    let mut states = STATES.lock().unwrap_or_else(|e| e.into_inner());
    states
        .get_or_insert_with(HashMap::new)
        .insert(id, State::seeded(seed as u64));
}

/// Drops a finished session's generator.
pub(crate) fn forget(session_id: &str) {
    let mut states = STATES.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(states) = states.as_mut() {
        states.remove(session_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeded_draws_are_postgresql_s() {
        // PostgreSQL: SELECT setseed(0.5); SELECT random(), random(1, 100);
        let mut state = State::seeded((((1u64 << 52) - 1) as f64 * 0.5) as i64 as u64);
        assert_eq!(state.double(), 0.9851677175347999);
        assert_eq!(state.double(), 0.825301858027981);
        assert_eq!(1 + state.up_to(99) as i64, 17);
    }
}
