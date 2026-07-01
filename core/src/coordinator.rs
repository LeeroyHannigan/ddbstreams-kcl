//! Lease coordinator (pure, offline-tested): the "brain" that turns raw lease
//! rows into take decisions.
//!
//! Expiry follows KCL `DynamoDBLeaseTaker`: each lease has a
//! `lastCounterIncrementNanos`; a lease owned by another worker is expired once
//! `now - lastCounterIncrement > leaseDuration` (the owner stopped
//! heartbeating). We track counter-change times per lease and take a monotonic
//! clock (`now_ms`) as input so the logic stays pure and deterministically
//! testable. Take decisions come from [`crate::taker::compute_leases_to_take`].
//! See core/REFERENCES.md.

use crate::taker::{compute_leases_to_take, LeaseSnapshot};
use std::collections::HashMap;

/// A raw lease-table row as read from a scan.
#[derive(Clone, Debug)]
pub struct RawLease {
    pub lease_key: String,
    pub owner: Option<String>,
    pub lease_counter: u64,
    pub completed: bool,
}

#[derive(Clone, Copy, Debug)]
struct Seen {
    counter: u64,
    /// Monotonic time (ms) at which we last observed this lease's counter change.
    last_change_ms: u64,
}

/// Stateful, single-worker view of the lease table used to decide takes.
pub struct LeaseCoordinator {
    me: String,
    max_take: usize,
    lease_duration_ms: u64,
    seen: HashMap<String, Seen>,
}

impl LeaseCoordinator {
    pub fn new(me: impl Into<String>, max_take: usize, lease_duration_ms: u64) -> Self {
        Self {
            me: me.into(),
            max_take,
            lease_duration_ms,
            seen: HashMap::new(),
        }
    }

    /// Process one scan of the lease table at monotonic time `now_ms`. Updates
    /// counter-freshness tracking and returns the lease keys this worker should
    /// attempt to take.
    pub fn tick(&mut self, rows: &[RawLease], now_ms: u64) -> Vec<String> {
        // Derive expiry from the PRE-update freshness state.
        let snapshot: Vec<LeaseSnapshot> = rows
            .iter()
            .map(|r| LeaseSnapshot {
                lease_key: r.lease_key.clone(),
                owner: r.owner.clone(),
                expired: self.is_expired(r, now_ms),
                completed: r.completed,
            })
            .collect();

        // Update freshness: a counter that advanced this tick resets the clock.
        for r in rows {
            match self.seen.get_mut(&r.lease_key) {
                Some(s) => {
                    if r.lease_counter != s.counter {
                        s.counter = r.lease_counter;
                        s.last_change_ms = now_ms;
                    }
                }
                None => {
                    self.seen.insert(
                        r.lease_key.clone(),
                        Seen { counter: r.lease_counter, last_change_ms: now_ms },
                    );
                }
            }
        }
        // Forget leases that disappeared (e.g. deleted after completion).
        let present: std::collections::HashSet<&str> =
            rows.iter().map(|r| r.lease_key.as_str()).collect();
        self.seen.retain(|k, _| present.contains(k.as_str()));

        compute_leases_to_take(&snapshot, &self.me, self.max_take)
    }

    fn is_expired(&self, r: &RawLease, now_ms: u64) -> bool {
        match &r.owner {
            None => false,                      // unowned → available, not "expired"
            Some(o) if o == &self.me => false,  // mine
            Some(_) => match self.seen.get(&r.lease_key) {
                // Counter advanced since we last saw it → owner is alive.
                Some(s) if r.lease_counter != s.counter => false,
                // Counter stalled → expired once it has been stale beyond the
                // lease duration.
                Some(s) => now_ms.saturating_sub(s.last_change_ms) > self.lease_duration_ms,
                // First sighting → give it a full duration to prove liveness.
                None => false,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DUR: u64 = 1000;

    fn row(key: &str, owner: Option<&str>, counter: u64, completed: bool) -> RawLease {
        RawLease { lease_key: key.into(), owner: owner.map(|o| o.into()), lease_counter: counter, completed }
    }

    // Balanced by count (w1: a,b; w2: c,d) so load-balancing never triggers —
    // isolates expiry behavior.
    fn balanced() -> Vec<RawLease> {
        vec![
            row("a", Some("w1"), 1, false),
            row("b", Some("w1"), 1, false),
            row("c", Some("w2"), 5, false),
            row("d", Some("w2"), 5, false),
        ]
    }

    #[test]
    fn fresh_worker_not_expired_within_duration() {
        let mut c = LeaseCoordinator::new("w1", 10, DUR);
        let rows = balanced();
        assert!(c.tick(&rows, 0).is_empty()); // first sighting
        assert!(c.tick(&rows, DUR / 2).is_empty()); // stalled but within duration
    }

    #[test]
    fn stalled_beyond_duration_expires_and_is_taken() {
        let mut c = LeaseCoordinator::new("w1", 10, DUR);
        let rows = balanced();
        assert!(c.tick(&rows, 0).is_empty());
        // Past the lease duration with no counter change → w2 dead → take c,d.
        let mut take = c.tick(&rows, DUR + 1);
        take.sort();
        assert_eq!(take, vec!["c", "d"]);
    }

    #[test]
    fn advancing_counter_resets_liveness() {
        let mut c = LeaseCoordinator::new("w1", 10, DUR);
        c.tick(&balanced(), 0);
        // Well past the duration, BUT w2's counters advanced this tick → alive.
        let advanced = vec![
            row("a", Some("w1"), 1, false),
            row("b", Some("w1"), 1, false),
            row("c", Some("w2"), 6, false),
            row("d", Some("w2"), 7, false),
        ];
        assert!(c.tick(&advanced, DUR * 5).is_empty());
        // And it stays alive for another full duration from the change.
        assert!(c.tick(&advanced, DUR * 5 + DUR / 2).is_empty());
    }

    #[test]
    fn unowned_taken_immediately() {
        let mut c = LeaseCoordinator::new("w1", 10, DUR);
        let rows = vec![row("a", None, 0, false), row("b", None, 0, false)];
        let mut take = c.tick(&rows, 0);
        take.sort();
        assert_eq!(take, vec!["a", "b"]);
    }

    #[test]
    fn forgets_disappeared_leases() {
        let mut c = LeaseCoordinator::new("w1", 10, DUR);
        c.tick(&[row("x", Some("w2"), 5, false)], 0);
        c.tick(&[], DUR + 1); // x gone → freshness forgotten
        // x reappears far in the future → fresh first-sighting → not expired.
        assert!(c.tick(&[row("x", Some("w2"), 5, false)], DUR * 100).is_empty());
    }
}
