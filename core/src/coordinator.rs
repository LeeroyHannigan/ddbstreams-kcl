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

/// Reserved lease-key prefix for per-worker heartbeat rows. A heartbeat row
/// (`__hb__:<worker>`, `owner = <worker>`) is the worker's single liveness
/// signal: it is bumped on the heartbeat cadence regardless of how many shards
/// the worker holds, so idle shards cost no lease-table writes. The coordinator
/// judges an owner alive from ITS heartbeat row, not from each shard's counter.
pub const HEARTBEAT_KEY_PREFIX: &str = "__hb__:";

/// Build the heartbeat lease key for `worker`.
pub fn heartbeat_key(worker: &str) -> String {
    format!("{HEARTBEAT_KEY_PREFIX}{worker}")
}

/// True if `key` is a per-worker heartbeat row (not a shard lease).
pub fn is_heartbeat_key(key: &str) -> bool {
    key.starts_with(HEARTBEAT_KEY_PREFIX)
}

/// A raw lease-table row as read from a scan.
#[derive(Clone, Debug)]
pub struct RawLease {
    pub lease_key: String,
    pub owner: Option<String>,
    pub lease_counter: u64,
    pub completed: bool,
    /// Opaque resume checkpoint (`None` = start at `TRIM_HORIZON`). The
    /// coordinator ignores this; the fleet uses it to resume a shard task from
    /// the last persisted position instead of re-reading from the beginning.
    pub checkpoint: Option<String>,
    /// Shard lineage (parent shard ids), published on the lease row by the
    /// shard-sync leader so that non-leader workers can reconstruct the shard
    /// graph — and enforce parent-before-child — WITHOUT calling DescribeStream
    /// themselves. The coordinator's take logic ignores this; see
    /// [`crate::leader`]. Empty for the leader sentinel and for root shards.
    pub parents: Vec<String>,
}

#[derive(Clone, Copy, Debug)]
struct Seen {
    counter: u64,
    /// Monotonic time (ms) at which we last observed this WORKER's heartbeat
    /// counter change (or first saw the worker, if it has no heartbeat yet).
    last_change_ms: u64,
}

/// Stateful, single-worker view of the lease table used to decide takes.
///
/// Liveness is tracked **per worker** (from `__hb__:<worker>` heartbeat rows),
/// not per shard lease: an owner is alive while its heartbeat counter keeps
/// advancing, so a live worker keeps ALL its shards (even idle ones) and a dead
/// worker's entire lease set becomes takeable at once.
pub struct LeaseCoordinator {
    me: String,
    max_take: usize,
    lease_duration_ms: u64,
    /// worker -> heartbeat freshness.
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
    /// per-worker heartbeat freshness and returns the shard lease keys this
    /// worker should attempt to take. Heartbeat rows (`__hb__:*`) drive
    /// liveness but are never themselves returned as takeable.
    pub fn tick(&mut self, rows: &[RawLease], now_ms: u64) -> Vec<String> {
        // Update per-worker freshness FIRST so this tick's heartbeats count.
        // A heartbeat row whose counter advanced resets that worker's clock.
        for r in rows.iter().filter(|r| is_heartbeat_key(&r.lease_key)) {
            let Some(worker) = r.owner.as_deref() else {
                continue;
            };
            match self.seen.get_mut(worker) {
                Some(s) => {
                    if r.lease_counter != s.counter {
                        s.counter = r.lease_counter;
                        s.last_change_ms = now_ms;
                    }
                }
                None => {
                    self.seen.insert(
                        worker.to_string(),
                        Seen {
                            counter: r.lease_counter,
                            last_change_ms: now_ms,
                        },
                    );
                }
            }
        }
        // Seed a grace clock for any owner that holds a shard lease but has no
        // heartbeat row yet — so a worker that never heartbeats is still
        // reclaimed after one lease duration (rather than held forever).
        for r in rows.iter().filter(|r| !is_heartbeat_key(&r.lease_key)) {
            if let Some(o) = r.owner.as_deref() {
                if o != self.me {
                    self.seen.entry(o.to_string()).or_insert(Seen {
                        counter: 0,
                        last_change_ms: now_ms,
                    });
                }
            }
        }
        // Forget workers no longer referenced this tick (heartbeat row gone AND
        // no owned lease) so a reappearance gets a fresh grace window.
        let live_workers: std::collections::HashSet<&str> =
            rows.iter().filter_map(|r| r.owner.as_deref()).collect();
        self.seen.retain(|w, _| live_workers.contains(w.as_str()));

        // Expiry of each shard lease is keyed on its OWNER's heartbeat freshness.
        let snapshot: Vec<LeaseSnapshot> = rows
            .iter()
            .filter(|r| !is_heartbeat_key(&r.lease_key))
            .map(|r| LeaseSnapshot {
                lease_key: r.lease_key.clone(),
                owner: r.owner.clone(),
                expired: self.is_owner_expired(r.owner.as_deref(), now_ms),
                completed: r.completed,
            })
            .collect();

        compute_leases_to_take(&snapshot, &self.me, self.max_take)
    }

    /// Is the shard's owner dead? Keyed on the OWNER's heartbeat freshness.
    fn is_owner_expired(&self, owner: Option<&str>, now_ms: u64) -> bool {
        match owner {
            None => false,                    // unowned → available, not "expired"
            Some(o) if o == self.me => false, // mine
            Some(o) => match self.seen.get(o) {
                // Heartbeat (or grace clock) stale beyond the lease duration → dead.
                Some(s) => now_ms.saturating_sub(s.last_change_ms) > self.lease_duration_ms,
                // Owner not yet seen → grace (seeded this tick).
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
        RawLease {
            lease_key: key.into(),
            owner: owner.map(|o| o.into()),
            lease_counter: counter,
            completed,
            checkpoint: None,
            parents: vec![],
        }
    }

    fn hb(worker: &str, counter: u64) -> RawLease {
        row(&heartbeat_key(worker), Some(worker), counter, false)
    }

    // w1 owns a,b; w2 owns c,d (balanced by count so load-balancing never
    // triggers — isolates liveness behavior). Both workers heartbeat.
    fn balanced_with_hb() -> Vec<RawLease> {
        vec![
            row("a", Some("w1"), 1, false),
            row("b", Some("w1"), 1, false),
            row("c", Some("w2"), 5, false),
            row("d", Some("w2"), 5, false),
            hb("w1", 100),
            hb("w2", 200),
        ]
    }

    #[test]
    fn live_worker_keeps_all_shards_even_when_shard_counters_never_move() {
        // w2's SHARD counters (c,d) never change, but its heartbeat advances →
        // w2 is alive → w1 takes nothing. This is the core semantic shift: a
        // slow/idle shard no longer looks dead.
        let mut c = LeaseCoordinator::new("w1", 10, DUR);
        let mut rows = balanced_with_hb();
        assert!(c.tick(&rows, 0).is_empty());
        // Far past the duration, shard counters unchanged, but w2 heartbeats.
        rows[5] = hb("w2", 201); // w2 heartbeat advances
        assert!(
            c.tick(&rows, DUR * 5).is_empty(),
            "live (heartbeating) worker keeps its shards regardless of shard-counter staleness"
        );
    }

    #[test]
    fn dead_worker_frees_all_its_shards_at_once() {
        // w2 stops heartbeating (and stops everything). After the lease
        // duration, ALL of w2's shards become takeable together.
        let mut c = LeaseCoordinator::new("w1", 10, DUR);
        let rows = balanced_with_hb();
        assert!(c.tick(&rows, 0).is_empty());
        // w2 heartbeat frozen at 200; past duration → w2 dead → take c AND d.
        let mut take = c.tick(&rows, DUR + 1);
        take.sort();
        assert_eq!(take, vec!["c", "d"]);
    }

    #[test]
    fn worker_within_duration_not_expired() {
        let mut c = LeaseCoordinator::new("w1", 10, DUR);
        let rows = balanced_with_hb();
        assert!(c.tick(&rows, 0).is_empty());
        assert!(c.tick(&rows, DUR / 2).is_empty()); // heartbeat stale but within duration
    }

    #[test]
    fn advancing_heartbeat_resets_liveness() {
        let mut c = LeaseCoordinator::new("w1", 10, DUR);
        c.tick(&balanced_with_hb(), 0);
        let mut advanced = balanced_with_hb();
        advanced[5] = hb("w2", 201); // only the heartbeat moves; shard counters static
        assert!(c.tick(&advanced, DUR * 5).is_empty());
        assert!(c.tick(&advanced, DUR * 5 + DUR / 2).is_empty());
    }

    #[test]
    fn owner_with_no_heartbeat_row_is_reclaimed_after_grace() {
        // A worker holding shards but never writing a heartbeat row must not
        // hold them forever: seeded on first sighting, expired after one
        // duration. Balanced ownership (w1:a,b + hb; w2:c,d, NO hb) so only
        // expiry — not load-balancing — can move c,d.
        let mut c = LeaseCoordinator::new("w1", 10, DUR);
        let rows = vec![
            row("a", Some("w1"), 1, false),
            row("b", Some("w1"), 1, false),
            row("c", Some("w2"), 5, false),
            row("d", Some("w2"), 5, false),
            hb("w1", 100),
            // no heartbeat row for w2
        ];
        assert!(c.tick(&rows, 0).is_empty()); // first sighting → grace, balanced
        let mut take = c.tick(&rows, DUR + 1);
        take.sort();
        assert_eq!(
            take,
            vec!["c", "d"],
            "no-heartbeat owner reclaimed after grace"
        );
    }

    #[test]
    fn heartbeat_rows_are_never_taken() {
        // An unowned/other heartbeat row must never be returned as a takeable
        // shard, even though it is "owned by another / expired"-shaped.
        let mut c = LeaseCoordinator::new("w1", 10, DUR);
        let rows = vec![row("a", Some("w1"), 1, false), hb("w2", 5)];
        assert!(c.tick(&rows, 0).is_empty());
        assert!(
            c.tick(&rows, DUR * 10).is_empty(),
            "heartbeat row is not a shard"
        );
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
    fn forgets_disappeared_workers() {
        let mut c = LeaseCoordinator::new("w1", 10, DUR);
        c.tick(&[row("x", Some("w2"), 5, false), hb("w2", 1)], 0);
        c.tick(&[], DUR + 1); // w2 gone → freshness forgotten
                              // x reappears far in the future → fresh grace → not expired.
        assert!(c
            .tick(&[row("x", Some("w2"), 5, false), hb("w2", 1)], DUR * 100)
            .is_empty());
    }
}
