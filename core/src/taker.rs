//! Lease-taker balancing logic (pure, offline-tested).
//!
//! Decides which leases a worker should take to converge on an even
//! distribution: available (unowned/expired) leases first, then — if still below
//! the fair-share target — steal from the most-loaded worker. Mirrors KCL
//! `DynamoDBLeaseTaker.computeLeasesToTake` (Apache-2.0). See core/REFERENCES.md.
//!
//! Expiry (owner present but `leaseCounter` not advancing) is detected by the
//! coordinator over time — see [`crate::Lease::is_takeable`]. Here it is an input
//! so the balancing decision stays pure and deterministic.

use std::collections::HashMap;

/// A worker's view of one lease-table row for balancing decisions.
#[derive(Clone, Debug)]
pub struct LeaseSnapshot {
    pub lease_key: String,
    pub owner: Option<String>,
    /// Owned but the owner stopped heartbeating (counter stalled).
    pub expired: bool,
    /// Shard fully processed (SHARD_END) — never taken for processing.
    pub completed: bool,
}

/// Compute the leases `me` should attempt to take, capped at `max_to_take`.
///
/// Deterministic: candidates are considered in `lease_key` order so behavior is
/// reproducible (and testable). Returns lease keys to claim via a conditional
/// write; a claim may still lose the optimistic-lock race, which is expected.
pub fn compute_leases_to_take(
    leases: &[LeaseSnapshot],
    me: &str,
    max_to_take: usize,
) -> Vec<String> {
    // Only leases still needing processing participate.
    let active: Vec<&LeaseSnapshot> = leases.iter().filter(|l| !l.completed).collect();
    if active.is_empty() || max_to_take == 0 {
        return Vec::new();
    }

    // Live worker set: owners of non-expired owned leases, plus me.
    let mut workers: std::collections::HashSet<&str> = active
        .iter()
        .filter(|l| !l.expired)
        .filter_map(|l| l.owner.as_deref())
        .collect();
    workers.insert(me);

    let total = active.len();
    let target = total.div_ceil(workers.len()); // fair share (ceil)

    let my_current = active
        .iter()
        .filter(|l| !l.expired && l.owner.as_deref() == Some(me))
        .count();
    if my_current >= target {
        return Vec::new();
    }
    let mut need = target - my_current;
    let mut take: Vec<String> = Vec::new();

    // 1) Available leases: unowned or expired (and not mine).
    let mut available: Vec<&&LeaseSnapshot> = active
        .iter()
        .filter(|l| l.owner.as_deref() != Some(me))
        .filter(|l| l.owner.is_none() || l.expired)
        .collect();
    available.sort_by(|a, b| a.lease_key.cmp(&b.lease_key));
    for l in available {
        if need == 0 || take.len() == max_to_take {
            break;
        }
        take.push(l.lease_key.clone());
        need -= 1;
    }

    // 2) Still short → steal from the most-loaded OTHER worker.
    if need > 0 && take.len() < max_to_take {
        let mut counts: HashMap<&str, usize> = HashMap::new();
        for l in active.iter().filter(|l| !l.expired) {
            if let Some(o) = l.owner.as_deref() {
                if o != me {
                    *counts.entry(o).or_default() += 1;
                }
            }
        }
        // Most-loaded worker above target is the steal victim.
        if let Some((victim, &count)) = counts.iter().max_by_key(|(_, c)| **c) {
            if count > target {
                let stealable = count - target; // don't push the victim below target
                let mut victim_leases: Vec<&&LeaseSnapshot> = active
                    .iter()
                    .filter(|l| !l.expired && l.owner.as_deref() == Some(*victim))
                    .collect();
                victim_leases.sort_by(|a, b| a.lease_key.cmp(&b.lease_key));
                for l in victim_leases.into_iter().take(stealable) {
                    if need == 0 || take.len() == max_to_take {
                        break;
                    }
                    take.push(l.lease_key.clone());
                    need -= 1;
                }
            }
        }
    }

    take
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(key: &str, owner: Option<&str>, expired: bool, completed: bool) -> LeaseSnapshot {
        LeaseSnapshot {
            lease_key: key.into(),
            owner: owner.map(|o| o.into()),
            expired,
            completed,
        }
    }

    #[test]
    fn takes_unowned_up_to_fair_share() {
        // 2 unowned leases, only worker w1 → target = 2 → take both.
        let leases = vec![snap("a", None, false, false), snap("b", None, false, false)];
        let take = compute_leases_to_take(&leases, "w1", 10);
        assert_eq!(take, vec!["a", "b"]);
    }

    #[test]
    fn takes_expired_lease_from_dead_worker() {
        let leases = vec![
            snap("a", Some("w1"), false, false),
            snap("b", Some("w2"), true, false), // w2 dead (expired)
        ];
        // total 2, workers {w1} (w2 expired) → target 2, w1 has 1 → take b.
        let take = compute_leases_to_take(&leases, "w1", 10);
        assert_eq!(take, vec!["b"]);
    }

    #[test]
    fn steals_from_most_loaded_when_no_available() {
        // w2 holds 3 fresh, w1 holds 1 → total 4, workers {w1,w2}, target 2.
        // w1 needs 1, nothing available → steal 1 from w2 (over target by 1).
        let leases = vec![
            snap("a", Some("w1"), false, false),
            snap("b", Some("w2"), false, false),
            snap("c", Some("w2"), false, false),
            snap("d", Some("w2"), false, false),
        ];
        let take = compute_leases_to_take(&leases, "w1", 10);
        assert_eq!(take.len(), 1);
        assert!(["b", "c", "d"].contains(&take[0].as_str()));
    }

    #[test]
    fn balanced_takes_nothing() {
        let leases = vec![
            snap("a", Some("w1"), false, false),
            snap("b", Some("w1"), false, false),
            snap("c", Some("w2"), false, false),
            snap("d", Some("w2"), false, false),
        ];
        assert!(compute_leases_to_take(&leases, "w1", 10).is_empty());
    }

    #[test]
    fn respects_max_cap() {
        let leases = vec![
            snap("a", None, false, false),
            snap("b", None, false, false),
            snap("c", None, false, false),
        ];
        let take = compute_leases_to_take(&leases, "w1", 2);
        assert_eq!(take.len(), 2);
    }

    #[test]
    fn skips_completed_leases() {
        let leases = vec![
            snap("a", None, false, true), // completed → ignored
            snap("b", None, false, false),
        ];
        let take = compute_leases_to_take(&leases, "w1", 10);
        assert_eq!(take, vec!["b"]);
    }

    #[test]
    fn does_not_steal_from_worker_at_target() {
        // 3 leases, workers {w1,w2}, target = ceil(3/2) = 2. w2 holds 2 (== target),
        // w1 holds 1 and needs 1, but stealing would push w2 below target → no steal.
        let leases = vec![
            snap("a", Some("w1"), false, false),
            snap("b", Some("w2"), false, false),
            snap("c", Some("w2"), false, false),
        ];
        assert!(compute_leases_to_take(&leases, "w1", 10).is_empty());
    }
}
