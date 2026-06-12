//! Deterministic synthetic feed against a virtual `orders` table.
//!
//! `LANES` orders are in flight concurrently; each tick advances one lane
//! (round-robin) through the lifecycle insert -> update -> update -> delete,
//! after which the lane starts the next order id. The data sequence is fully
//! deterministic; only `timestamp` varies between runs.

use std::time::Duration;

use tokio::sync::mpsc;

use crate::{ChangeType, FeedEvent};

const LANES: u64 = 3;
const PHASES: u8 = 4; // insert, update, update, delete
const CUSTOMERS: [&str; 5] = ["alice", "bob", "carol", "dave", "erin"];

/// Projected row for `order` at revision `rev` (0 = as inserted).
fn order_row(order: u64, rev: u8) -> serde_json::Value {
    let customer = CUSTOMERS[((order.saturating_sub(1)) % CUSTOMERS.len() as u64) as usize];
    let base = 1200.0 + (order % 5) as f64 * 150.0;
    let (status, total) = match rev {
        0 => ("open", base),
        1 => ("paid", base + 100.0),
        _ => ("shipped", base + 250.0),
    };
    serde_json::json!({ "id": order, "customer": customer, "status": status, "total": total })
}

fn mock_event(query_id: &str, order: u64, phase: u8) -> FeedEvent {
    let (change, before, after) = match phase {
        0 => (ChangeType::Added, None, Some(order_row(order, 0))),
        1 => (
            ChangeType::Updated,
            Some(order_row(order, 0)),
            Some(order_row(order, 1)),
        ),
        2 => (
            ChangeType::Updated,
            Some(order_row(order, 1)),
            Some(order_row(order, 2)),
        ),
        // Delete carries the tombstone revision (rev 3) as its upstream id.
        _ => (ChangeType::Deleted, Some(order_row(order, 2)), None),
    };
    FeedEvent {
        query_id: query_id.to_string(),
        change,
        before,
        after,
        timestamp: Some(chrono::Utc::now()),
        upstream_id: Some(format!("order-{order}-rev-{phase}")),
    }
}

/// Emits the deterministic synthetic orders scenario onto `tx`, one event per
/// `interval` tick (first event immediately). Returns `Ok(())` once the
/// receiver side of `tx` is dropped.
pub async fn run_mock_feed(
    tx: mpsc::Sender<FeedEvent>,
    query_id: String,
    interval: Duration,
) -> anyhow::Result<()> {
    // tokio::time::interval panics on a zero period.
    let period = interval.max(Duration::from_micros(1));
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut orders: Vec<u64> = (1..=LANES).collect();
    let mut phases: Vec<u8> = vec![0; LANES as usize];
    let mut next_order = LANES + 1;
    let mut tick: u64 = 0;
    loop {
        ticker.tick().await;
        let lane = (tick % LANES) as usize;
        let ev = mock_event(&query_id, orders[lane], phases[lane]);
        if phases[lane] + 1 >= PHASES {
            orders[lane] = next_order;
            next_order += 1;
            phases[lane] = 0;
        } else {
            phases[lane] += 1;
        }
        tick += 1;
        if tx.send(ev).await.is_err() {
            tracing::debug!("mock feed receiver dropped; stopping");
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    async fn collect_mock(n: usize) -> Vec<FeedEvent> {
        let (tx, mut rx) = mpsc::channel(n.max(1));
        let handle = tokio::spawn(run_mock_feed(
            tx,
            "high-value-orders".to_string(),
            Duration::from_millis(1),
        ));
        let mut out = Vec::with_capacity(n);
        while out.len() < n {
            let ev = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("mock feed stalled")
                .expect("mock feed channel closed early");
            out.push(ev);
        }
        drop(rx);
        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .unwrap()
            .unwrap();
        assert!(result.is_ok());
        out
    }

    fn order_of(ev: &FeedEvent) -> u64 {
        let id = ev.upstream_id.as_deref().unwrap();
        let rest = id.strip_prefix("order-").unwrap();
        rest.split('-').next().unwrap().parse().unwrap()
    }

    #[tokio::test]
    async fn mock_feed_cycles_insert_updates_delete_with_correct_before_after() {
        // 12 = full lifecycle for orders 1..3, then 3 inserts for orders 4..6.
        let events = collect_mock(15).await;

        for ev in &events {
            assert_eq!(ev.query_id, "high-value-orders");
            assert!(ev.timestamp.is_some());
        }

        // First three ticks: concurrent inserts for distinct orders 1, 2, 3.
        for (i, ev) in events[..3].iter().enumerate() {
            assert_eq!(ev.change, ChangeType::Added);
            assert_eq!(ev.before, None);
            let after = ev.after.as_ref().unwrap();
            assert_eq!(after["id"], (i + 1) as u64);
            assert_eq!(after["status"], "open");
            assert_eq!(
                ev.upstream_id.as_deref().unwrap(),
                format!("order-{}-rev-0", i + 1)
            );
        }

        let mut by_order: BTreeMap<u64, Vec<&FeedEvent>> = BTreeMap::new();
        for ev in &events {
            by_order.entry(order_of(ev)).or_default().push(ev);
        }
        assert_eq!(
            by_order.keys().copied().collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5, 6]
        );

        for (order, evs) in &by_order {
            let expected = [
                ChangeType::Added,
                ChangeType::Updated,
                ChangeType::Updated,
                ChangeType::Deleted,
            ];
            for (k, ev) in evs.iter().enumerate() {
                assert_eq!(ev.change, expected[k], "order {order} phase {k}");
                assert_eq!(
                    ev.upstream_id.as_deref().unwrap(),
                    format!("order-{order}-rev-{k}")
                );
            }
            // before/after chaining across the lifecycle.
            for pair in evs.windows(2) {
                assert_eq!(
                    pair[1].before, pair[0].after,
                    "order {order}: before must equal previous after"
                );
            }
            if evs.len() == 4 {
                let insert_after = evs[0].after.as_ref().unwrap();
                assert_eq!(insert_after["id"], *order);
                assert_ne!(evs[1].before, evs[1].after, "update must change the row");
                assert_ne!(evs[2].before, evs[2].after, "update must change the row");
                assert_eq!(evs[3].after, None);
                assert_eq!(evs[3].before.as_ref().unwrap()["status"], "shipped");
            }
        }

        // Lanes recycle: events 12..15 are inserts for the next order ids.
        for (i, ev) in events[12..15].iter().enumerate() {
            assert_eq!(ev.change, ChangeType::Added);
            assert_eq!(order_of(ev), (i + 4) as u64);
        }
    }

    #[tokio::test]
    async fn mock_feed_is_deterministic_modulo_timestamps() {
        let strip = |evs: Vec<FeedEvent>| {
            evs.into_iter()
                .map(|ev| (ev.change, ev.before, ev.after, ev.upstream_id))
                .collect::<Vec<_>>()
        };
        let a = strip(collect_mock(20).await);
        let b = strip(collect_mock(20).await);
        assert_eq!(a, b);
    }
}
