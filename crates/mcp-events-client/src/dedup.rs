//! Bounded LRU set for `eventId` / `webhook-id` deduplication.

use std::collections::{HashSet, VecDeque};

#[derive(Debug)]
pub struct LruSet {
    cap: usize,
    set: HashSet<String>,
    order: VecDeque<String>,
}

impl LruSet {
    pub fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            set: HashSet::new(),
            order: VecDeque::new(),
        }
    }

    /// Returns `true` if `key` was already present (a duplicate). Present keys
    /// are promoted to most-recently-used; new keys evict the oldest entry
    /// once the capacity is reached.
    pub fn check_and_insert(&mut self, key: &str) -> bool {
        if self.set.contains(key) {
            if let Some(pos) = self.order.iter().position(|k| k == key) {
                if let Some(k) = self.order.remove(pos) {
                    self.order.push_back(k);
                }
            }
            return true;
        }
        if self.set.len() >= self.cap {
            if let Some(oldest) = self.order.pop_front() {
                self.set.remove(&oldest);
            }
        }
        self.set.insert(key.to_owned());
        self.order.push_back(key.to_owned());
        false
    }

    pub fn len(&self) -> usize {
        self.set.len()
    }

    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_duplicates() {
        let mut s = LruSet::new(8);
        assert!(!s.check_and_insert("a"));
        assert!(s.check_and_insert("a"));
        assert!(!s.check_and_insert("b"));
    }

    #[test]
    fn evicts_oldest_at_capacity() {
        let mut s = LruSet::new(2);
        s.check_and_insert("a");
        s.check_and_insert("b");
        s.check_and_insert("c"); // evicts "a"
        assert_eq!(s.len(), 2);
        assert!(!s.check_and_insert("a"), "a should have been evicted");
    }

    #[test]
    fn duplicate_access_promotes() {
        let mut s = LruSet::new(2);
        s.check_and_insert("a");
        s.check_and_insert("b");
        assert!(s.check_and_insert("a")); // promote a; b is now oldest
        s.check_and_insert("c"); // evicts "b"
        assert!(s.check_and_insert("a"), "a should have survived");
        assert!(!s.check_and_insert("b"), "b should have been evicted");
    }
}
