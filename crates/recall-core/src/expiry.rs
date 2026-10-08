use bytes::Bytes;
use std::collections::{HashMap, TryReserveError};

struct Timer {
    key: Bytes,
    deadline: i64,
}

/// Exactly one heap node and position entry per expiring key.
pub(crate) struct ExpiryIndex {
    heap: Vec<Timer>,
    positions: HashMap<Bytes, usize>,
}

impl ExpiryIndex {
    pub(crate) fn new(capacity: usize) -> Result<Self, TryReserveError> {
        let mut heap = Vec::new();
        let mut positions = HashMap::new();
        heap.try_reserve_exact(capacity)?;
        positions.try_reserve(capacity)?;
        Ok(Self { heap, positions })
    }

    pub(crate) fn len(&self) -> usize {
        self.heap.len()
    }

    pub(crate) fn set(&mut self, key: Bytes, deadline: i64) {
        if let Some(index) = self.positions.get(&key).copied() {
            self.heap[index].deadline = deadline;
            let index = self.up(index);
            self.down(index);
        } else {
            let index = self.heap.len();
            self.positions.insert(key.clone(), index);
            self.heap.push(Timer { key, deadline });
            self.up(index);
        }
    }

    pub(crate) fn remove(&mut self, key: &Bytes) {
        let Some(index) = self.positions.remove(key) else {
            return;
        };
        self.heap.swap_remove(index);
        if index < self.heap.len() {
            self.positions.insert(self.heap[index].key.clone(), index);
            let index = self.up(index);
            self.down(index);
        }
    }

    pub(crate) fn expired(&self, now_ms: i64) -> Option<Bytes> {
        self.heap
            .first()
            .filter(|timer| timer.deadline <= now_ms)
            .map(|timer| timer.key.clone())
    }

    fn swap(&mut self, left: usize, right: usize) {
        self.heap.swap(left, right);
        self.positions.insert(self.heap[left].key.clone(), left);
        self.positions.insert(self.heap[right].key.clone(), right);
    }

    fn up(&mut self, mut index: usize) -> usize {
        while index > 0 {
            let parent = (index - 1) / 2;
            if self.heap[parent].deadline <= self.heap[index].deadline {
                break;
            }
            self.swap(parent, index);
            index = parent;
        }
        index
    }

    fn down(&mut self, mut index: usize) {
        loop {
            let left = index * 2 + 1;
            if left >= self.heap.len() {
                break;
            }
            let right = left + 1;
            let child = if right < self.heap.len()
                && self.heap[right].deadline < self.heap[left].deadline
            {
                right
            } else {
                left
            };
            if self.heap[index].deadline <= self.heap[child].deadline {
                break;
            }
            self.swap(index, child);
            index = child;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_updates_reuse_one_timer() {
        let mut timers = ExpiryIndex::new(8).unwrap();
        let key = Bytes::from_static(b"key");
        for deadline in 0..10_000 {
            timers.set(key.clone(), deadline);
            assert_eq!(timers.len(), 1);
        }
        assert!(timers.expired(9998).is_none());
        assert_eq!(timers.expired(9999), Some(key.clone()));
        timers.remove(&key);
        assert_eq!(timers.len(), 0);
    }

    #[test]
    fn updates_and_arbitrary_removals_preserve_heap_order() {
        let mut timers = ExpiryIndex::new(100).unwrap();
        for i in 0..100 {
            timers.set(Bytes::from(i.to_string()), 100 - i);
        }
        for i in (0..100).step_by(3) {
            timers.remove(&Bytes::from(i.to_string()));
        }
        for i in (1..100).step_by(3) {
            timers.set(Bytes::from(i.to_string()), i);
        }
        for deadline in 0..=100 {
            while let Some(key) = timers.expired(deadline) {
                let position = timers.positions[&key];
                assert!(timers.heap[position].deadline <= deadline);
                timers.remove(&key);
            }
            assert!(timers.heap.iter().all(|timer| timer.deadline > deadline));
        }
        assert_eq!(timers.len(), 0);
    }

    #[test]
    fn equal_deadlines_noop_updates_and_extreme_values() {
        let mut timers = ExpiryIndex::new(0).unwrap();
        // Peeking an empty index never expires anything, even at i64::MAX.
        assert_eq!(timers.expired(i64::MAX), None);
        let a = Bytes::from_static(b"a");
        let b = Bytes::from_static(b"b");
        let c = Bytes::from_static(b"c");
        timers.set(a.clone(), 10);
        timers.set(b.clone(), 10);
        timers.set(c.clone(), 10);
        // Re-setting an identical deadline still keeps one node per key.
        timers.set(a.clone(), 10);
        assert_eq!(timers.len(), 3);
        // Removing an unknown key is a no-op.
        timers.remove(&Bytes::from_static(b"missing"));
        assert_eq!(timers.len(), 3);
        // Tied deadlines all become visible exactly at the shared deadline.
        assert_eq!(timers.expired(9), None);
        let mut drained = 0;
        while let Some(key) = timers.expired(10) {
            timers.remove(&key);
            drained += 1;
        }
        assert_eq!(drained, 3);
        assert_eq!(timers.len(), 0);
        // The minimum deadline is expired at every later clock reading.
        timers.set(a.clone(), i64::MIN);
        assert_eq!(timers.expired(i64::MIN), Some(a.clone()));
        assert_eq!(timers.expired(i64::MAX), Some(a));
    }
}
