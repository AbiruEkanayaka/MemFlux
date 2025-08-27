use std::collections::{HashSet, VecDeque};
use std::hash::Hash;

#[derive(Debug)]
pub struct ArcCache<K: Eq + Hash + Clone> {
    p: usize,
    pub capacity: usize,

    t1: VecDeque<K>, // MRU is at the front
    b1: VecDeque<K>,

    t2: VecDeque<K>,
    b2: VecDeque<K>,

    // For quick lookups
    t1_keys: HashSet<K>,
    t2_keys: HashSet<K>,
    b1_keys: HashSet<K>,
    b2_keys: HashSet<K>,
}

impl<K: Eq + Hash + Clone> ArcCache<K> {
    pub fn new(capacity: usize) -> Self {
        Self {
            p: 0,
            capacity,
            t1: VecDeque::with_capacity(capacity),
            b1: VecDeque::with_capacity(capacity),
            t2: VecDeque::with_capacity(capacity),
            b2: VecDeque::with_capacity(capacity),
            t1_keys: HashSet::with_capacity(capacity),
            t2_keys: HashSet::with_capacity(capacity),
            b1_keys: HashSet::with_capacity(capacity),
            b2_keys: HashSet::with_capacity(capacity),
        }
    }

    pub fn prime(&mut self, keys: Vec<K>) {
        for key in keys.into_iter().take(self.capacity) {
             self.t1.push_front(key.clone());
             self.t1_keys.insert(key);
         }
    }

    fn remove_from_deque(deque: &mut VecDeque<K>, key: &K) -> Option<K> {
        if let Some(pos) = deque.iter().position(|k| k == key) {
            deque.remove(pos)
        } else {
            None
        }
    }

    pub fn track_access(&mut self, key: &K) {
        // Case 1: Hit in T1 or T2
        if self.t1_keys.contains(key) {
            let k = Self::remove_from_deque(&mut self.t1, key).unwrap();
            self.t1_keys.remove(&k);
            self.t2.push_front(k.clone());
            self.t2_keys.insert(k);
            return;
        }

        if self.t2_keys.contains(key) {
            let k = Self::remove_from_deque(&mut self.t2, key).unwrap();
            self.t2.push_front(k);
            return;
        }

        // Case 2: Miss, check ghost lists
        if self.b1_keys.contains(key) {
                        let delta = if self.b1.len() >= self.b2.len() { 
                1 
            } else if self.b1.len() == 0 {
                1
            } else {
                (self.b2.len() / self.b1.len()).max(1)
            };
            self.p = (self.p + delta).min(self.capacity);
            self.replace(key);
            let k = Self::remove_from_deque(&mut self.b1, key).unwrap();
            self.b1_keys.remove(&k);
            self.t2.push_front(k.clone());
            self.t2_keys.insert(k);
            return;
        }

        if self.b2_keys.contains(key) {
                        let delta = if self.b2.len() >= self.b1.len() { 
                1 
            } else if self.b2.len() == 0 {
                1
            } else {
                (self.b1.len() / self.b2.len()).max(1)
            };
            self.p = (self.p as isize - delta as isize).max(0) as usize;
            self.replace(key);
            let k = Self::remove_from_deque(&mut self.b2, key).unwrap();
            self.b2_keys.remove(&k);
            self.t2.push_front(k.clone());
            self.t2_keys.insert(k);
            return;
        }

        // Case 3: True miss (new key)
        if self.t1.len() + self.b1.len() == self.capacity {
            if self.t1.len() < self.capacity {
                if let Some(k) = self.b1.pop_back() {
                    self.b1_keys.remove(&k);
                }
                self.replace(key);
            } else {
                if let Some(k) = self.t1.pop_back() {
                    self.t1_keys.remove(&k);
                }
            }
        } else if self.t1.len() + self.b1.len() < self.capacity {
            let total_len = self.t1.len() + self.t2.len() + self.b1.len() + self.b2.len();
            if total_len >= self.capacity {
                if total_len == 2 * self.capacity {
                    if let Some(k) = self.b2.pop_back() {
                        self.b2_keys.remove(&k);
                    }
                }
                self.replace(key);
            }
        }

        self.t1.push_front(key.clone());
        self.t1_keys.insert(key.clone());
    }

    fn replace(&mut self, key: &K) {
        let t1_full = self.t1.len() >= self.p;
        let b2_hit = self.b2_keys.contains(key);

        if !self.t1.is_empty() && (t1_full || b2_hit) {
            if let Some(k) = self.t1.pop_back() {
                self.t1_keys.remove(&k);
                self.b1.push_front(k.clone());
                self.b1_keys.insert(k);
            }
        } else {
            if let Some(k) = self.t2.pop_back() {
                self.t2_keys.remove(&k);
                self.b2.push_front(k.clone());
                self.b2_keys.insert(k);
            }
        }
    }

    pub fn evict(&mut self) -> Option<K> {
        let t1_len = self.t1.len();
        if t1_len > 0 && t1_len >= self.p {
            self.t1.pop_back().map(|k| {
                self.t1_keys.remove(&k);
                self.b1.push_front(k.clone());
                self.b1_keys.insert(k.clone());
                k
            })
        } else {
            self.t2.pop_back().map(|k| {
                self.t2_keys.remove(&k);
                self.b2.push_front(k.clone());
                self.b2_keys.insert(k.clone());
                k
            })
        }
    }

    pub fn forget_key(&mut self, key: &K) {
        if self.t1_keys.remove(key) {
            Self::remove_from_deque(&mut self.t1, key);
        }
        if self.t2_keys.remove(key) {
            Self::remove_from_deque(&mut self.t2, key);
        }
        if self.b1_keys.remove(key) {
            Self::remove_from_deque(&mut self.b1, key);
        }
        if self.b2_keys.remove(key) {
            Self::remove_from_deque(&mut self.b2, key);
        }
    }

    pub fn restore_evicted_key(&mut self, key: K) {
        self.forget_key(&key);
        self.t1.push_front(key.clone());
        self.t1_keys.insert(key);
    }
}
