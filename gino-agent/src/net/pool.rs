//! Fake-address pool over 198.18.0.0/15 mapping allowed names to addresses.

use std::collections::{BTreeSet, HashMap};
use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

use super::rules::normalize_name;

/// Network address of the pool (198.18.0.0).
const POOL_NET: u32 = 0xC612_0000;
/// Mask of the /15.
const POOL_MASK: u32 = 0xFFFE_0000;
/// First handed-out address (198.18.0.1); the network address is skipped.
const FIRST_ADDR: u32 = POOL_NET + 1;
/// Usable addresses: the /15 minus the network and broadcast addresses.
pub const MAX_CAPACITY: usize = (1 << 17) - 2;

/// Pool sizing and reuse policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolConfig {
    /// Maximum number of addresses handed out (1..=`MAX_CAPACITY`).
    pub capacity: usize,
    /// Minimum idle time before an address may be given to another name.
    pub idle_reuse: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            capacity: 4096,
            idle_reuse: Duration::from_mins(10),
        }
    }
}

/// Pool configuration or allocation failure.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum PoolError {
    /// The configured capacity is 0 or larger than the usable /15.
    #[error("fake address pool capacity {capacity} is out of range (1..={MAX_CAPACITY})")]
    InvalidCapacity { capacity: usize },
    /// Every address is in use and none has been idle long enough to reuse.
    #[error("fake address pool exhausted ({capacity} addresses in use)")]
    Exhausted { capacity: usize },
}

#[derive(Debug)]
struct Entry {
    name: String,
    last_use: Instant,
}

/// Bidirectional name↔address map with least-recently-used reclaim.
/// Time is supplied by the caller; the pool never reads a clock.
#[derive(Debug)]
pub struct FakePool {
    capacity: u32,
    idle_reuse: Duration,
    /// Offset (from `FIRST_ADDR`) of the next never-used address.
    next_unused: u32,
    by_name: HashMap<String, u32>,
    by_offset: HashMap<u32, Entry>,
    /// Entries ordered by last use, oldest first.
    lru: BTreeSet<(Instant, u32)>,
}

impl FakePool {
    /// Creates an empty pool.
    pub fn new(cfg: PoolConfig) -> Result<Self, PoolError> {
        let capacity = u32::try_from(cfg.capacity)
            .ok()
            .filter(|_| (1..=MAX_CAPACITY).contains(&cfg.capacity))
            .ok_or(PoolError::InvalidCapacity {
                capacity: cfg.capacity,
            })?;
        Ok(Self {
            capacity,
            idle_reuse: cfg.idle_reuse,
            next_unused: 0,
            by_name: HashMap::new(),
            by_offset: HashMap::new(),
            lru: BTreeSet::new(),
        })
    }

    /// Returns the address for `name` (normalised), allocating one if needed.
    /// A new name takes a never-used address, else reclaims the least
    /// recently used entry idle for at least `idle_reuse`.
    pub fn allocate(&mut self, name: &str, now: Instant) -> Result<Ipv4Addr, PoolError> {
        let name = normalize_name(name);
        if let Some(&offset) = self.by_name.get(&name) {
            self.refresh(offset, now);
            return Ok(addr_of(offset));
        }
        let offset = if self.next_unused < self.capacity {
            let offset = self.next_unused;
            self.next_unused += 1;
            offset
        } else {
            self.reclaim(now)?
        };
        self.by_name.insert(name.clone(), offset);
        self.by_offset.insert(
            offset,
            Entry {
                name,
                last_use: now,
            },
        );
        self.lru.insert((now, offset));
        Ok(addr_of(offset))
    }

    /// Name currently mapped to `addr`; refreshes its last use.
    pub fn lookup(&mut self, addr: Ipv4Addr, now: Instant) -> Option<String> {
        let offset = self.offset_of(addr)?;
        self.refresh(offset, now);
        self.by_offset.get(&offset).map(|entry| entry.name.clone())
    }

    /// Refreshes the last use of `addr` (for long-lived flows).
    pub fn touch(&mut self, addr: Ipv4Addr, now: Instant) {
        if let Some(offset) = self.offset_of(addr) {
            self.refresh(offset, now);
        }
    }

    /// True when `addr` lies inside 198.18.0.0/15.
    pub fn contains(addr: Ipv4Addr) -> bool {
        u32::from(addr) & POOL_MASK == POOL_NET
    }

    /// Number of names currently mapped.
    pub fn len(&self) -> usize {
        self.by_name.len()
    }

    /// True when no name is mapped.
    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }

    /// Offset of a mapped address, if any.
    fn offset_of(&self, addr: Ipv4Addr) -> Option<u32> {
        let offset = u32::from(addr).checked_sub(FIRST_ADDR)?;
        self.by_offset.contains_key(&offset).then_some(offset)
    }

    fn refresh(&mut self, offset: u32, now: Instant) {
        if let Some(entry) = self.by_offset.get_mut(&offset) {
            self.lru.remove(&(entry.last_use, offset));
            entry.last_use = now;
            self.lru.insert((now, offset));
        }
    }

    /// Frees the oldest entry if it has been idle long enough.
    fn reclaim(&mut self, now: Instant) -> Result<u32, PoolError> {
        let exhausted = PoolError::Exhausted {
            capacity: self.capacity as usize,
        };
        let &(last_use, offset) = self.lru.first().ok_or_else(|| exhausted.clone())?;
        if now.saturating_duration_since(last_use) < self.idle_reuse {
            return Err(exhausted);
        }
        self.lru.remove(&(last_use, offset));
        if let Some(entry) = self.by_offset.remove(&offset) {
            self.by_name.remove(&entry.name);
        }
        Ok(offset)
    }
}

fn addr_of(offset: u32) -> Ipv4Addr {
    Ipv4Addr::from(FIRST_ADDR + offset)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool(capacity: usize, idle_secs: u64) -> FakePool {
        FakePool::new(PoolConfig {
            capacity,
            idle_reuse: Duration::from_secs(idle_secs),
        })
        .unwrap()
    }

    fn at(base: Instant, secs: u64) -> Instant {
        base + Duration::from_secs(secs)
    }

    #[test]
    fn default_config() {
        let cfg = PoolConfig::default();
        assert_eq!(cfg.capacity, 4096);
        assert_eq!(cfg.idle_reuse, Duration::from_mins(10));
        assert!(FakePool::new(cfg).unwrap().is_empty());
    }

    #[test]
    fn capacity_bounds() {
        for capacity in [0, MAX_CAPACITY + 1, usize::MAX] {
            let err = FakePool::new(PoolConfig {
                capacity,
                ..PoolConfig::default()
            })
            .unwrap_err();
            assert_eq!(err, PoolError::InvalidCapacity { capacity });
            assert!(err.to_string().contains(&capacity.to_string()), "{err}");
        }
        assert_eq!(MAX_CAPACITY, 131_070);
        let mut p = pool(MAX_CAPACITY, 0);
        let t = Instant::now();
        assert_eq!(
            p.allocate("a.com", t).unwrap(),
            Ipv4Addr::new(198, 18, 0, 1)
        );
        p.next_unused = p.capacity - 1;
        assert_eq!(
            p.allocate("last.com", t).unwrap(),
            Ipv4Addr::new(198, 19, 255, 254)
        );
    }

    #[test]
    fn stable_address_per_name() {
        let mut p = pool(16, 60);
        let t = Instant::now();
        let a = p.allocate("Example.com", t).unwrap();
        for name in ["example.com", "EXAMPLE.COM.", "example.com."] {
            assert_eq!(p.allocate(name, t).unwrap(), a);
        }
        assert_eq!(p.len(), 1);
    }

    #[test]
    fn distinct_names_distinct_addresses() {
        let mut p = pool(16, 60);
        let t = Instant::now();
        let addrs: Vec<_> = ["a.com", "b.com", "c.com"]
            .iter()
            .map(|n| p.allocate(n, t).unwrap())
            .collect();
        assert_ne!(addrs[0], addrs[1]);
        assert_ne!(addrs[1], addrs[2]);
        assert_ne!(addrs[0], addrs[2]);
        assert!(addrs.iter().all(|a| FakePool::contains(*a)));
        assert_eq!(p.len(), 3);
    }

    #[test]
    fn lookup_reverse() {
        let mut p = pool(16, 60);
        let t = Instant::now();
        let a = p.allocate("GitHub.com.", t).unwrap();
        assert_eq!(p.lookup(a, t).as_deref(), Some("github.com"));
        assert_eq!(p.lookup(Ipv4Addr::new(198, 18, 0, 200), t), None);
        assert_eq!(p.lookup(Ipv4Addr::new(198, 18, 0, 0), t), None);
        assert_eq!(p.lookup(Ipv4Addr::new(10, 0, 0, 1), t), None);
    }

    #[test]
    fn exhausted_before_idle_reuse() {
        let mut p = pool(2, 60);
        let base = Instant::now();
        p.allocate("a.com", base).unwrap();
        p.allocate("b.com", at(base, 1)).unwrap();
        let err = p.allocate("c.com", at(base, 59)).unwrap_err();
        assert_eq!(err, PoolError::Exhausted { capacity: 2 });
        assert_eq!(
            err.to_string(),
            "fake address pool exhausted (2 addresses in use)"
        );
        assert_eq!(p.len(), 2);
    }

    #[test]
    fn idle_entry_reclaimed() {
        let mut p = pool(2, 60);
        let base = Instant::now();
        let a = p.allocate("a.com", base).unwrap();
        let b = p.allocate("b.com", at(base, 10)).unwrap();
        let c = p.allocate("c.com", at(base, 60)).unwrap();
        assert_eq!(c, a);
        assert_eq!(p.lookup(a, at(base, 60)).as_deref(), Some("c.com"));
        assert_eq!(p.lookup(b, at(base, 60)).as_deref(), Some("b.com"));
        assert_eq!(p.len(), 2);
        // a.com lost its mapping and needs a fresh one.
        assert_eq!(
            p.allocate("a.com", at(base, 61)),
            Err(PoolError::Exhausted { capacity: 2 })
        );
    }

    #[test]
    fn lookup_refreshes_last_use() {
        let mut p = pool(2, 60);
        let base = Instant::now();
        let a = p.allocate("a.com", base).unwrap();
        let b = p.allocate("b.com", at(base, 10)).unwrap();
        assert_eq!(p.lookup(a, at(base, 50)).as_deref(), Some("a.com"));
        // b.com is now the oldest entry.
        assert_eq!(p.allocate("c.com", at(base, 70)).unwrap(), b);
        assert_eq!(p.lookup(a, at(base, 70)).as_deref(), Some("a.com"));
        assert_eq!(
            p.allocate("d.com", at(base, 100)),
            Err(PoolError::Exhausted { capacity: 2 })
        );
    }

    #[test]
    fn touch_and_allocate_refresh_last_use() {
        let mut p = pool(2, 60);
        let base = Instant::now();
        let a = p.allocate("a.com", base).unwrap();
        let b = p.allocate("b.com", at(base, 10)).unwrap();
        p.touch(a, at(base, 50));
        p.touch(Ipv4Addr::new(1, 1, 1, 1), at(base, 50));
        assert_eq!(p.allocate("c.com", at(base, 70)).unwrap(), b);
        assert_eq!(p.allocate("a.com", at(base, 109)).unwrap(), a);
        let c = p.allocate("d.com", at(base, 130)).unwrap();
        assert_eq!(p.lookup(c, at(base, 130)).as_deref(), Some("d.com"));
        assert_eq!(p.lookup(a, at(base, 130)).as_deref(), Some("a.com"));
    }

    #[test]
    fn contains() {
        assert!(FakePool::contains(Ipv4Addr::new(198, 18, 0, 0)));
        assert!(FakePool::contains(Ipv4Addr::new(198, 18, 0, 1)));
        assert!(FakePool::contains(Ipv4Addr::new(198, 19, 255, 255)));
        assert!(!FakePool::contains(Ipv4Addr::new(198, 17, 255, 255)));
        assert!(!FakePool::contains(Ipv4Addr::new(198, 20, 0, 0)));
        assert!(!FakePool::contains(Ipv4Addr::new(10, 0, 0, 1)));
    }
}
