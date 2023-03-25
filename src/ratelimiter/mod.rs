use std::{cmp::Ordering, time::Instant};

fn min_f64(a: f64, b: f64) -> f64 {
    match a.partial_cmp(&b) {
        Some(Ordering::Less) => a,
        Some(Ordering::Greater) => b,
        Some(Ordering::Equal) => a,
        None => panic!("unable compare {a} with {b}"),
    }
}

fn must_gt_zero(f: f64) {
    if f < 0.0 {
        panic!("{f} is less than 0");
    }
}

pub struct RateLimiter {
    quota: f64,
    burst: f64,
    replenish_per_sec: f64,
    start: Instant,
}

impl RateLimiter {
    pub fn new(burst: f64, replenish_per_sec: f64) -> Self {
        Self {
            quota: burst,
            burst,
            replenish_per_sec,
            start: Instant::now(),
        }
    }

    pub fn take_all(&mut self) -> bool {
        self.take(self.burst)
    }

    pub fn take(&mut self, n: f64) -> bool {
        if n > self.burst {
            return false;
        }

        let t = Instant::now();
        self.quota = self.next_quota(t);
        self.start = t;

        if n <= self.quota {
            self.quota -= n;
            true
        } else {
            false
        }
    }

    pub fn can_take(&self, n: f64) -> bool {
        self.next_quota(Instant::now()) >= n
    }

    pub fn put(&mut self, n: f64) {
        must_gt_zero(n);
        self.quota = min_f64(self.burst, self.quota + n);
    }

    pub fn set_burst(&mut self, burst: f64) {
        must_gt_zero(burst);
        self.burst = burst;
    }

    pub fn burst(&self) -> f64 {
        self.burst
    }

    pub fn next_quota(&self, t: Instant) -> f64 {
        min_f64(
            self.burst,
            self.quota + t.duration_since(self.start).as_secs_f64() * self.replenish_per_sec,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_limiter() {
        let mut limiter = RateLimiter::new(3.0, 1.0);
        assert!(limiter.take(3.0));
        assert!(!limiter.take(3.0));

        thread::sleep(Duration::from_secs(1));
        assert!(limiter.next_quota(Instant::now()) > 1.0);
        thread::sleep(Duration::from_secs(2));

        assert!(limiter.take(3.0));
        limiter.put(5.0);
        assert!(limiter.next_quota(Instant::now()) >= 3.0);
        assert!(limiter.take(3.0));
    }
}
