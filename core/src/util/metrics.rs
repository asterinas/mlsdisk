//! System metrics. (Only available in `std`)
//!
//! Measures breakdown latencies and amplifications of the system.
//!
//! # Usage Example
//!
//! Measure latency of a function.
//!
//! ```
//! fn write_something() {
//!     /* omitted */
//! }
//!
//! int main() {
//!     let timer = LatencyMetrics::start_timer(ReqType::Write, "write_something", "write_anything");
//!     write_something();
//!     LatencyMetrics::stop_timer(timer);
//!
//!     Metrics::display();
//! }
//! ```
use crate::os::RwLock;
use hashbrown::HashMap;
use lazy_static::lazy_static;
use std::time::{Duration, Instant};

lazy_static! {
    static ref METRICS: RwLock<Metrics> = RwLock::new(Metrics::new(true));
}

const ENABLE_METRICS: bool = true;

/// System metrics.
/// It measures latency and amplification information.
pub struct Metrics {
    enable: bool,
    pub latency: LatencyMetrics,
    pub amplification: AmplificationMetrics,
}

/// Maintains latency information for each request type.
pub struct LatencyMetrics {
    table: HashMap<ReqType, ReqLatency>,
}

/// Maintains all latency information (in a parent-child manner).
#[derive(Debug)]
pub struct ReqLatency {
    table: HashMap<String, Latency>,
    total: Duration,
}

/// Latency information of one category.
#[derive(Debug)]
pub struct Latency {
    category: String,
    parent_category: String,
    value: Duration,
    level: u8,
}

/// Three request types for measuring latency.
#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum ReqType {
    Write,
    Read,
    Sync,
}

/// Maintains amplification information for each type.
#[derive(Debug)]
pub struct AmplificationMetrics {
    table: HashMap<AmpType, Amplification>,
}

/// One specific amplification.
#[derive(Default, Debug)]
pub struct Amplification {
    pub data: usize,
    pub index: usize,
    pub journal: usize,
    pub total: usize,
}

/// Three amplification types.
#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum AmpType {
    Write,
    Read,
    Space,
}

/// A tik-tok timer for measuring latency once.
pub struct Timer {
    start: Instant,
    req_type: ReqType,
    category: String,
}

impl Metrics {
    pub fn new(enable: bool) -> Self {
        Self {
            enable,
            latency: LatencyMetrics::new(),
            amplification: AmplificationMetrics::new(),
        }
    }

    pub fn display() {
        if !ENABLE_METRICS {
            return;
        }
        println!("========= SwornDisk Metrics =========");
        LatencyMetrics::display();
        AmplificationMetrics::display();
        println!("========= SwornDisk Metrics =========\n",);
    }

    pub fn reset() {
        if !ENABLE_METRICS {
            return;
        }
        LatencyMetrics::reset();
        AmplificationMetrics::reset();
    }
}

impl LatencyMetrics {
    pub fn new() -> Self {
        let table = [
            (ReqType::Write, ReqLatency::new()),
            (ReqType::Read, ReqLatency::new()),
            (ReqType::Sync, ReqLatency::new()),
        ]
        .into_iter()
        .collect();
        Self { table }
    }

    pub fn start_timer(req_type: ReqType, category: &str, parent_category: &str) -> Timer {
        if !ENABLE_METRICS {
            return Timer::default();
        }
        let mut metrics = METRICS.write();
        let table = &mut metrics.latency.table.get_mut(&req_type).unwrap().table;
        let level = if !parent_category.is_empty() && table.contains_key(parent_category) {
            table
                .get(parent_category)
                .expect("parent category must be valid")
                .level
                + 1
        } else {
            0
        };
        if !table.contains_key(category) {
            table.insert(
                category.to_string(),
                Latency {
                    category: category.to_string(),
                    parent_category: parent_category.to_string(),
                    value: Duration::ZERO,
                    level,
                },
            );
        }
        drop(metrics);
        Timer {
            start: Instant::now(),
            req_type,
            category: category.to_string(),
        }
    }

    pub fn stop_timer(timer: Timer) {
        if !ENABLE_METRICS {
            return;
        }
        let elapsed = timer.start.elapsed();
        let mut metrics = METRICS.write();
        let req_latency = metrics.latency.table.get_mut(&timer.req_type).unwrap();
        let latency = req_latency.table.get_mut(&timer.category).unwrap();
        latency.value += elapsed;
        if latency.level == 0 {
            req_latency.total += elapsed;
        }
    }

    pub fn reset() {
        let mut metrics = METRICS.write();
        metrics.latency.table.values_mut().for_each(|req_latency| {
            req_latency.table.clear();
            req_latency.total = Duration::ZERO;
        });
    }

    pub fn display() {
        println!("===== Latency Metrics =====");
        let metrics = METRICS.read();
        for (req_type, req_latency) in metrics.latency.table.iter() {
            println!("{:?}:", req_type);
            let mut level = 0u8;
            loop {
                let mut lats: Vec<_> = req_latency
                    .table
                    .iter()
                    .filter(|(_, lat)| lat.level == level)
                    .collect();
                if lats.is_empty() {
                    break;
                }
                lats.sort_by_key(|lat| &lat.1.parent_category);
                for (cat, lat) in &lats {
                    let indent = "  ".repeat(level as _);
                    let parent_cat = &lat.parent_category;
                    let total_lat = if parent_cat.is_empty() {
                        req_latency.total
                    } else {
                        req_latency.table.get(parent_cat).unwrap().value
                    };
                    println!(
                        "{indent}{parent_cat}-{cat}: {:.3?} ({:.2}%)",
                        lat.value,
                        (lat.value.as_secs_f64() / total_lat.as_secs_f64()) * 100.0
                    );
                }
                level += 1;
            }
        }
        println!("===== Latency Metrics =====");
    }
}

impl ReqLatency {
    pub fn new() -> Self {
        Self {
            table: HashMap::new(),
            total: Duration::ZERO,
        }
    }
}

impl Default for Timer {
    fn default() -> Self {
        Self {
            start: Instant::now(),
            req_type: ReqType::Read,
            category: String::new(),
        }
    }
}

impl AmplificationMetrics {
    pub fn new() -> Self {
        let table = [
            (AmpType::Write, Amplification::default()),
            (AmpType::Read, Amplification::default()),
            (AmpType::Space, Amplification::default()),
        ]
        .into_iter()
        .collect();
        Self { table }
    }

    pub fn acc_data_amount(amp_type: AmpType, amount: usize) {
        if !ENABLE_METRICS {
            return;
        }
        let mut metrics = METRICS.write();
        let amp = metrics.amplification.table.get_mut(&amp_type).unwrap();
        amp.data += amount;
    }

    pub fn acc_index_amount(amp_type: AmpType, amount: usize) {
        if !ENABLE_METRICS {
            return;
        }
        let mut metrics = METRICS.write();
        let amp = metrics.amplification.table.get_mut(&amp_type).unwrap();
        amp.index += amount;
    }

    pub fn acc_journal_amount(amp_type: AmpType, amount: usize) {
        if !ENABLE_METRICS {
            return;
        }
        let mut metrics = METRICS.write();
        let amp = metrics.amplification.table.get_mut(&amp_type).unwrap();
        amp.journal += amount;
    }

    pub fn reset() {
        let mut metrics = METRICS.write();
        metrics.amplification.table.values_mut().for_each(|amp| {
            amp.data = 0;
            amp.index = 0;
            amp.journal = 0;
            amp.total = 0;
        });
    }

    pub fn display() {
        println!("===== Amplification Metrics =====");
        let metrics = METRICS.read();
        for (amp_type, amp) in metrics.amplification.table.iter() {
            let factor = if amp.data != 0 {
                (amp.data + amp.index + amp.journal) as f64 / amp.data as f64
            } else {
                f64::NAN
            };
            println!("{:?} Amplification Factor: {:.3}", amp_type, factor);
        }
        println!("===== Amplification Metrics =====");
    }
}
