use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use ratatui::style::Color;
use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewMode {
    Flat,
    Tree,
}

impl ViewMode {
    pub const fn toggle(self) -> Self {
        match self {
            Self::Flat => Self::Tree,
            Self::Tree => Self::Flat,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortMode {
    Alias,
    Address,
    Type,
    Cluster,
    Mem,
    Ops,
    Lat,
    LatMax,
    Status,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum SortDirection {
    #[serde(rename = "asc")]
    Asc,
    #[serde(rename = "desc")]
    Desc,
}

impl SortDirection {
    pub const fn toggle(self) -> Self {
        match self {
            Self::Asc => Self::Desc,
            Self::Desc => Self::Asc,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TargetProtocol {
    Tcp,
    Unix,
}

#[derive(Debug, Clone)]
pub struct Target {
    pub alias: Option<String>,
    pub addr: String,
    pub protocol: TargetProtocol,
    pub username: Option<String>,
    pub password: Option<String>,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct RuntimeSettings {
    pub refresh_interval: Duration,
    pub connect_timeout: Duration,
    pub command_timeout: Duration,
    pub concurrency_limit: usize,
    pub default_view: ViewMode,
    pub default_sort: SortMode,
    pub ui_theme: UiTheme,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UiColor {
    Black,
    Red,
    Green,
    Yellow,
    Blue,
    Magenta,
    Cyan,
    Gray,
    White,
}

impl UiColor {
    pub const fn to_ratatui_color(self) -> Color {
        match self {
            Self::Black => Color::Black,
            Self::Red => Color::Red,
            Self::Green => Color::Green,
            Self::Yellow => Color::Yellow,
            Self::Blue => Color::Blue,
            Self::Magenta => Color::Magenta,
            Self::Cyan => Color::Cyan,
            Self::Gray => Color::Gray,
            Self::White => Color::White,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UiTheme {
    pub background: UiColor,
    pub foreground: UiColor,
    pub carat: UiColor,
    pub warning: UiColor,
    pub critical: UiColor,
}

impl Default for UiTheme {
    fn default() -> Self {
        Self {
            background: UiColor::Black,
            foreground: UiColor::White,
            carat: UiColor::White,
            warning: UiColor::Yellow,
            critical: UiColor::Red,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceType {
    Standalone,
    Cluster,
    Primary,
    Replica,
}

impl InstanceType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Standalone => "standalone",
            Self::Cluster => "cluster",
            Self::Primary => "primary",
            Self::Replica => "replica",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Ok,
    AuthFail,
    Timeout,
    Down,
    Loading,
    Error,
}

impl Status {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::AuthFail => "AUTHFAIL",
            Self::Timeout => "TIMEOUT",
            Self::Down => "DOWN",
            Self::Loading => "LOADING",
            Self::Error => "ERROR",
        }
    }

    pub const fn severity(self) -> u8 {
        match self {
            Self::Down => 0,
            Self::AuthFail => 1,
            Self::Timeout => 2,
            Self::Loading => 3,
            Self::Error => 4,
            Self::Ok => 5,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct DetailMetrics {
    pub redis_version: Option<String>,
    pub uptime_seconds: Option<u64>,
    pub used_memory_rss: Option<u64>,
    pub total_commands_processed: Option<u64>,
    pub connected_clients: Option<u64>,
    pub blocked_clients: Option<u64>,
    pub keyspace_hits: Option<u64>,
    pub keyspace_misses: Option<u64>,
    pub evicted_keys: Option<u64>,
    pub expired_keys: Option<u64>,
    pub role: Option<String>,
    pub master_host: Option<String>,
    pub master_port: Option<u16>,
    pub cluster_enabled: bool,
    pub raw_info: Option<String>,
}

#[derive(Debug, Clone)]
pub struct InstanceState {
    pub key: String,
    pub alias: Option<String>,
    pub addr: String,
    pub kind: InstanceType,
    pub cluster_id: Option<String>,
    pub parent_addr: Option<String>,
    pub tags: Vec<String>,
    pub info: HashMap<String, String>,
    pub used_memory_bytes: Option<u64>,
    pub maxmemory_bytes: Option<u64>,
    pub ops_per_sec: Option<u64>,
    pub last_latency_ms: Option<f64>,
    pub max_latency_ms: f64,
    pub avg_latency_ms: f64,
    pub status: Status,
    pub last_error: Option<String>,
    pub last_updated: Option<Instant>,
    pub latency_window: VecDeque<f64>,
    pub detail: DetailMetrics,
}

impl InstanceState {
    pub fn new(key: String, addr: String) -> Self {
        Self {
            key,
            alias: None,
            addr,
            kind: InstanceType::Standalone,
            cluster_id: None,
            parent_addr: None,
            tags: Vec::new(),
            info: HashMap::new(),
            used_memory_bytes: None,
            maxmemory_bytes: None,
            ops_per_sec: None,
            last_latency_ms: None,
            max_latency_ms: 0.0,
            avg_latency_ms: 0.0,
            status: Status::Down,
            last_error: None,
            last_updated: None,
            latency_window: VecDeque::with_capacity(120),
            detail: DetailMetrics::default(),
        }
    }

    pub fn is_stale(&self, refresh_interval: Duration) -> bool {
        self.last_updated
            .is_none_or(|ts| ts.elapsed() > refresh_interval.saturating_mul(2))
    }

    pub fn push_latency_sample(&mut self, sample_ms: f64) {
        const MAX_SAMPLES: usize = 120;
        if self.latency_window.len() == MAX_SAMPLES {
            let _ = self.latency_window.pop_front();
        }
        self.latency_window.push_back(sample_ms);
        if sample_ms > self.max_latency_ms {
            self.max_latency_ms = sample_ms;
        }
        let total: f64 = self.latency_window.iter().sum();
        self.avg_latency_ms = total / crate::column::usize_to_f64(self.latency_window.len());
    }
}
