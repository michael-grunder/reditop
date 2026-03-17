use std::cmp::Ordering;

use crate::model::{InstanceState, Status};
use crate::target_addr::strip_host;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Align {
    Left,
    Right,
    Center,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WidthHint {
    pub min: u16,
    pub ideal: u16,
    pub max: Option<u16>,
    pub fixed: Option<u16>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SortKey {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    Str(String),
}

impl SortKey {
    pub fn compare(&self, other: &Self) -> Ordering {
        use SortKey::{Bool, F64, I64, Null, Str, U64};

        match (self, other) {
            (Null, Null) => Ordering::Equal,
            (Null, _) => Ordering::Greater,
            (_, Null) => Ordering::Less,
            (Bool(a), Bool(b)) => a.cmp(b),
            (I64(a), I64(b)) => a.cmp(b),
            (U64(a), U64(b)) => a.cmp(b),
            (F64(a), F64(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Str(a), Str(b)) => a.cmp(b),
            (a, b) => variant_rank(a).cmp(&variant_rank(b)),
        }
    }
}

const fn variant_rank(key: &SortKey) -> u8 {
    match key {
        SortKey::Null => 0,
        SortKey::Bool(_) => 1,
        SortKey::I64(_) => 2,
        SortKey::U64(_) => 3,
        SortKey::F64(_) => 4,
        SortKey::Str(_) => 5,
    }
}

#[derive(Debug, Clone)]
pub struct CellText {
    pub text: String,
}

impl CellText {
    pub const fn plain(text: String) -> Self {
        Self { text }
    }
}

pub struct RenderCtx<'a> {
    pub snap: &'a InstanceState,
    pub omit_host: bool,
    pub tree_prefix: &'a str,
    pub cluster_label: Option<&'a str>,
}

pub struct SortCtx<'a> {
    pub snap: &'a InstanceState,
    pub omit_host: bool,
    pub cluster_label: Option<&'a str>,
}

pub trait Column: Send + Sync {
    fn header(&self) -> &str;
    fn align(&self) -> Align;
    fn width_hint(&self) -> WidthHint;
    fn render_cell(&self, ctx: &RenderCtx<'_>) -> CellText;
    fn sort_key(&self, ctx: &SortCtx<'_>) -> SortKey;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    String,
    I64,
    U64,
    F64,
    Bytes,
    Percent,
    Bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormatSpec {
    Raw,
    BytesHuman,
    Fixed(u8),
    Percent(u8),
    Millis(u8),
}

pub fn parse_u64(snap: &InstanceState, key: &str) -> Option<u64> {
    snap.info.get(key)?.parse().ok()
}

pub fn parse_i64(snap: &InstanceState, key: &str) -> Option<i64> {
    snap.info.get(key)?.parse().ok()
}

pub fn parse_f64(snap: &InstanceState, key: &str) -> Option<f64> {
    snap.info.get(key)?.parse().ok()
}

pub fn parse_bool(snap: &InstanceState, key: &str) -> Option<bool> {
    let value = snap.info.get(key)?;
    match value.as_str() {
        "1" | "true" | "yes" => Some(true),
        "0" | "false" | "no" => Some(false),
        _ => None,
    }
}

pub fn parse_string(snap: &InstanceState, key: &str) -> Option<String> {
    snap.info.get(key).cloned()
}

pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = u64_to_f64(bytes);
    let mut idx = 0;
    while value >= 1024.0 && idx + 1 < UNITS.len() {
        value /= 1024.0;
        idx += 1;
    }
    if idx == 0 {
        format!("{bytes} {}", UNITS[idx])
    } else if value.fract() == 0.0 {
        format!("{value:.0} {}", UNITS[idx])
    } else {
        format!("{value:.1} {}", UNITS[idx])
    }
}

pub fn format_percent(value: f64, decimals: u8) -> String {
    format!("{:.*}%", decimals as usize, value)
}

pub fn format_millis(value: f64, decimals: u8) -> String {
    format!("{:.*}", decimals as usize, value)
}

#[allow(clippy::cast_precision_loss)]
pub const fn u64_to_f64(value: u64) -> f64 {
    value as f64
}

#[allow(clippy::cast_precision_loss)]
pub const fn i64_to_f64(value: i64) -> f64 {
    value as f64
}

#[allow(clippy::cast_precision_loss)]
pub const fn usize_to_f64(value: usize) -> f64 {
    value as f64
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub const fn nonnegative_f64_to_u64(value: f64) -> u64 {
    value.max(0.0) as u64
}

pub const fn compact_role(snap: &InstanceState) -> &'static str {
    match snap.kind {
        crate::model::InstanceType::Standalone => "STD",
        crate::model::InstanceType::Cluster => "CLU",
        crate::model::InstanceType::Primary => "PRI",
        crate::model::InstanceType::Replica => "REP",
    }
}

pub fn default_label(addr: &str, omit_host: bool) -> String {
    if omit_host && let Some(without_host) = strip_host(addr) {
        return without_host;
    }
    addr.rsplit('/').next().unwrap_or(addr).to_string()
}

pub const fn status_text(status: Status) -> &'static str {
    status.as_str()
}
