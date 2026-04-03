use crate::column::{
    Align, CellText, Column, Emphasis, EmphasisLifetime, EmphasisStyle, FormatSpec, RenderCtx,
    SortCtx, SortKey, WidthHint, compact_role, default_label, format_millis, format_percent,
    nonnegative_f64_to_u64, parse_u64, status_text, u64_to_f64,
};
use crate::target_addr::is_local_addr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CalcKind {
    Addr,
    Alias,
    ProcessId,
    Role,
    Cluster,
    Status,
    LatencyLastMs,
    LatencyMaxMs,
    MaxmemoryPercent {
        used_key: String,
        max_key: String,
    },
    HitratePercent {
        hits_key: String,
        misses_key: String,
    },
    ClientsTotal {
        key: String,
    },
    OpsPerSec {
        key: String,
    },
}

pub struct CalcColumn {
    pub header: String,
    pub kind: CalcKind,
    pub format: FormatSpec,
    pub missing: String,
    pub emphasis: Option<Emphasis>,
    pub emphasis_style: Option<EmphasisStyle>,
    pub align: Align,
    pub width_hint: WidthHint,
}

impl CalcColumn {
    fn calc_value(&self, ctx: &RenderCtx<'_>) -> Option<String> {
        let snap = ctx.snap;
        match &self.kind {
            CalcKind::Addr => Some(snap.addr.clone()),
            CalcKind::Alias => {
                let base = snap
                    .alias
                    .clone()
                    .unwrap_or_else(|| default_label(&snap.addr, ctx.omit_host));
                Some(format!("{}{}", ctx.tree_prefix, base))
            }
            CalcKind::ProcessId => is_local_addr(&snap.addr)
                .then_some(snap.detail.process_id)
                .flatten()
                .map(|value| value.to_string()),
            CalcKind::Role => Some(compact_role(snap).to_string()),
            CalcKind::Cluster => Some(ctx.cluster_label.unwrap_or("?").to_string()),
            CalcKind::Status => Some(status_text(snap.status).to_string()),
            CalcKind::LatencyLastMs => snap.last_latency_ms.map(|value| self.format_f64(value)),
            CalcKind::LatencyMaxMs => Some(self.format_f64(snap.max_latency_ms)),
            CalcKind::MaxmemoryPercent { used_key, max_key } => {
                let used = parse_u64(snap, used_key).or(snap.used_memory_bytes)?;
                let max = parse_u64(snap, max_key).or(snap.maxmemory_bytes)?;
                if max == 0 {
                    return None;
                }
                let value = u64_to_f64(used) / u64_to_f64(max) * 100.0;
                Some(self.format_f64(value))
            }
            CalcKind::HitratePercent {
                hits_key,
                misses_key,
            } => {
                let hits = parse_u64(snap, hits_key)
                    .or(snap.detail.keyspace_hits)
                    .unwrap_or(0);
                let misses = parse_u64(snap, misses_key)
                    .or(snap.detail.keyspace_misses)
                    .unwrap_or(0);
                let total = hits + misses;
                if total == 0 {
                    return None;
                }
                let value = u64_to_f64(hits) / u64_to_f64(total) * 100.0;
                Some(self.format_f64(value))
            }
            CalcKind::ClientsTotal { key } => parse_u64(snap, key)
                .or(snap.detail.connected_clients)
                .map(|value| value.to_string()),
            CalcKind::OpsPerSec { key } => parse_u64(snap, key)
                .or(snap.ops_per_sec)
                .map(|value| value.to_string()),
        }
    }

    fn calc_sort_key(&self, ctx: &SortCtx<'_>) -> SortKey {
        let snap = ctx.snap;
        match &self.kind {
            CalcKind::Addr => SortKey::Str(snap.addr.to_ascii_lowercase()),
            CalcKind::Alias => {
                let value = snap
                    .alias
                    .clone()
                    .unwrap_or_else(|| default_label(&snap.addr, ctx.omit_host));
                SortKey::Str(value.to_ascii_lowercase())
            }
            CalcKind::ProcessId => is_local_addr(&snap.addr)
                .then_some(snap.detail.process_id)
                .flatten()
                .map_or(SortKey::Null, |value| SortKey::U64(u64::from(value))),
            CalcKind::Role => SortKey::Str(compact_role(snap).to_string()),
            CalcKind::Cluster => ctx
                .cluster_label
                .map_or(SortKey::Null, |value| SortKey::Str(value.to_string())),
            CalcKind::Status => SortKey::U64(u64::from(snap.status.severity())),
            CalcKind::LatencyLastMs => snap.last_latency_ms.map_or(SortKey::Null, SortKey::F64),
            CalcKind::LatencyMaxMs => SortKey::F64(snap.max_latency_ms),
            CalcKind::MaxmemoryPercent { used_key, max_key } => {
                let used = parse_u64(snap, used_key).or(snap.used_memory_bytes);
                let max = parse_u64(snap, max_key).or(snap.maxmemory_bytes);
                match (used, max) {
                    (Some(used), Some(max)) if max > 0 => {
                        SortKey::F64(u64_to_f64(used) / u64_to_f64(max))
                    }
                    _ => SortKey::Null,
                }
            }
            CalcKind::HitratePercent {
                hits_key,
                misses_key,
            } => {
                let hits = parse_u64(snap, hits_key)
                    .or(snap.detail.keyspace_hits)
                    .unwrap_or(0);
                let misses = parse_u64(snap, misses_key)
                    .or(snap.detail.keyspace_misses)
                    .unwrap_or(0);
                let total = hits + misses;
                if total == 0 {
                    SortKey::Null
                } else {
                    SortKey::F64(u64_to_f64(hits) / u64_to_f64(total))
                }
            }
            CalcKind::ClientsTotal { key } => parse_u64(snap, key)
                .or(snap.detail.connected_clients)
                .map_or(SortKey::Null, SortKey::U64),
            CalcKind::OpsPerSec { key } => parse_u64(snap, key)
                .or(snap.ops_per_sec)
                .map_or(SortKey::Null, SortKey::U64),
        }
    }

    fn format_f64(&self, value: f64) -> String {
        match self.format {
            FormatSpec::Raw => value.to_string(),
            FormatSpec::Fixed(decimals) => format!("{:.*}", decimals as usize, value),
            FormatSpec::Percent(decimals) => format_percent(value, decimals),
            FormatSpec::Millis(decimals) => format_millis(value, decimals),
            FormatSpec::BytesHuman => crate::column::format_bytes(nonnegative_f64_to_u64(value)),
        }
    }
}

impl Column for CalcColumn {
    fn header(&self) -> &str {
        &self.header
    }

    fn align(&self) -> Align {
        self.align
    }

    fn width_hint(&self) -> WidthHint {
        self.width_hint
    }

    fn render_cell(&self, ctx: &RenderCtx<'_>) -> CellText {
        let text = self.calc_value(ctx).unwrap_or_else(|| self.missing.clone());
        CellText::plain(text)
    }

    fn sort_key(&self, ctx: &SortCtx<'_>) -> SortKey {
        self.calc_sort_key(ctx)
    }

    fn emphasis(&self) -> Option<Emphasis> {
        self.emphasis
    }

    fn emphasis_lifetime(&self) -> EmphasisLifetime {
        match self.kind {
            CalcKind::LatencyMaxMs => EmphasisLifetime::TransientRecord,
            _ => EmphasisLifetime::PersistentWinner,
        }
    }

    fn emphasis_style(&self) -> Option<EmphasisStyle> {
        self.emphasis_style
    }
}
