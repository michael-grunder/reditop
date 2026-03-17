use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Result, bail};
use serde::Deserialize;

use crate::column::{Align, Column, FormatSpec, ValueType, WidthHint};
use crate::columns::calc::{CalcColumn, CalcKind};
use crate::columns::info::RedisInfoFieldColumn;
use crate::model::{SortDirection, SortMode};

const DEFAULT_COLUMNS_TOML: &str = include_str!("default_columns.toml");

pub struct ColumnRegistry {
    columns: HashMap<String, Arc<dyn Column>>,
    column_order: Vec<String>,
    pub visible_overview: Vec<String>,
    pub default_sort_by: String,
    pub default_sort_direction: SortDirection,
}

impl ColumnRegistry {
    pub fn load(config_path: Option<&Path>, no_default_config: bool, cli_sort: SortMode) -> Self {
        let mut registry = Self {
            columns: HashMap::new(),
            column_order: Vec::new(),
            visible_overview: Vec::new(),
            default_sort_by: legacy_sort_key(cli_sort).to_string(),
            default_sort_direction: legacy_sort_direction(cli_sort),
        };

        if let Err(err) = registry.apply_layer(DEFAULT_COLUMNS_TOML) {
            eprintln!("warning: failed to parse built-in column config: {err}");
        }

        let user_config = resolve_config_path(config_path, no_default_config);
        if let Some(path) = user_config {
            match fs::read_to_string(&path) {
                Ok(content) => {
                    if let Err(err) = registry.apply_layer(&content) {
                        eprintln!(
                            "warning: failed to parse column definitions from {}: {err}",
                            path.display()
                        );
                    }
                }
                Err(err) => {
                    eprintln!(
                        "warning: failed to read config for column overrides {}: {err}",
                        path.display()
                    );
                }
            }
        }

        if registry.visible_overview.is_empty() {
            registry.visible_overview = default_visible_columns();
        }

        registry
            .visible_overview
            .retain(|key| registry.columns.contains_key(key));
        if registry.visible_overview.is_empty() {
            registry.visible_overview = registry.column_order.clone();
        }

        if !registry.columns.contains_key(&registry.default_sort_by) {
            registry.default_sort_by = registry
                .visible_overview
                .first()
                .cloned()
                .unwrap_or_else(|| "alias".to_string());
        }

        registry
    }

    pub fn column(&self, key: &str) -> Option<&Arc<dyn Column>> {
        self.columns.get(key)
    }

    pub fn visible_columns(&self, show_address: bool) -> Vec<String> {
        self.visible_overview
            .iter()
            .filter(|key| show_address || key.as_str() != "addr")
            .filter(|key| self.columns.contains_key(key.as_str()))
            .cloned()
            .collect()
    }

    fn apply_layer(&mut self, input: &str) -> Result<()> {
        let config: ColumnFileConfig = toml::from_str(input)?;

        if let Some(columns) = config.columns {
            for (key, def) in columns {
                let column = build_column(&key, &def)?;
                let exists = self.columns.insert(key.clone(), Arc::from(column));
                if exists.is_none() {
                    self.column_order.push(key);
                }
            }
        }

        if let Some(view) = config.view
            && let Some(overview) = view.overview
        {
            if let Some(visible) = overview.visible {
                self.visible_overview = visible;
            }
            if let Some(sort) = overview.sort {
                if let Some(by) = sort.by {
                    self.default_sort_by = by;
                }
                if let Some(dir) = sort.dir {
                    self.default_sort_direction = dir;
                }
            }
        }

        Ok(())
    }
}

fn resolve_config_path(explicit: Option<&Path>, no_default: bool) -> Option<PathBuf> {
    if let Some(path) = explicit {
        return Some(path.to_path_buf());
    }
    if no_default {
        return None;
    }

    let mut candidates = Vec::new();
    if let Some(xdg) = env::var_os("XDG_CONFIG_HOME") {
        candidates.push(PathBuf::from(xdg).join("redis-top").join("config.toml"));
    }
    if let Some(home) = env::var_os("HOME") {
        candidates.push(
            PathBuf::from(home)
                .join(".config")
                .join("redis-top")
                .join("config.toml"),
        );
    }
    candidates.push(PathBuf::from("redis-top.toml"));

    candidates.into_iter().find(|candidate| candidate.exists())
}

#[derive(Debug, Deserialize, Default)]
struct ColumnFileConfig {
    columns: Option<HashMap<String, ColumnDef>>,
    view: Option<ViewConfig>,
}

#[derive(Debug, Deserialize, Default)]
struct ViewConfig {
    overview: Option<OverviewConfig>,
}

#[derive(Debug, Deserialize, Default)]
struct OverviewConfig {
    visible: Option<Vec<String>>,
    sort: Option<SortConfig>,
}

#[derive(Debug, Deserialize, Default)]
struct SortConfig {
    by: Option<String>,
    dir: Option<SortDirection>,
}

#[derive(Debug, Deserialize, Clone)]
struct ColumnDef {
    #[serde(rename = "type")]
    type_name: String,
    header: Option<String>,
    align: Option<String>,
    min_width: Option<u16>,
    ideal_width: Option<u16>,
    max_width: Option<u16>,
    fixed_width: Option<u16>,

    info_key: Option<String>,
    value_type: Option<String>,
    format: Option<String>,
    missing: Option<String>,

    calc: Option<String>,
}

fn build_column(key: &str, def: &ColumnDef) -> Result<Box<dyn Column>> {
    let width_hint = WidthHint {
        min: def.min_width.unwrap_or(4),
        ideal: def.ideal_width.unwrap_or(8),
        max: def.max_width,
        fixed: def.fixed_width,
    };
    let header = def.header.clone().unwrap_or_else(|| key.to_string());
    let missing = def.missing.clone().unwrap_or_default();

    match def.type_name.as_str() {
        "info" => {
            let info_key = def
                .info_key
                .clone()
                .ok_or_else(|| anyhow::anyhow!("info column {key} missing info_key"))?;
            let value_type = parse_value_type(def.value_type.as_deref().unwrap_or("string"))?;
            let format = parse_format(def.format.as_deref().unwrap_or("raw"))?;
            let align = def
                .align
                .as_deref()
                .map(parse_align)
                .transpose()?
                .unwrap_or_else(|| default_align_for_value_type(value_type));

            Ok(Box::new(RedisInfoFieldColumn {
                header,
                info_key,
                value_type,
                format,
                missing,
                align,
                width_hint,
            }))
        }
        "calc" => {
            let calc_raw = def
                .calc
                .clone()
                .ok_or_else(|| anyhow::anyhow!("calc column {key} missing calc"))?;
            let calc_kind = parse_calc_kind(&calc_raw)?;
            let format = parse_format(def.format.as_deref().unwrap_or("raw"))?;
            let align = def
                .align
                .as_deref()
                .map(parse_align)
                .transpose()?
                .unwrap_or(default_align_for_calc(&calc_kind));

            Ok(Box::new(CalcColumn {
                header,
                kind: calc_kind,
                format,
                missing,
                align,
                width_hint,
            }))
        }
        other => bail!("unsupported column type for {key}: {other}"),
    }
}

fn parse_value_type(raw: &str) -> Result<ValueType> {
    match raw {
        "string" => Ok(ValueType::String),
        "u64" => Ok(ValueType::U64),
        "i64" => Ok(ValueType::I64),
        "f64" => Ok(ValueType::F64),
        "bytes" => Ok(ValueType::Bytes),
        "percent" => Ok(ValueType::Percent),
        "bool" => Ok(ValueType::Bool),
        other => bail!("unsupported value_type: {other}"),
    }
}

fn parse_align(raw: &str) -> Result<Align> {
    match raw {
        "left" => Ok(Align::Left),
        "right" => Ok(Align::Right),
        "center" => Ok(Align::Center),
        other => bail!("unsupported align value: {other}"),
    }
}

fn parse_format(raw: &str) -> Result<FormatSpec> {
    if raw == "raw" {
        return Ok(FormatSpec::Raw);
    }
    if raw == "bytes_human" {
        return Ok(FormatSpec::BytesHuman);
    }
    if let Some((kind, tail)) = raw.split_once(':') {
        let decimals = tail.parse::<u8>()?;
        return match kind {
            "fixed" => Ok(FormatSpec::Fixed(decimals)),
            "pct" => Ok(FormatSpec::Percent(decimals)),
            "ms" => Ok(FormatSpec::Millis(decimals)),
            _ => bail!("unsupported format: {raw}"),
        };
    }
    bail!("unsupported format: {raw}")
}

fn parse_calc_kind(raw: &str) -> Result<CalcKind> {
    match raw {
        "$addr" => Ok(CalcKind::Addr),
        "$alias" => Ok(CalcKind::Alias),
        "$role" => Ok(CalcKind::Role),
        "$cluster" => Ok(CalcKind::Cluster),
        "$status" => Ok(CalcKind::Status),
        "latency_last_ms" => Ok(CalcKind::LatencyLastMs),
        "latency_max_ms" => Ok(CalcKind::LatencyMaxMs),
        "maxmemory_percent" => Ok(CalcKind::MaxmemoryPercent {
            used_key: "used_memory".to_string(),
            max_key: "maxmemory".to_string(),
        }),
        "hitrate_percent" => Ok(CalcKind::HitratePercent {
            hits_key: "keyspace_hits".to_string(),
            misses_key: "keyspace_misses".to_string(),
        }),
        "clients_total" => Ok(CalcKind::ClientsTotal {
            key: "connected_clients".to_string(),
        }),
        "ops_per_sec" => Ok(CalcKind::OpsPerSec {
            key: "instantaneous_ops_per_sec".to_string(),
        }),
        other => bail!("unsupported calc: {other}"),
    }
}

const fn default_align_for_value_type(value_type: ValueType) -> Align {
    match value_type {
        ValueType::String => Align::Left,
        ValueType::Bool => Align::Center,
        ValueType::I64
        | ValueType::U64
        | ValueType::F64
        | ValueType::Bytes
        | ValueType::Percent => Align::Right,
    }
}

const fn default_align_for_calc(kind: &CalcKind) -> Align {
    match kind {
        CalcKind::Addr
        | CalcKind::Alias
        | CalcKind::Role
        | CalcKind::Status
        | CalcKind::Cluster => Align::Left,
        _ => Align::Right,
    }
}

fn default_visible_columns() -> Vec<String> {
    vec![
        "alias".to_string(),
        "addr".to_string(),
        "role".to_string(),
        "cluster".to_string(),
        "used_mem".to_string(),
        "maxmem_pct".to_string(),
        "ops".to_string(),
        "lat_last".to_string(),
        "lat_max".to_string(),
        "status".to_string(),
    ]
}

pub const fn legacy_sort_key(mode: SortMode) -> &'static str {
    match mode {
        SortMode::Alias => "alias",
        SortMode::Address => "addr",
        SortMode::Type => "role",
        SortMode::Cluster => "cluster",
        SortMode::Mem => "used_mem",
        SortMode::Ops => "ops",
        SortMode::Lat => "lat_last",
        SortMode::LatMax => "lat_max",
        SortMode::Status => "status",
    }
}

pub const fn legacy_sort_direction(mode: SortMode) -> SortDirection {
    match mode {
        SortMode::Alias
        | SortMode::Address
        | SortMode::Type
        | SortMode::Cluster
        | SortMode::Status => SortDirection::Asc,
        SortMode::Mem | SortMode::Ops | SortMode::Lat | SortMode::LatMax => SortDirection::Desc,
    }
}

#[cfg(test)]
mod tests {
    use super::ColumnRegistry;
    use crate::model::SortMode;

    #[test]
    fn loads_builtin_columns() {
        let registry = ColumnRegistry::load(None, true, SortMode::Address);
        assert!(registry.column("alias").is_some());
        assert!(registry.column("used_mem").is_some());
        assert!(registry.visible_overview.iter().any(|k| k == "alias"));
    }
}
