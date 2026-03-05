use std::collections::HashMap;

use crate::model::{InstanceState, RuntimeSettings, SortMode, ViewMode};
use crate::topology::build_tree_groups;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveView {
    Overview,
    Detail,
}

#[derive(Debug, Clone)]
pub struct DisplayRow {
    pub key: String,
    pub alias_or_addr: String,
    pub address: String,
    pub node_type: String,
    pub cluster: String,
    pub mem_used: String,
    pub maxmem_pct: String,
    pub ops: String,
    pub lat_last: String,
    pub lat_max: String,
    pub status: String,
    pub stale: bool,
}

pub struct AppState {
    pub settings: RuntimeSettings,
    pub view_mode: ViewMode,
    pub sort_mode: SortMode,
    pub filter: String,
    pub is_filtering: bool,
    pub show_help: bool,
    pub active_view: ActiveView,
    pub selected_index: usize,
    pub detail_tab: usize,
    pub instances: HashMap<String, InstanceState>,
    pub should_quit: bool,
}

impl AppState {
    pub fn new(settings: RuntimeSettings) -> Self {
        Self {
            view_mode: settings.default_view,
            sort_mode: settings.default_sort,
            settings,
            filter: String::new(),
            is_filtering: false,
            show_help: false,
            active_view: ActiveView::Overview,
            selected_index: 0,
            detail_tab: 0,
            instances: HashMap::new(),
            should_quit: false,
        }
    }

    pub fn apply_update(&mut self, update: InstanceState) {
        self.instances.insert(update.key.clone(), update);
        self.clamp_selection();
    }

    pub fn selected_key(&self) -> Option<String> {
        self.visible_rows()
            .get(self.selected_index)
            .map(|row| row.key.clone())
    }

    pub fn move_selection(&mut self, delta: isize) {
        let len = self.visible_rows().len();
        if len == 0 {
            self.selected_index = 0;
            return;
        }

        let current = self.selected_index as isize;
        let next = (current + delta).clamp(0, (len - 1) as isize) as usize;
        self.selected_index = next;
    }

    pub fn clamp_selection(&mut self) {
        let len = self.visible_rows().len();
        if len == 0 {
            self.selected_index = 0;
        } else if self.selected_index >= len {
            self.selected_index = len - 1;
        }
    }

    pub fn visible_rows(&self) -> Vec<DisplayRow> {
        let mut nodes: Vec<&InstanceState> = self.instances.values().collect();
        nodes.retain(|node| self.matches_filter(node));

        match self.view_mode {
            ViewMode::Flat => {
                sort_instances(&mut nodes, self.sort_mode);
                nodes
                    .into_iter()
                    .map(|node| self.to_display_row(node, ""))
                    .collect()
            }
            ViewMode::Tree => self.build_tree_rows(nodes),
        }
    }

    fn build_tree_rows(&self, filtered_nodes: Vec<&InstanceState>) -> Vec<DisplayRow> {
        let mut filtered_map: HashMap<String, &InstanceState> = HashMap::new();
        for node in filtered_nodes {
            filtered_map.insert(node.key.clone(), node);
        }

        let mut out = Vec::new();
        for group in build_tree_groups(&self.instances) {
            let mut roots: Vec<&InstanceState> = group
                .roots
                .iter()
                .filter_map(|key| filtered_map.get(key))
                .copied()
                .collect();
            sort_instances(&mut roots, self.sort_mode);

            for (idx, root) in roots.iter().enumerate() {
                let is_last = idx + 1 == roots.len();
                let prefix = if is_last { "└─ " } else { "├─ " };
                out.push(self.to_display_row(root, prefix));

                let mut children: Vec<&InstanceState> = group
                    .children
                    .get(&root.key)
                    .map(|keys| {
                        keys.iter()
                            .filter_map(|key| filtered_map.get(key))
                            .copied()
                            .collect::<Vec<&InstanceState>>()
                    })
                    .unwrap_or_default();
                sort_instances(&mut children, self.sort_mode);

                for (child_idx, child) in children.iter().enumerate() {
                    let child_last = child_idx + 1 == children.len();
                    let branch = if is_last { "   " } else { "│  " };
                    let leaf = if child_last { "└─ " } else { "├─ " };
                    out.push(self.to_display_row(child, &format!("{branch}{leaf}")));
                }
            }
        }

        out
    }

    fn to_display_row(&self, node: &InstanceState, prefix: &str) -> DisplayRow {
        let alias = node
            .alias
            .clone()
            .unwrap_or_else(|| shorten_addr(&node.addr).to_string());
        let alias_or_addr = format!("{prefix}{alias}");

        DisplayRow {
            key: node.key.clone(),
            alias_or_addr,
            address: node.addr.clone(),
            node_type: node.kind.as_str().to_string(),
            cluster: node
                .cluster_id
                .clone()
                .unwrap_or_else(|| "Standalone".to_string()),
            mem_used: node
                .used_memory_bytes
                .map(human_bytes)
                .unwrap_or_else(|| "-".to_string()),
            maxmem_pct: node
                .maxmemory_percent()
                .map(|pct| format!("{pct:.1}%"))
                .unwrap_or_default(),
            ops: node
                .ops_per_sec
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".to_string()),
            lat_last: node
                .last_latency_ms
                .map(|v| format!("{v:.2}"))
                .unwrap_or_else(|| "-".to_string()),
            lat_max: format!("{:.2}", node.max_latency_ms),
            status: node.status.as_str().to_string(),
            stale: node.is_stale(self.settings.refresh_interval),
        }
    }

    fn matches_filter(&self, node: &InstanceState) -> bool {
        if self.filter.trim().is_empty() {
            return true;
        }
        let needle = self.filter.to_ascii_lowercase();
        node.alias
            .as_deref()
            .map(|s| s.to_ascii_lowercase().contains(&needle))
            .unwrap_or(false)
            || node.addr.to_ascii_lowercase().contains(&needle)
            || node
                .cluster_id
                .as_deref()
                .map(|s| s.to_ascii_lowercase().contains(&needle))
                .unwrap_or(false)
            || node
                .tags
                .iter()
                .any(|tag| tag.to_ascii_lowercase().contains(&needle))
    }
}

fn sort_instances(instances: &mut Vec<&InstanceState>, mode: SortMode) {
    match mode {
        SortMode::Address => instances.sort_by(|a, b| a.addr.cmp(&b.addr)),
        SortMode::Mem => instances.sort_by(|a, b| {
            b.used_memory_bytes
                .unwrap_or(0)
                .cmp(&a.used_memory_bytes.unwrap_or(0))
                .then_with(|| a.addr.cmp(&b.addr))
        }),
        SortMode::Ops => instances.sort_by(|a, b| {
            b.ops_per_sec
                .unwrap_or(0)
                .cmp(&a.ops_per_sec.unwrap_or(0))
                .then_with(|| a.addr.cmp(&b.addr))
        }),
        SortMode::Lat => instances.sort_by(|a, b| {
            b.last_latency_ms
                .unwrap_or(0.0)
                .partial_cmp(&a.last_latency_ms.unwrap_or(0.0))
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.addr.cmp(&b.addr))
        }),
        SortMode::Status => instances.sort_by(|a, b| {
            a.status
                .severity()
                .cmp(&b.status.severity())
                .then_with(|| a.addr.cmp(&b.addr))
        }),
    }
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut idx = 0;
    while value >= 1024.0 && idx + 1 < UNITS.len() {
        value /= 1024.0;
        idx += 1;
    }
    if idx == 0 {
        format!("{} {}", bytes, UNITS[idx])
    } else {
        format!("{value:.1} {}", UNITS[idx])
    }
}

fn shorten_addr(addr: &str) -> &str {
    addr.rsplit('/').next().unwrap_or(addr)
}
