use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use crate::model::{InstanceState, InstanceType, RuntimeSettings, SortMode, ViewMode};
use crate::target_addr::is_local_address;
use crate::topology::{TreeGroup, build_tree_groups};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveView {
    Overview,
    Detail,
    Help,
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
    pub previous_view: ActiveView,
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
            previous_view: ActiveView::Overview,
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

    pub fn open_help_view(&mut self) {
        if self.active_view != ActiveView::Help {
            self.previous_view = self.active_view;
        }
        self.active_view = ActiveView::Help;
    }

    pub fn close_help_view(&mut self) {
        self.active_view = self.previous_view;
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

    pub fn show_address_column(&self) -> bool {
        if self.instances.is_empty() {
            return true;
        }

        self.instances
            .values()
            .any(|instance| !is_local_address(&instance.addr))
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
            sort_tree_roots(&mut roots, self.sort_mode);
            let mut rendered = HashSet::new();

            for root in roots {
                rendered.insert(root.key.clone());
                out.push(self.to_display_row(root, ""));
                self.append_tree_children(
                    &mut out,
                    &filtered_map,
                    &group,
                    &root.key,
                    "  ",
                    &mut rendered,
                );
            }
        }

        out
    }

    fn append_tree_children(
        &self,
        out: &mut Vec<DisplayRow>,
        filtered_map: &HashMap<String, &InstanceState>,
        group: &TreeGroup,
        parent_key: &str,
        indent: &str,
        rendered: &mut HashSet<String>,
    ) {
        let mut children: Vec<&InstanceState> = group
            .children
            .get(parent_key)
            .map(|keys| {
                keys.iter()
                    .filter_map(|key| filtered_map.get(key))
                    .copied()
                    .collect::<Vec<&InstanceState>>()
            })
            .unwrap_or_default();
        sort_instances(&mut children, self.sort_mode);

        for (idx, child) in children.iter().enumerate() {
            if rendered.contains(&child.key) {
                continue;
            }
            rendered.insert(child.key.clone());

            let is_last = idx + 1 == children.len();
            let branch = if is_last { "└─ " } else { "├─ " };
            out.push(self.to_display_row(child, &format!("{indent}{branch}")));

            let next_indent = if is_last {
                format!("{indent}   ")
            } else {
                format!("{indent}│  ")
            };
            self.append_tree_children(out, filtered_map, group, &child.key, &next_indent, rendered);
        }
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
    instances.sort_by(|a, b| compare_instances(a, b, mode));
}

fn compare_instances(a: &InstanceState, b: &InstanceState, mode: SortMode) -> Ordering {
    match mode {
        SortMode::Address => a.addr.cmp(&b.addr),
        SortMode::Mem => b
            .used_memory_bytes
            .unwrap_or(0)
            .cmp(&a.used_memory_bytes.unwrap_or(0))
            .then_with(|| a.addr.cmp(&b.addr)),
        SortMode::Ops => b
            .ops_per_sec
            .unwrap_or(0)
            .cmp(&a.ops_per_sec.unwrap_or(0))
            .then_with(|| a.addr.cmp(&b.addr)),
        SortMode::Lat => b
            .last_latency_ms
            .unwrap_or(0.0)
            .partial_cmp(&a.last_latency_ms.unwrap_or(0.0))
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.addr.cmp(&b.addr)),
        SortMode::Status => a
            .status
            .severity()
            .cmp(&b.status.severity())
            .then_with(|| a.addr.cmp(&b.addr)),
    }
}

fn sort_tree_roots(instances: &mut Vec<&InstanceState>, mode: SortMode) {
    instances.sort_by(|a, b| {
        root_kind_rank(a.kind)
            .cmp(&root_kind_rank(b.kind))
            .then_with(|| compare_instances(a, b, mode))
    });
}

fn root_kind_rank(kind: InstanceType) -> u8 {
    match kind {
        InstanceType::Primary => 0,
        InstanceType::Cluster => 1,
        InstanceType::Standalone => 2,
        InstanceType::Replica => 3,
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

#[cfg(test)]
mod tests {
    use super::AppState;
    use crate::model::{InstanceState, InstanceType, RuntimeSettings, SortMode, ViewMode};
    use std::time::Duration;

    fn settings() -> RuntimeSettings {
        RuntimeSettings {
            refresh_interval: Duration::from_secs(1),
            connect_timeout: Duration::from_millis(300),
            command_timeout: Duration::from_millis(500),
            concurrency_limit: 4,
            default_view: ViewMode::Tree,
            default_sort: SortMode::Address,
        }
    }

    #[test]
    fn tree_view_places_replicas_below_primary() {
        let mut app = AppState::new(settings());

        let mut replica = InstanceState::new("replica".into(), "127.0.0.1:6380".into());
        replica.kind = InstanceType::Replica;
        replica.parent_addr = Some("127.0.0.1:6379".into());

        let mut primary = InstanceState::new("primary".into(), "127.0.0.1:6379".into());
        primary.kind = InstanceType::Primary;

        app.apply_update(replica);
        app.apply_update(primary);

        let rows = app.visible_rows();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].key, "primary");
        assert!(!rows[0].alias_or_addr.contains("└─"));
        assert_eq!(rows[1].key, "replica");
        assert!(rows[1].alias_or_addr.starts_with("  └─ "));
    }

    #[test]
    fn hides_address_column_when_all_instances_are_local() {
        let mut app = AppState::new(settings());
        app.apply_update(InstanceState::new("a".into(), "127.0.0.1:6379".into()));
        app.apply_update(InstanceState::new("b".into(), "localhost:6380".into()));
        assert!(!app.show_address_column());
    }

    #[test]
    fn keeps_address_column_when_any_instance_is_remote() {
        let mut app = AppState::new(settings());
        app.apply_update(InstanceState::new("a".into(), "127.0.0.1:6379".into()));
        app.apply_update(InstanceState::new("b".into(), "10.0.0.12:6380".into()));
        assert!(app.show_address_column());
    }
}
