use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};

use crate::model::{
    InstanceState, InstanceType, RuntimeSettings, SortDirection, SortMode, ViewMode,
};
use crate::target_addr::{canonical_host, strip_host};
use crate::topology::{TreeGroup, build_tree_groups};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveView {
    Overview,
    Detail,
    Help,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterPromptMode {
    Search,
    Filter,
}

impl FilterPromptMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::Search => "Search",
            Self::Filter => "Filter",
        }
    }
}

#[derive(Debug, Clone)]
pub struct DisplayRow {
    pub key: String,
    pub alias_or_addr: String,
    pub address: String,
    pub node_type: String,
    pub cluster: String,
    pub memory: String,
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
    pub sort_direction: SortDirection,
    pub is_sorting: bool,
    pub sort_picker_index: usize,
    pub filter: String,
    pub is_filtering: bool,
    pub filter_prompt_mode: FilterPromptMode,
    pub show_help: bool,
    pub active_view: ActiveView,
    pub previous_view: ActiveView,
    pub selected_index: usize,
    pub detail_tab: usize,
    pub force_show_host: bool,
    pub instances: HashMap<String, InstanceState>,
    pub should_quit: bool,
}

struct TreeRenderCtx<'a> {
    filtered_map: &'a HashMap<String, &'a InstanceState>,
    group: &'a TreeGroup,
    cluster_labels: &'a HashMap<String, String>,
}

impl AppState {
    pub fn new(settings: RuntimeSettings) -> Self {
        Self {
            view_mode: settings.default_view,
            sort_mode: settings.default_sort,
            sort_direction: default_sort_direction(settings.default_sort),
            is_sorting: false,
            sort_picker_index: 0,
            settings,
            filter: String::new(),
            is_filtering: false,
            filter_prompt_mode: FilterPromptMode::Filter,
            show_help: false,
            active_view: ActiveView::Overview,
            previous_view: ActiveView::Overview,
            selected_index: 0,
            detail_tab: 0,
            force_show_host: false,
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

    pub fn start_filter_input(&mut self, mode: FilterPromptMode, clear_existing: bool) {
        self.filter_prompt_mode = mode;
        if clear_existing {
            self.filter.clear();
        }
        self.is_filtering = true;
        self.clamp_selection();
    }

    pub fn visible_rows(&self) -> Vec<DisplayRow> {
        let mut nodes: Vec<&InstanceState> = self.instances.values().collect();
        nodes.retain(|node| self.matches_filter(node));
        let cluster_labels = self.cluster_labels();
        let should_omit_host = self.should_omit_host_in_rendering();

        match self.view_mode {
            ViewMode::Flat => {
                sort_instances(&mut nodes, self.sort_mode, self.sort_direction);
                nodes
                    .into_iter()
                    .map(|node| self.to_display_row(node, "", should_omit_host, &cluster_labels))
                    .collect()
            }
            ViewMode::Tree => self.build_tree_rows(nodes, should_omit_host, &cluster_labels),
        }
    }

    pub fn show_address_column(&self) -> bool {
        !self.should_omit_host_in_rendering()
    }

    pub fn toggle_host_rendering(&mut self) {
        self.force_show_host = !self.force_show_host;
    }

    pub fn sortable_columns(&self) -> Vec<SortMode> {
        let mut columns = vec![SortMode::Alias];
        if self.show_address_column() {
            columns.push(SortMode::Address);
        }
        columns.extend([
            SortMode::Type,
            SortMode::Cluster,
            SortMode::Mem,
            SortMode::Ops,
            SortMode::Lat,
            SortMode::LatMax,
            SortMode::Status,
        ]);
        columns
    }

    pub fn open_sort_picker(&mut self) {
        let columns = self.sortable_columns();
        self.sort_picker_index = columns
            .iter()
            .position(|mode| *mode == self.sort_mode)
            .unwrap_or(0);
        self.is_sorting = true;
    }

    pub fn close_sort_picker(&mut self) {
        self.is_sorting = false;
    }

    pub fn move_sort_picker_selection(&mut self, delta: isize) {
        let columns = self.sortable_columns();
        if columns.is_empty() {
            self.sort_picker_index = 0;
            return;
        }
        let current = self.sort_picker_index as isize;
        let next = (current + delta).clamp(0, (columns.len() - 1) as isize) as usize;
        self.sort_picker_index = next;
    }

    pub fn apply_sort_picker_selection(&mut self) {
        let columns = self.sortable_columns();
        let Some(chosen_mode) = columns.get(self.sort_picker_index).copied() else {
            self.is_sorting = false;
            return;
        };
        if self.sort_mode == chosen_mode {
            self.sort_direction = self.sort_direction.toggle();
        } else {
            self.sort_mode = chosen_mode;
            self.sort_direction = default_sort_direction(chosen_mode);
        }
        self.is_sorting = false;
        self.clamp_selection();
    }

    pub fn cycle_sort_mode(&mut self) {
        let columns = self.sortable_columns();
        let current_idx = columns
            .iter()
            .position(|mode| *mode == self.sort_mode)
            .unwrap_or(0);
        let next_idx = (current_idx + 1) % columns.len();
        self.sort_mode = columns[next_idx];
        self.sort_direction = default_sort_direction(self.sort_mode);
        self.clamp_selection();
    }

    fn build_tree_rows(
        &self,
        filtered_nodes: Vec<&InstanceState>,
        should_omit_host: bool,
        cluster_labels: &HashMap<String, String>,
    ) -> Vec<DisplayRow> {
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
            sort_tree_roots(&mut roots, self.sort_mode, self.sort_direction);
            let mut rendered = HashSet::new();
            let ctx = TreeRenderCtx {
                filtered_map: &filtered_map,
                group: &group,
                cluster_labels,
            };

            for root in roots {
                rendered.insert(root.key.clone());
                out.push(self.to_display_row(root, "", should_omit_host, cluster_labels));
                self.append_tree_children(
                    &mut out,
                    &ctx,
                    &root.key,
                    "",
                    should_omit_host,
                    &mut rendered,
                );
            }
        }

        out
    }

    fn append_tree_children(
        &self,
        out: &mut Vec<DisplayRow>,
        ctx: &TreeRenderCtx<'_>,
        parent_key: &str,
        indent: &str,
        should_omit_host: bool,
        rendered: &mut HashSet<String>,
    ) {
        let mut children: Vec<&InstanceState> = ctx
            .group
            .children
            .get(parent_key)
            .map(|keys| {
                keys.iter()
                    .filter_map(|key| ctx.filtered_map.get(key))
                    .copied()
                    .collect::<Vec<&InstanceState>>()
            })
            .unwrap_or_default();
        sort_instances(&mut children, self.sort_mode, self.sort_direction);

        for (idx, child) in children.iter().enumerate() {
            if rendered.contains(&child.key) {
                continue;
            }
            rendered.insert(child.key.clone());

            let is_last = idx + 1 == children.len();
            let branch = if is_last { "└─ " } else { "├─ " };
            out.push(self.to_display_row(
                child,
                &format!("{indent}{branch}"),
                should_omit_host,
                ctx.cluster_labels,
            ));

            let next_indent = if is_last {
                format!("{indent}   ")
            } else {
                format!("{indent}│  ")
            };
            self.append_tree_children(
                out,
                ctx,
                &child.key,
                &next_indent,
                should_omit_host,
                rendered,
            );
        }
    }

    fn to_display_row(
        &self,
        node: &InstanceState,
        prefix: &str,
        should_omit_host: bool,
        cluster_labels: &HashMap<String, String>,
    ) -> DisplayRow {
        let alias = node
            .alias
            .clone()
            .unwrap_or_else(|| default_row_label(&node.addr, should_omit_host));
        let alias_or_addr = format!("{prefix}{alias}");
        let raw_cluster = node
            .cluster_id
            .clone()
            .unwrap_or_else(|| "Standalone".to_string());

        DisplayRow {
            key: node.key.clone(),
            alias_or_addr,
            address: node.addr.clone(),
            node_type: compact_instance_type(node.kind).to_string(),
            cluster: cluster_labels
                .get(&raw_cluster)
                .cloned()
                .unwrap_or_else(|| "?".to_string()),
            memory: format_memory_usage(node.used_memory_bytes, node.maxmemory_bytes),
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

    fn cluster_labels(&self) -> HashMap<String, String> {
        let mut ordered = BTreeMap::<String, ()>::new();
        for instance in self.instances.values() {
            let raw_cluster = instance
                .cluster_id
                .clone()
                .unwrap_or_else(|| "Standalone".to_string());
            ordered.insert(raw_cluster, ());
        }

        ordered
            .keys()
            .enumerate()
            .map(|(idx, raw_cluster)| (raw_cluster.clone(), (idx + 1).to_string()))
            .collect()
    }

    pub fn should_omit_host_in_rendering(&self) -> bool {
        if self.force_show_host || self.instances.is_empty() {
            return false;
        }

        let mut hosts = self
            .instances
            .values()
            .map(|instance| canonical_host(&instance.addr));
        let Some(Some(first)) = hosts.next() else {
            return false;
        };
        hosts.all(|host| host.as_deref() == Some(first.as_str()))
    }
}

fn sort_instances(instances: &mut Vec<&InstanceState>, mode: SortMode, direction: SortDirection) {
    instances.sort_by(|a, b| compare_instances(a, b, mode, direction));
}

fn compare_instances(
    a: &InstanceState,
    b: &InstanceState,
    mode: SortMode,
    direction: SortDirection,
) -> Ordering {
    let ordering = match mode {
        SortMode::Alias => instance_sort_label(a).cmp(&instance_sort_label(b)),
        SortMode::Address => a.addr.cmp(&b.addr),
        SortMode::Type => compact_instance_type(a.kind).cmp(compact_instance_type(b.kind)),
        SortMode::Cluster => a.cluster_id.cmp(&b.cluster_id),
        SortMode::Mem => a
            .used_memory_bytes
            .unwrap_or(0)
            .cmp(&b.used_memory_bytes.unwrap_or(0)),
        SortMode::Ops => a.ops_per_sec.unwrap_or(0).cmp(&b.ops_per_sec.unwrap_or(0)),
        SortMode::Lat => a
            .last_latency_ms
            .unwrap_or(0.0)
            .partial_cmp(&b.last_latency_ms.unwrap_or(0.0))
            .unwrap_or(Ordering::Equal),
        SortMode::LatMax => a
            .max_latency_ms
            .partial_cmp(&b.max_latency_ms)
            .unwrap_or(Ordering::Equal),
        SortMode::Status => a.status.severity().cmp(&b.status.severity()),
    };
    apply_direction(ordering, direction).then_with(|| a.addr.cmp(&b.addr))
}

fn sort_tree_roots(instances: &mut Vec<&InstanceState>, mode: SortMode, direction: SortDirection) {
    instances.sort_by(|a, b| {
        root_kind_rank(a.kind)
            .cmp(&root_kind_rank(b.kind))
            .then_with(|| compare_instances(a, b, mode, direction))
    });
}

fn apply_direction(ordering: Ordering, direction: SortDirection) -> Ordering {
    match direction {
        SortDirection::Asc => ordering,
        SortDirection::Desc => ordering.reverse(),
    }
}

fn default_sort_direction(mode: SortMode) -> SortDirection {
    match mode {
        SortMode::Alias
        | SortMode::Address
        | SortMode::Type
        | SortMode::Cluster
        | SortMode::Status => SortDirection::Asc,
        SortMode::Mem | SortMode::Ops | SortMode::Lat | SortMode::LatMax => SortDirection::Desc,
    }
}

fn instance_sort_label(instance: &InstanceState) -> String {
    instance
        .alias
        .clone()
        .unwrap_or_else(|| instance.addr.clone())
        .to_ascii_lowercase()
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

fn format_memory_usage(used_bytes: Option<u64>, max_bytes: Option<u64>) -> String {
    let Some(used) = used_bytes else {
        return "-".to_string();
    };
    let used_human = human_bytes(used);
    match max_bytes {
        Some(max) if max > 0 => format!("{used_human}/{}", human_bytes(max)),
        _ => used_human,
    }
}

fn shorten_addr(addr: &str) -> &str {
    addr.rsplit('/').next().unwrap_or(addr)
}

fn default_row_label(addr: &str, omit_host: bool) -> String {
    if omit_host && let Some(without_host) = strip_host(addr) {
        return without_host;
    }
    shorten_addr(addr).to_string()
}

fn compact_instance_type(kind: InstanceType) -> &'static str {
    match kind {
        InstanceType::Standalone => "STD",
        InstanceType::Cluster => "CLU",
        InstanceType::Primary => "PRI",
        InstanceType::Replica => "REP",
    }
}

#[cfg(test)]
mod tests {
    use super::{AppState, FilterPromptMode, format_memory_usage};
    use crate::model::{
        InstanceState, InstanceType, RuntimeSettings, SortDirection, SortMode, ViewMode,
    };
    use std::collections::HashMap;
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
        assert!(rows[1].alias_or_addr.starts_with("└─ "));
        assert_eq!(rows[0].node_type, "PRI");
        assert_eq!(rows[1].node_type, "REP");
    }

    #[test]
    fn maps_raw_cluster_ids_to_compact_logical_ids() {
        let mut app = AppState::new(settings());

        let mut a = InstanceState::new("a".into(), "127.0.0.1:6379".into());
        a.cluster_id = Some("def959b8".into());
        let mut b = InstanceState::new("b".into(), "127.0.0.1:6380".into());
        b.cluster_id = Some("af8898a8".into());
        let mut c = InstanceState::new("c".into(), "127.0.0.1:6381".into());
        c.cluster_id = Some("def959b8".into());

        app.apply_update(a);
        app.apply_update(b);
        app.apply_update(c);

        let rows = app.visible_rows();
        let cluster_by_key: HashMap<String, String> =
            rows.into_iter().map(|row| (row.key, row.cluster)).collect();

        assert_eq!(cluster_by_key.get("a"), Some(&"2".to_string()));
        assert_eq!(cluster_by_key.get("b"), Some(&"1".to_string()));
        assert_eq!(cluster_by_key.get("c"), Some(&"2".to_string()));
    }

    #[test]
    fn hides_address_column_when_all_instance_hosts_are_equal() {
        let mut app = AppState::new(settings());
        app.apply_update(InstanceState::new("a".into(), "10.0.0.12:6379".into()));
        app.apply_update(InstanceState::new("b".into(), "10.0.0.12:6380".into()));
        assert!(!app.show_address_column());
    }

    #[test]
    fn keeps_address_column_when_instance_hosts_are_mixed() {
        let mut app = AppState::new(settings());
        app.apply_update(InstanceState::new("a".into(), "127.0.0.1:6379".into()));
        app.apply_update(InstanceState::new("b".into(), "10.0.0.12:6380".into()));
        assert!(app.show_address_column());
    }

    #[test]
    fn default_label_omits_host_when_all_hosts_match() {
        let mut app = AppState::new(settings());
        app.apply_update(InstanceState::new("a".into(), "127.0.0.1:6379".into()));
        app.apply_update(InstanceState::new("b".into(), "127.0.0.1:6380".into()));

        let rows = app.visible_rows();
        assert_eq!(rows[0].alias_or_addr, "6379");
        assert_eq!(rows[1].alias_or_addr, "6380");
    }

    #[test]
    fn force_show_host_override_keeps_address_column_visible() {
        let mut app = AppState::new(settings());
        app.apply_update(InstanceState::new("a".into(), "127.0.0.1:6379".into()));
        app.apply_update(InstanceState::new("b".into(), "127.0.0.1:6380".into()));
        app.toggle_host_rendering();

        assert!(app.show_address_column());
        let rows = app.visible_rows();
        assert_eq!(rows[0].alias_or_addr, "127.0.0.1:6379");
    }

    #[test]
    fn memory_display_uses_single_column_with_optional_max() {
        assert_eq!(
            format_memory_usage(Some(1024 * 1024 * 2), Some(1024 * 1024 * 4)),
            "2.0 MiB/4.0 MiB"
        );
        assert_eq!(format_memory_usage(Some(1024 * 2), Some(0)), "2.0 KiB");
        assert_eq!(format_memory_usage(Some(1024 * 2), None), "2.0 KiB");
        assert_eq!(format_memory_usage(None, Some(1024 * 2)), "-");
    }

    #[test]
    fn start_filter_input_sets_mode_and_clear_behavior() {
        let mut app = AppState::new(settings());
        app.filter = "redis".to_string();

        app.start_filter_input(FilterPromptMode::Search, false);
        assert!(app.is_filtering);
        assert_eq!(app.filter_prompt_mode, FilterPromptMode::Search);
        assert_eq!(app.filter, "redis");

        app.start_filter_input(FilterPromptMode::Filter, true);
        assert!(app.is_filtering);
        assert_eq!(app.filter_prompt_mode, FilterPromptMode::Filter);
        assert!(app.filter.is_empty());
    }

    #[test]
    fn sort_picker_uses_only_visible_columns() {
        let mut app = AppState::new(settings());
        app.apply_update(InstanceState::new("a".into(), "127.0.0.1:6379".into()));
        app.apply_update(InstanceState::new("b".into(), "127.0.0.1:6380".into()));

        let columns = app.sortable_columns();
        assert!(columns.contains(&SortMode::Alias));
        assert!(!columns.contains(&SortMode::Address));
    }

    #[test]
    fn applying_same_sort_column_toggles_direction() {
        let mut app = AppState::new(settings());
        app.sort_mode = SortMode::Status;
        app.sort_direction = SortDirection::Asc;
        app.open_sort_picker();

        app.apply_sort_picker_selection();

        assert_eq!(app.sort_mode, SortMode::Status);
        assert_eq!(app.sort_direction, SortDirection::Desc);
    }
}
