use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};

use crate::column::{Emphasis, RenderCtx, SortCtx, SortKey};
use crate::model::{InstanceState, InstanceType, RuntimeSettings, SortDirection, ViewMode};
use crate::registry::ColumnRegistry;
use crate::target_addr::canonical_host;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverviewModal {
    None,
    SortPicker,
    ColumnPicker,
}

impl FilterPromptMode {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Search => "Search",
            Self::Filter => "Filter",
        }
    }
}

#[derive(Debug, Clone)]
pub struct DisplayRow {
    pub key: String,
    pub tree_prefix: String,
    pub stale: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommandstatsViewState {
    pub filter: String,
    pub is_filtering: bool,
    pub scroll_offset: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BigkeysViewState {
    pub filter: String,
    pub is_filtering: bool,
    pub scroll_offset: usize,
}

#[allow(clippy::struct_excessive_bools)]
pub struct AppState {
    pub settings: RuntimeSettings,
    pub view_mode: ViewMode,
    pub sort_by: String,
    pub sort_direction: SortDirection,
    pub overview_modal: OverviewModal,
    pub sort_picker_index: usize,
    pub column_picker_index: usize,
    pub column_picker_reorder_mode: bool,
    pub filter: String,
    pub is_filtering: bool,
    pub filter_prompt_mode: FilterPromptMode,
    pub show_help: bool,
    pub active_view: ActiveView,
    pub previous_view: ActiveView,
    pub selected_index: usize,
    pub detail_tab: usize,
    pub commandstats_view: CommandstatsViewState,
    pub bigkeys_view: BigkeysViewState,
    pub force_show_host: bool,
    pub instances: HashMap<String, InstanceState>,
    pub should_quit: bool,
    pub column_registry: ColumnRegistry,
    pub runtime_overview_column_order: Vec<String>,
    pub runtime_visible_overview: Vec<String>,
}

struct TreeRenderCtx<'a> {
    filtered_map: &'a HashMap<String, &'a InstanceState>,
    group: &'a TreeGroup,
    cluster_labels: &'a HashMap<String, String>,
}

impl AppState {
    pub fn new(settings: RuntimeSettings, column_registry: ColumnRegistry) -> Self {
        Self {
            view_mode: settings.default_view,
            sort_by: column_registry.default_sort_by.clone(),
            sort_direction: column_registry.default_sort_direction,
            overview_modal: OverviewModal::None,
            sort_picker_index: 0,
            column_picker_index: 0,
            column_picker_reorder_mode: false,
            settings,
            filter: String::new(),
            is_filtering: false,
            filter_prompt_mode: FilterPromptMode::Filter,
            show_help: false,
            active_view: ActiveView::Overview,
            previous_view: ActiveView::Overview,
            selected_index: 0,
            detail_tab: 0,
            commandstats_view: CommandstatsViewState::default(),
            bigkeys_view: BigkeysViewState::default(),
            force_show_host: false,
            instances: HashMap::new(),
            should_quit: false,
            runtime_overview_column_order: column_registry.available_overview_columns(),
            runtime_visible_overview: column_registry.default_visible_overview_columns(),
            column_registry,
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

        let current = isize::try_from(self.selected_index).unwrap_or(isize::MAX);
        let max_index = isize::try_from(len - 1).unwrap_or(isize::MAX);
        let next = (current + delta).clamp(0, max_index);
        let next = usize::try_from(next).unwrap_or(0);
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

    pub const fn close_help_view(&mut self) {
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

    pub fn start_commandstats_filter_input(&mut self, clear_existing: bool) {
        if clear_existing {
            self.commandstats_view.filter.clear();
        }
        self.commandstats_view.is_filtering = true;
        self.commandstats_view.scroll_offset = 0;
    }

    pub fn visible_commandstats<'a>(
        &self,
        stats: &'a [crate::model::CommandStat],
    ) -> Vec<&'a crate::model::CommandStat> {
        let needle = self.commandstats_view.filter.trim().to_ascii_lowercase();
        let mut filtered = stats
            .iter()
            .filter(|stat| needle.is_empty() || stat.command.to_ascii_lowercase().contains(&needle))
            .collect::<Vec<_>>();
        filtered.sort_by(|left, right| {
            right
                .calls
                .cmp(&left.calls)
                .then_with(|| left.command.cmp(&right.command))
        });
        filtered
    }

    pub fn clamp_commandstats_scroll(
        &mut self,
        stats: &[crate::model::CommandStat],
        page_len: usize,
    ) {
        let visible_len = self.visible_commandstats(stats).len();
        let max_offset = visible_len.saturating_sub(page_len.max(1));
        if self.commandstats_view.scroll_offset > max_offset {
            self.commandstats_view.scroll_offset = max_offset;
        }
    }

    pub fn move_commandstats_scroll(
        &mut self,
        delta: isize,
        stats: &[crate::model::CommandStat],
        page_len: usize,
    ) {
        let visible_len = self.visible_commandstats(stats).len();
        let max_offset = visible_len.saturating_sub(page_len.max(1));
        let current = isize::try_from(self.commandstats_view.scroll_offset).unwrap_or(isize::MAX);
        let max_index = isize::try_from(max_offset).unwrap_or(isize::MAX);
        let next = (current + delta).clamp(0, max_index);
        self.commandstats_view.scroll_offset = usize::try_from(next).unwrap_or(0);
    }

    pub fn clamp_bigkeys_scroll(&mut self, rows_len: usize, page_len: usize) {
        let max_offset = rows_len.saturating_sub(page_len.max(1));
        if self.bigkeys_view.scroll_offset > max_offset {
            self.bigkeys_view.scroll_offset = max_offset;
        }
    }

    pub fn move_bigkeys_scroll(&mut self, delta: isize, rows_len: usize, page_len: usize) {
        let max_offset = rows_len.saturating_sub(page_len.max(1));
        let current = isize::try_from(self.bigkeys_view.scroll_offset).unwrap_or(isize::MAX);
        let max_index = isize::try_from(max_offset).unwrap_or(isize::MAX);
        let next = (current + delta).clamp(0, max_index);
        self.bigkeys_view.scroll_offset = usize::try_from(next).unwrap_or(0);
    }

    pub fn start_bigkeys_filter_input(&mut self, clear_existing: bool) {
        if clear_existing {
            self.bigkeys_view.filter.clear();
        }
        self.bigkeys_view.is_filtering = true;
        self.bigkeys_view.scroll_offset = 0;
    }

    pub fn visible_bigkeys<'a>(
        &self,
        entries: &'a [crate::model::BigkeyEntry],
    ) -> Vec<&'a crate::model::BigkeyEntry> {
        let needle = self.bigkeys_view.filter.trim().to_ascii_lowercase();
        entries
            .iter()
            .filter(|entry| {
                needle.is_empty()
                    || entry.key.to_ascii_lowercase().contains(&needle)
                    || entry.key_type.to_ascii_lowercase().contains(&needle)
            })
            .collect()
    }

    pub fn visible_rows(&self) -> Vec<DisplayRow> {
        let mut nodes: Vec<&InstanceState> = self.instances.values().collect();
        nodes.retain(|node| self.matches_filter(node));
        let cluster_labels = self.cluster_labels();
        let should_omit_host = self.should_omit_host_in_rendering();

        match self.view_mode {
            ViewMode::Flat => {
                sort_instances(
                    &mut nodes,
                    &self.sort_by,
                    self.sort_direction,
                    &cluster_labels,
                    should_omit_host,
                    &self.column_registry,
                );
                nodes
                    .into_iter()
                    .map(|node| self.to_display_row(node, ""))
                    .collect()
            }
            ViewMode::Tree => self.build_tree_rows(nodes, should_omit_host, &cluster_labels),
        }
    }

    pub fn visible_column_keys(&self) -> Vec<String> {
        self.runtime_overview_column_order
            .iter()
            .filter(|key| {
                self.runtime_visible_overview
                    .iter()
                    .any(|visible| visible == *key)
            })
            .filter(|key| self.column_registry.column(key).is_some())
            .filter(|key| self.show_address_column() || key.as_str() != "addr")
            .cloned()
            .collect()
    }

    pub fn show_address_column(&self) -> bool {
        !self.should_omit_host_in_rendering()
    }

    pub const fn toggle_host_rendering(&mut self) {
        self.force_show_host = !self.force_show_host;
    }

    pub fn sortable_columns(&self) -> Vec<String> {
        self.visible_column_keys()
    }

    pub fn available_overview_columns(&self) -> Vec<String> {
        self.runtime_overview_column_order
            .iter()
            .filter(|key| self.column_registry.column(key).is_some())
            .cloned()
            .collect()
    }

    pub fn sort_label(&self) -> String {
        self.column_registry.column(&self.sort_by).map_or_else(
            || self.sort_by.clone(),
            |column| column.header().to_string(),
        )
    }

    pub fn open_sort_picker(&mut self) {
        let columns = self.sortable_columns();
        self.sort_picker_index = columns
            .iter()
            .position(|key| *key == self.sort_by)
            .unwrap_or(0);
        self.overview_modal = OverviewModal::SortPicker;
    }

    pub fn open_column_picker(&mut self) {
        let columns = self.available_overview_columns();
        self.column_picker_index = columns
            .iter()
            .position(|key| {
                self.runtime_visible_overview
                    .iter()
                    .any(|visible| visible == key)
            })
            .unwrap_or(0);
        self.column_picker_reorder_mode = false;
        self.overview_modal = OverviewModal::ColumnPicker;
    }

    pub const fn close_overview_modal(&mut self) {
        self.column_picker_reorder_mode = false;
        self.overview_modal = OverviewModal::None;
    }

    pub fn move_sort_picker_selection(&mut self, delta: isize) {
        let columns = self.sortable_columns();
        if columns.is_empty() {
            self.sort_picker_index = 0;
            return;
        }
        let current = isize::try_from(self.sort_picker_index).unwrap_or(isize::MAX);
        let max_index = isize::try_from(columns.len() - 1).unwrap_or(isize::MAX);
        let next = (current + delta).clamp(0, max_index);
        let next = usize::try_from(next).unwrap_or(0);
        self.sort_picker_index = next;
    }

    pub fn apply_sort_picker_selection(&mut self) {
        let columns = self.sortable_columns();
        let Some(chosen_key) = columns.get(self.sort_picker_index).cloned() else {
            self.overview_modal = OverviewModal::None;
            return;
        };
        if self.sort_by == chosen_key {
            self.sort_direction = self.sort_direction.toggle();
        } else {
            self.sort_by = chosen_key;
            self.sort_direction = default_sort_direction_for_column(&self.sort_by);
        }
        self.overview_modal = OverviewModal::None;
        self.clamp_selection();
    }

    pub fn move_column_picker_selection(&mut self, delta: isize) {
        let columns = self.available_overview_columns();
        if columns.is_empty() {
            self.column_picker_index = 0;
            return;
        }
        let current = isize::try_from(self.column_picker_index).unwrap_or(isize::MAX);
        let max_index = isize::try_from(columns.len() - 1).unwrap_or(isize::MAX);
        let next = (current + delta).clamp(0, max_index);
        self.column_picker_index = usize::try_from(next).unwrap_or(0);
    }

    pub fn set_column_picker_reorder_mode(&mut self, enabled: bool) {
        self.column_picker_reorder_mode = enabled && self.is_column_picker_open();
    }

    pub fn move_selected_column(&mut self, delta: isize) {
        let columns = self.available_overview_columns();
        let Some(chosen_key) = columns.get(self.column_picker_index).cloned() else {
            return;
        };
        let Some(chosen_order_idx) = self
            .runtime_overview_column_order
            .iter()
            .position(|key| key == &chosen_key)
        else {
            return;
        };

        let current = isize::try_from(chosen_order_idx).unwrap_or(isize::MAX);
        let max_index = isize::try_from(self.runtime_overview_column_order.len().saturating_sub(1))
            .unwrap_or(0);
        let next = (current + delta).clamp(0, max_index);
        let next_order_idx = usize::try_from(next).unwrap_or(chosen_order_idx);
        if next_order_idx == chosen_order_idx {
            return;
        }

        self.runtime_overview_column_order
            .swap(chosen_order_idx, next_order_idx);
        self.column_picker_index = next_order_idx;
    }

    pub fn toggle_selected_column_visibility(&mut self) {
        let columns = self.available_overview_columns();
        let Some(chosen_key) = columns.get(self.column_picker_index).cloned() else {
            return;
        };

        if self
            .runtime_visible_overview
            .iter()
            .any(|key| key == &chosen_key)
        {
            let next_visible = self
                .runtime_visible_overview
                .iter()
                .filter(|key| key.as_str() != chosen_key)
                .filter(|key| self.column_registry.column(key).is_some())
                .filter(|key| self.show_address_column() || key.as_str() != "addr")
                .count();
            if next_visible == 0 {
                return;
            }
            self.runtime_visible_overview
                .retain(|key| key != &chosen_key);
            self.ensure_sort_column_visible();
            self.clamp_selection();
            return;
        }

        self.runtime_visible_overview.push(chosen_key);
        self.normalize_runtime_visible_columns();
        self.ensure_sort_column_visible();
        self.clamp_selection();
    }

    pub fn cycle_sort_mode(&mut self) {
        let columns = self.sortable_columns();
        if columns.is_empty() {
            return;
        }
        let current_idx = columns
            .iter()
            .position(|key| *key == self.sort_by)
            .unwrap_or(0);
        let next_idx = (current_idx + 1) % columns.len();
        self.sort_by.clone_from(&columns[next_idx]);
        self.sort_direction = default_sort_direction_for_column(&self.sort_by);
        self.clamp_selection();
    }

    pub fn is_sort_picker_open(&self) -> bool {
        self.overview_modal == OverviewModal::SortPicker
    }

    pub fn is_column_picker_open(&self) -> bool {
        self.overview_modal == OverviewModal::ColumnPicker
    }

    pub fn is_column_visible(&self, column_key: &str) -> bool {
        self.runtime_visible_overview
            .iter()
            .any(|key| key == column_key)
    }

    pub fn render_cell(&self, row: &DisplayRow, column_key: &str) -> Option<String> {
        let node = self.instances.get(&row.key)?;
        let column = self.column_registry.column(column_key)?;
        let cluster_labels = self.cluster_labels();
        let ctx = self.render_ctx(row, node, &cluster_labels);
        Some(column.render_cell(&ctx).text)
    }

    pub fn emphasized_rows_by_column(&self, rows: &[DisplayRow]) -> HashMap<String, String> {
        let cluster_labels = self.cluster_labels();
        let mut emphasized = HashMap::new();

        for column_key in self.visible_column_keys() {
            let Some(column) = self.column_registry.column(&column_key) else {
                continue;
            };
            let Some(rule) = column.emphasis() else {
                continue;
            };

            let winner = rows
                .iter()
                .filter_map(|row| {
                    let node = self.instances.get(&row.key)?;
                    let sort_ctx = self.sort_ctx(node, &cluster_labels);
                    let sort_key = column.sort_key(&sort_ctx);
                    if matches!(sort_key, SortKey::Null) {
                        None
                    } else {
                        Some((row.key.as_str(), sort_key))
                    }
                })
                .reduce(|best, candidate| {
                    let ordering = candidate.1.compare(&best.1);
                    let take_candidate = match rule {
                        Emphasis::Max => ordering.is_gt(),
                        Emphasis::Min => ordering.is_lt(),
                    };
                    if take_candidate { candidate } else { best }
                });

            if let Some((key, _)) = winner {
                emphasized.insert(column_key, key.to_string());
            }
        }

        emphasized
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
            sort_tree_roots(
                &mut roots,
                &self.sort_by,
                self.sort_direction,
                cluster_labels,
                should_omit_host,
                &self.column_registry,
            );
            let mut rendered = HashSet::new();
            let ctx = TreeRenderCtx {
                filtered_map: &filtered_map,
                group: &group,
                cluster_labels,
            };

            for root in roots {
                rendered.insert(root.key.clone());
                out.push(self.to_display_row(root, ""));
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
        sort_instances(
            &mut children,
            &self.sort_by,
            self.sort_direction,
            ctx.cluster_labels,
            should_omit_host,
            &self.column_registry,
        );

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

    fn to_display_row(&self, node: &InstanceState, prefix: &str) -> DisplayRow {
        DisplayRow {
            key: node.key.clone(),
            tree_prefix: prefix.to_string(),
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
            .is_some_and(|s| s.to_ascii_lowercase().contains(&needle))
            || node.addr.to_ascii_lowercase().contains(&needle)
            || node
                .cluster_id
                .as_deref()
                .is_some_and(|s| s.to_ascii_lowercase().contains(&needle))
            || node
                .tags
                .iter()
                .any(|tag| tag.to_ascii_lowercase().contains(&needle))
    }

    pub(crate) fn cluster_labels(&self) -> HashMap<String, String> {
        let mut ordered = BTreeSet::<String>::new();
        for instance in self.instances.values() {
            let raw_cluster = instance
                .cluster_id
                .clone()
                .unwrap_or_else(|| "Standalone".to_string());
            ordered.insert(raw_cluster);
        }

        ordered
            .into_iter()
            .enumerate()
            .map(|(idx, raw_cluster)| (raw_cluster, (idx + 1).to_string()))
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

    fn render_ctx<'a>(
        &'a self,
        row: &'a DisplayRow,
        node: &'a InstanceState,
        cluster_labels: &'a HashMap<String, String>,
    ) -> RenderCtx<'a> {
        let raw_cluster = node
            .cluster_id
            .clone()
            .unwrap_or_else(|| "Standalone".to_string());
        let cluster_label = cluster_labels.get(&raw_cluster).map(String::as_str);
        RenderCtx {
            snap: node,
            omit_host: self.should_omit_host_in_rendering(),
            tree_prefix: &row.tree_prefix,
            cluster_label,
        }
    }

    fn sort_ctx<'a>(
        &'a self,
        node: &'a InstanceState,
        cluster_labels: &'a HashMap<String, String>,
    ) -> SortCtx<'a> {
        let raw_cluster = node
            .cluster_id
            .clone()
            .unwrap_or_else(|| "Standalone".to_string());
        SortCtx {
            snap: node,
            omit_host: self.should_omit_host_in_rendering(),
            cluster_label: cluster_labels.get(&raw_cluster).map(String::as_str),
        }
    }

    fn normalize_runtime_visible_columns(&mut self) {
        let registry_columns = self.column_registry.available_overview_columns();
        let mut ordered = Vec::with_capacity(registry_columns.len());
        for key in &self.runtime_overview_column_order {
            if registry_columns.iter().any(|candidate| candidate == key)
                && !ordered.iter().any(|existing| existing == key)
            {
                ordered.push(key.clone());
            }
        }
        for key in &registry_columns {
            if !ordered.iter().any(|existing| existing == key) {
                ordered.push(key.clone());
            }
        }
        self.runtime_overview_column_order = ordered;

        let mut deduped = Vec::with_capacity(self.runtime_visible_overview.len());
        for key in &self.runtime_visible_overview {
            if self
                .runtime_overview_column_order
                .iter()
                .any(|candidate| candidate == key)
                && !deduped.iter().any(|existing| existing == key)
            {
                deduped.push(key.clone());
            }
        }
        self.runtime_visible_overview = deduped;
    }

    fn ensure_sort_column_visible(&mut self) {
        if self
            .visible_column_keys()
            .iter()
            .any(|key| key == &self.sort_by)
        {
            return;
        }

        if let Some(next_sort) = self.visible_column_keys().into_iter().next() {
            self.sort_by = next_sort;
            self.sort_direction = default_sort_direction_for_column(&self.sort_by);
        }
    }
}

fn sort_instances(
    instances: &mut Vec<&InstanceState>,
    sort_by: &str,
    direction: SortDirection,
    cluster_labels: &HashMap<String, String>,
    omit_host: bool,
    registry: &ColumnRegistry,
) {
    instances.sort_by(|a, b| {
        compare_instances(
            a,
            b,
            sort_by,
            direction,
            cluster_labels,
            omit_host,
            registry,
        )
    });
}

fn compare_instances(
    a: &InstanceState,
    b: &InstanceState,
    sort_by: &str,
    direction: SortDirection,
    cluster_labels: &HashMap<String, String>,
    omit_host: bool,
    registry: &ColumnRegistry,
) -> Ordering {
    let ordering = registry.column(sort_by).map_or_else(
        || a.addr.cmp(&b.addr),
        |column| {
            let a_cluster = a
                .cluster_id
                .clone()
                .unwrap_or_else(|| "Standalone".to_string());
            let b_cluster = b
                .cluster_id
                .clone()
                .unwrap_or_else(|| "Standalone".to_string());
            let a_ctx = SortCtx {
                snap: a,
                omit_host,
                cluster_label: cluster_labels.get(&a_cluster).map(String::as_str),
            };
            let b_ctx = SortCtx {
                snap: b,
                omit_host,
                cluster_label: cluster_labels.get(&b_cluster).map(String::as_str),
            };
            column.sort_key(&a_ctx).compare(&column.sort_key(&b_ctx))
        },
    );

    apply_direction(ordering, direction).then_with(|| a.addr.cmp(&b.addr))
}

fn sort_tree_roots(
    instances: &mut Vec<&InstanceState>,
    sort_by: &str,
    direction: SortDirection,
    cluster_labels: &HashMap<String, String>,
    omit_host: bool,
    registry: &ColumnRegistry,
) {
    instances.sort_by(|a, b| {
        root_kind_rank(a.kind)
            .cmp(&root_kind_rank(b.kind))
            .then_with(|| {
                compare_instances(
                    a,
                    b,
                    sort_by,
                    direction,
                    cluster_labels,
                    omit_host,
                    registry,
                )
            })
    });
}

const fn apply_direction(ordering: Ordering, direction: SortDirection) -> Ordering {
    match direction {
        SortDirection::Asc => ordering,
        SortDirection::Desc => ordering.reverse(),
    }
}

fn default_sort_direction_for_column(column_key: &str) -> SortDirection {
    match column_key {
        "alias" | "addr" | "role" | "cluster" | "status" => SortDirection::Asc,
        _ => SortDirection::Desc,
    }
}

const fn root_kind_rank(kind: InstanceType) -> u8 {
    match kind {
        InstanceType::Primary => 0,
        InstanceType::Cluster => 1,
        InstanceType::Standalone => 2,
        InstanceType::Replica => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::{AppState, FilterPromptMode};
    use crate::model::{
        CommandStat, InstanceState, InstanceType, RuntimeSettings, SortDirection, SortMode,
        UiTheme, ViewMode,
    };
    use crate::registry::ColumnRegistry;
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
            ui_theme: UiTheme::default(),
        }
    }

    fn app() -> AppState {
        AppState::new(
            settings(),
            ColumnRegistry::load(None, true, SortMode::Address),
        )
    }

    #[test]
    fn tree_view_places_replicas_below_primary() {
        let mut app = app();

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
        assert!(
            !app.render_cell(&rows[0], "alias")
                .unwrap_or_default()
                .contains("└─")
        );
        assert_eq!(rows[1].key, "replica");
        assert!(
            app.render_cell(&rows[1], "alias")
                .unwrap_or_default()
                .starts_with("└─ ")
        );
        assert_eq!(app.render_cell(&rows[0], "role").as_deref(), Some("PRI"));
        assert_eq!(app.render_cell(&rows[1], "role").as_deref(), Some("REP"));
    }

    #[test]
    fn maps_raw_cluster_ids_to_compact_logical_ids() {
        let mut app = app();

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
        let cluster_by_key: HashMap<String, String> = rows
            .into_iter()
            .map(|row| {
                (
                    row.key.clone(),
                    app.render_cell(&row, "cluster").unwrap_or_default(),
                )
            })
            .collect();

        assert_eq!(cluster_by_key.get("a"), Some(&"2".to_string()));
        assert_eq!(cluster_by_key.get("b"), Some(&"1".to_string()));
        assert_eq!(cluster_by_key.get("c"), Some(&"2".to_string()));
    }

    #[test]
    fn hides_address_column_when_all_instance_hosts_are_equal() {
        let mut app = app();
        app.apply_update(InstanceState::new("a".into(), "10.0.0.12:6379".into()));
        app.apply_update(InstanceState::new("b".into(), "10.0.0.12:6380".into()));
        assert!(!app.show_address_column());
    }

    #[test]
    fn keeps_address_column_when_instance_hosts_are_mixed() {
        let mut app = app();
        app.apply_update(InstanceState::new("a".into(), "127.0.0.1:6379".into()));
        app.apply_update(InstanceState::new("b".into(), "10.0.0.12:6380".into()));
        assert!(app.show_address_column());
    }

    #[test]
    fn default_label_omits_host_when_all_hosts_match() {
        let mut app = app();
        app.apply_update(InstanceState::new("a".into(), "127.0.0.1:6379".into()));
        app.apply_update(InstanceState::new("b".into(), "127.0.0.1:6380".into()));

        let rows = app.visible_rows();
        assert_eq!(app.render_cell(&rows[0], "alias").as_deref(), Some("6379"));
        assert_eq!(app.render_cell(&rows[1], "alias").as_deref(), Some("6380"));
    }

    #[test]
    fn force_show_host_override_keeps_address_column_visible() {
        let mut app = app();
        app.apply_update(InstanceState::new("a".into(), "127.0.0.1:6379".into()));
        app.apply_update(InstanceState::new("b".into(), "127.0.0.1:6380".into()));
        app.toggle_host_rendering();

        assert!(app.show_address_column());
        let rows = app.visible_rows();
        assert_eq!(
            app.render_cell(&rows[0], "alias").as_deref(),
            Some("127.0.0.1:6379")
        );
    }

    #[test]
    fn start_filter_input_sets_mode_and_clear_behavior() {
        let mut app = app();
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
        let mut app = app();
        app.apply_update(InstanceState::new("a".into(), "127.0.0.1:6379".into()));
        app.apply_update(InstanceState::new("b".into(), "127.0.0.1:6380".into()));

        let columns = app.sortable_columns();
        assert!(columns.iter().any(|key| key == "alias"));
        assert!(!columns.iter().any(|key| key == "addr"));
    }

    #[test]
    fn applying_same_sort_column_toggles_direction() {
        let mut app = app();
        app.sort_by = "status".to_string();
        app.sort_direction = SortDirection::Asc;
        app.open_sort_picker();

        app.apply_sort_picker_selection();

        assert_eq!(app.sort_by, "status");
        assert_eq!(app.sort_direction, SortDirection::Desc);
    }

    #[test]
    fn column_picker_uses_all_available_columns() {
        let app = app();
        let columns = app.available_overview_columns();

        assert!(columns.iter().any(|key| key == "alias"));
        assert!(columns.iter().any(|key| key == "cluster"));
    }

    #[test]
    fn available_overview_columns_keep_visible_columns_first_in_runtime_order() {
        let mut app = app();
        app.runtime_visible_overview = vec!["ops".to_string(), "alias".to_string()];
        app.runtime_overview_column_order = vec![
            "ops".to_string(),
            "alias".to_string(),
            "cluster".to_string(),
        ];

        let columns = app.available_overview_columns();

        assert_eq!(columns.first().map(String::as_str), Some("ops"));
        assert_eq!(columns.get(1).map(String::as_str), Some("alias"));
        assert!(columns.iter().any(|key| key == "cluster"));
    }

    #[test]
    fn hiding_active_sort_column_moves_sort_to_next_visible_column() {
        let mut app = app();
        app.sort_by = "ops".to_string();
        app.sort_direction = SortDirection::Desc;
        app.open_column_picker();
        app.column_picker_index = app
            .available_overview_columns()
            .iter()
            .position(|key| key == "ops")
            .unwrap_or(0);

        app.toggle_selected_column_visibility();

        assert!(!app.is_column_visible("ops"));
        assert_ne!(app.sort_by, "ops");
        assert!(
            app.visible_column_keys()
                .iter()
                .any(|key| key == &app.sort_by)
        );
    }

    #[test]
    fn column_picker_keeps_at_least_one_visible_column() {
        let mut app = app();
        app.runtime_visible_overview = vec!["alias".to_string()];
        app.open_column_picker();
        app.column_picker_index = app
            .available_overview_columns()
            .iter()
            .position(|key| key == "alias")
            .unwrap_or(0);

        app.toggle_selected_column_visibility();

        assert_eq!(app.runtime_visible_overview, vec!["alias".to_string()]);
        assert_eq!(app.visible_column_keys(), vec!["alias".to_string()]);
    }

    #[test]
    fn auto_hidden_address_column_does_not_count_as_last_visible_column() {
        let mut app = app();
        app.apply_update(InstanceState::new("a".into(), "127.0.0.1:6379".into()));
        app.apply_update(InstanceState::new("b".into(), "127.0.0.1:6380".into()));
        app.runtime_visible_overview = vec!["alias".to_string(), "addr".to_string()];
        app.open_column_picker();
        app.column_picker_index = app
            .available_overview_columns()
            .iter()
            .position(|key| key == "alias")
            .unwrap_or(0);

        app.toggle_selected_column_visibility();

        assert_eq!(
            app.runtime_visible_overview,
            vec!["alias".to_string(), "addr".to_string()]
        );
        assert_eq!(app.visible_column_keys(), vec!["alias".to_string()]);
    }

    #[test]
    fn moving_selected_column_reorders_runtime_columns() {
        let mut app = app();
        app.runtime_overview_column_order =
            vec!["alias".to_string(), "ops".to_string(), "status".to_string()];
        app.runtime_visible_overview =
            vec!["alias".to_string(), "ops".to_string(), "status".to_string()];
        app.open_column_picker();
        app.column_picker_index = 1;

        app.move_selected_column(1);

        assert_eq!(
            app.runtime_overview_column_order,
            vec!["alias".to_string(), "status".to_string(), "ops".to_string()]
        );
        assert_eq!(app.column_picker_index, 2);
    }

    #[test]
    fn moving_hidden_column_reorders_runtime_order() {
        let mut app = app();
        app.runtime_overview_column_order = vec![
            "alias".to_string(),
            "ops".to_string(),
            "cluster".to_string(),
        ];
        app.runtime_visible_overview = vec!["alias".to_string(), "ops".to_string()];
        app.open_column_picker();
        app.column_picker_index = app
            .available_overview_columns()
            .iter()
            .position(|key| key == "cluster")
            .unwrap_or(0);

        app.move_selected_column(-1);

        assert_eq!(
            app.runtime_overview_column_order,
            vec![
                "alias".to_string(),
                "cluster".to_string(),
                "ops".to_string(),
            ]
        );
        assert_eq!(app.column_picker_index, 1);
    }

    #[test]
    fn toggling_column_visibility_keeps_picker_order_stable() {
        let mut app = app();
        app.runtime_overview_column_order = vec![
            "alias".to_string(),
            "ops".to_string(),
            "cluster".to_string(),
            "status".to_string(),
        ];
        app.runtime_visible_overview = vec![
            "alias".to_string(),
            "ops".to_string(),
            "cluster".to_string(),
            "status".to_string(),
        ];
        app.open_column_picker();
        app.column_picker_index = 1;
        let initial_columns = app.available_overview_columns();
        let initial_ops_index = initial_columns
            .iter()
            .position(|key| key == "ops")
            .unwrap_or(0);

        app.toggle_selected_column_visibility();

        let hidden_columns = app.available_overview_columns();
        let hidden_ops_index = hidden_columns
            .iter()
            .position(|key| key == "ops")
            .unwrap_or(0);
        assert_eq!(hidden_ops_index, initial_ops_index);
        assert_eq!(
            app.visible_column_keys(),
            vec![
                "alias".to_string(),
                "cluster".to_string(),
                "status".to_string(),
            ]
        );

        app.toggle_selected_column_visibility();

        let restored_columns = app.available_overview_columns();
        let restored_ops_index = restored_columns
            .iter()
            .position(|key| key == "ops")
            .unwrap_or(0);
        assert_eq!(restored_ops_index, initial_ops_index);
        assert_eq!(
            app.visible_column_keys(),
            vec![
                "alias".to_string(),
                "ops".to_string(),
                "cluster".to_string(),
                "status".to_string(),
            ]
        );
    }

    #[test]
    fn moving_column_swaps_with_hidden_neighbors_in_picker_order() {
        let mut app = app();
        app.runtime_overview_column_order = vec![
            "alias".to_string(),
            "cluster".to_string(),
            "ops".to_string(),
            "status".to_string(),
        ];
        app.runtime_visible_overview =
            vec!["alias".to_string(), "ops".to_string(), "status".to_string()];
        app.open_column_picker();
        app.column_picker_index = app
            .available_overview_columns()
            .iter()
            .position(|key| key == "ops")
            .unwrap_or(0);

        app.move_selected_column(-1);

        assert_eq!(
            app.runtime_overview_column_order,
            vec![
                "alias".to_string(),
                "ops".to_string(),
                "cluster".to_string(),
                "status".to_string(),
            ]
        );
        assert_eq!(
            app.visible_column_keys(),
            vec!["alias".to_string(), "ops".to_string(), "status".to_string()]
        );
        assert_eq!(app.column_picker_index, 1);
    }

    #[test]
    fn emphasizes_max_latency_rows_per_visible_column() {
        let mut app = app();
        app.view_mode = ViewMode::Flat;

        let mut a = InstanceState::new("a".into(), "127.0.0.1:6379".into());
        a.last_latency_ms = Some(0.25);
        a.max_latency_ms = 1.4;

        let mut b = InstanceState::new("b".into(), "127.0.0.1:6380".into());
        b.last_latency_ms = Some(0.95);
        b.max_latency_ms = 0.8;

        let mut c = InstanceState::new("c".into(), "127.0.0.1:6381".into());
        c.last_latency_ms = Some(0.40);
        c.max_latency_ms = 2.1;

        app.apply_update(a);
        app.apply_update(b);
        app.apply_update(c);

        let rows = app.visible_rows();
        let emphasized = app.emphasized_rows_by_column(&rows);

        assert_eq!(emphasized.get("lat_last"), Some(&"b".to_string()));
        assert_eq!(emphasized.get("lat_max"), Some(&"c".to_string()));
    }

    #[test]
    fn visible_commandstats_filters_and_sorts_by_calls_desc() {
        let mut app = app();
        app.commandstats_view.filter = "clu".to_string();

        let stats = vec![
            CommandStat {
                command: "get".into(),
                calls: 100,
                usec: 1_000,
                usec_per_call: 10.0,
            },
            CommandStat {
                command: "cluster|shards".into(),
                calls: 500,
                usec: 2_000,
                usec_per_call: 4.0,
            },
            CommandStat {
                command: "cluster|info".into(),
                calls: 50,
                usec: 500,
                usec_per_call: 10.0,
            },
        ];

        let visible = app.visible_commandstats(&stats);
        assert_eq!(visible.len(), 2);
        assert_eq!(visible[0].command, "cluster|shards");
        assert_eq!(visible[1].command, "cluster|info");
    }

    #[test]
    fn commandstats_scroll_is_clamped_to_visible_page() {
        let mut app = app();
        app.commandstats_view.scroll_offset = 10;

        let stats = vec![
            CommandStat {
                command: "a".into(),
                calls: 4,
                usec: 4,
                usec_per_call: 1.0,
            },
            CommandStat {
                command: "b".into(),
                calls: 3,
                usec: 3,
                usec_per_call: 1.0,
            },
            CommandStat {
                command: "c".into(),
                calls: 2,
                usec: 2,
                usec_per_call: 1.0,
            },
            CommandStat {
                command: "d".into(),
                calls: 1,
                usec: 1,
                usec_per_call: 1.0,
            },
        ];

        app.clamp_commandstats_scroll(&stats, 3);
        assert_eq!(app.commandstats_view.scroll_offset, 1);

        app.move_commandstats_scroll(-5, &stats, 3);
        assert_eq!(app.commandstats_view.scroll_offset, 0);
    }

    #[test]
    fn start_bigkeys_filter_input_sets_clear_behavior() {
        let mut app = app();
        app.bigkeys_view.filter = "session".to_string();

        app.start_bigkeys_filter_input(false);
        assert!(app.bigkeys_view.is_filtering);
        assert_eq!(app.bigkeys_view.filter, "session");

        app.start_bigkeys_filter_input(true);
        assert!(app.bigkeys_view.is_filtering);
        assert!(app.bigkeys_view.filter.is_empty());
        assert_eq!(app.bigkeys_view.scroll_offset, 0);
    }

    #[test]
    fn visible_bigkeys_filters_by_key_and_type() {
        let mut app = app();
        let entries = vec![
            crate::model::BigkeyEntry {
                key: "session:1".into(),
                key_type: "string".into(),
                size: Some(32),
                memory_usage: Some(128),
            },
            crate::model::BigkeyEntry {
                key: "timeline".into(),
                key_type: "zset".into(),
                size: Some(2_000),
                memory_usage: Some(70_968),
            },
            crate::model::BigkeyEntry {
                key: "profile".into(),
                key_type: "hash".into(),
                size: Some(100),
                memory_usage: Some(2_123),
            },
        ];

        app.bigkeys_view.filter = "set".to_string();
        let visible = app.visible_bigkeys(&entries);
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].key, "timeline");

        app.bigkeys_view.filter = "session".to_string();
        let visible = app.visible_bigkeys(&entries);
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].key, "session:1");
    }
}
