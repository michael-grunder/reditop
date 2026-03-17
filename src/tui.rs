use std::io::{self, Stdout};
use std::time::Duration;

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, Tabs, Wrap};

use crate::app::{ActiveView, AppState, FilterPromptMode};
use crate::cli::LaunchConfig;
use crate::column::{Align, EmphasisStyle};
use crate::poller;
use crate::registry::ColumnRegistry;

pub fn run(launch: LaunchConfig) -> Result<()> {
    if launch.verbose {
        eprintln!(
            "redis-top: targets={} refresh={} connect_timeout={} command_timeout={}",
            launch.targets.len(),
            humantime::format_duration(launch.settings.refresh_interval),
            humantime::format_duration(launch.settings.connect_timeout),
            humantime::format_duration(launch.settings.command_timeout)
        );
    }
    let mut terminal = setup_terminal()?;
    let result = run_loop(&mut terminal, launch);
    restore_terminal(&mut terminal)?;
    result
}

#[allow(clippy::too_many_lines)]
fn run_loop(terminal: &mut Terminal<CrosstermBackend<Stdout>>, launch: LaunchConfig) -> Result<()> {
    let registry = ColumnRegistry::load(
        launch.config_path.as_deref(),
        launch.no_default_config,
        launch.settings.default_sort,
    );
    let mut app = AppState::new(launch.settings.clone(), registry);
    let (mut updates_rx, refresh_tx) = poller::start(launch.targets, launch.settings);

    loop {
        while let Ok(update) = updates_rx.try_recv() {
            app.apply_update(update);
        }

        terminal.draw(|frame| draw(frame, &app))?;

        if app.should_quit {
            break;
        }

        if event::poll(Duration::from_millis(100))? {
            let Event::Key(key) = event::read()? else {
                continue;
            };
            if key.kind != KeyEventKind::Press {
                continue;
            }
            if is_quit_key(key) {
                app.should_quit = true;
                continue;
            }

            if app.is_filtering {
                match key.code {
                    KeyCode::Esc | KeyCode::Enter => {
                        app.is_filtering = false;
                        app.clamp_selection();
                    }
                    KeyCode::Backspace => {
                        let _ = app.filter.pop();
                        app.clamp_selection();
                    }
                    KeyCode::Char(ch) => {
                        app.filter.push(ch);
                        app.clamp_selection();
                    }
                    KeyCode::F(3) if app.active_view == ActiveView::Overview => {
                        app.start_filter_input(FilterPromptMode::Search, false);
                    }
                    KeyCode::F(4) if app.active_view == ActiveView::Overview => {
                        app.start_filter_input(FilterPromptMode::Filter, true);
                    }
                    _ => {}
                }
                continue;
            }

            if app.is_sorting {
                match key.code {
                    KeyCode::Esc => app.close_sort_picker(),
                    KeyCode::Up => app.move_sort_picker_selection(-1),
                    KeyCode::Down => app.move_sort_picker_selection(1),
                    KeyCode::Enter => app.apply_sort_picker_selection(),
                    _ => {}
                }
                continue;
            }

            match key.code {
                KeyCode::F(1) | KeyCode::Char('H') => app.open_help_view(),
                KeyCode::Char('?') => app.show_help = !app.show_help,
                KeyCode::F(5) | KeyCode::Char('t') if app.active_view == ActiveView::Overview => {
                    app.view_mode = app.view_mode.toggle();
                    app.clamp_selection();
                }
                KeyCode::F(6) if app.active_view == ActiveView::Overview => {
                    app.open_sort_picker();
                }
                KeyCode::Char('s') if app.active_view == ActiveView::Overview => {
                    app.cycle_sort_mode();
                }
                KeyCode::Char('h') if app.active_view == ActiveView::Overview => {
                    app.toggle_host_rendering();
                    app.clamp_selection();
                }
                KeyCode::F(3) if app.active_view == ActiveView::Overview => {
                    app.start_filter_input(FilterPromptMode::Search, false);
                }
                KeyCode::F(4) if app.active_view == ActiveView::Overview => {
                    app.start_filter_input(FilterPromptMode::Filter, true);
                }
                KeyCode::Char('/') if app.active_view == ActiveView::Overview => {
                    app.start_filter_input(FilterPromptMode::Filter, false);
                }
                KeyCode::Char('r') => {
                    let _ = refresh_tx.try_send(());
                }
                KeyCode::Up if app.active_view == ActiveView::Overview => app.move_selection(-1),
                KeyCode::Down if app.active_view == ActiveView::Overview => app.move_selection(1),
                KeyCode::Enter if app.active_view == ActiveView::Overview => {
                    if app.selected_key().is_some() {
                        app.active_view = ActiveView::Detail;
                    }
                }
                KeyCode::Esc if app.active_view == ActiveView::Detail => {
                    app.active_view = ActiveView::Overview;
                }
                KeyCode::Esc if app.active_view == ActiveView::Help => app.close_help_view(),
                KeyCode::Tab | KeyCode::Right if app.active_view == ActiveView::Detail => {
                    app.detail_tab = (app.detail_tab + 1) % 3;
                }
                KeyCode::Left if app.active_view == ActiveView::Detail => {
                    app.detail_tab = (app.detail_tab + 2) % 3;
                }
                _ => {}
            }
        }
    }

    Ok(())
}

const fn is_quit_key(key: KeyEvent) -> bool {
    matches!(key.code, KeyCode::Char('q'))
        || matches!(
            key,
            KeyEvent {
                code: KeyCode::Char('c'),
                modifiers,
                ..
            } if modifiers.contains(KeyModifiers::CONTROL)
        )
}

fn draw(frame: &mut ratatui::Frame<'_>, app: &AppState) {
    let area = frame.area();
    frame.render_widget(Block::default().style(base_style(app)), area);
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(5), Constraint::Length(2)])
        .split(area);

    match app.active_view {
        ActiveView::Overview => draw_overview(frame, app, chunks[0]),
        ActiveView::Detail => draw_detail(frame, app, chunks[0]),
        ActiveView::Help => draw_help_page(frame, app, chunks[0]),
    }
    draw_status_bar(frame, app, chunks[1]);

    if app.is_sorting {
        draw_sort_picker(frame, area, app);
    }

    if app.show_help {
        draw_help_overlay(frame, app, area);
    }
}

fn overview_cell(fitted: String, emphasis_style: Option<EmphasisStyle>) -> Cell<'static> {
    let Some(emphasis_style) = emphasis_style else {
        return Cell::from(fitted);
    };

    Cell::from(Line::styled(fitted, style_from_emphasis(emphasis_style)))
}

fn overview_cluster_gutter_cell(
    app: &AppState,
    row: &crate::app::DisplayRow,
    cluster_labels: &std::collections::HashMap<String, String>,
) -> Cell<'static> {
    let Some(instance) = app.instances.get(&row.key) else {
        return Cell::from(" ");
    };

    let Some(color) = cluster_gutter_color(app, instance, cluster_labels) else {
        return Cell::from(" ");
    };

    Cell::from(Line::from(vec![Span::styled(
        "│",
        Style::default().fg(color),
    )]))
}

fn cluster_gutter_color(
    app: &AppState,
    instance: &crate::model::InstanceState,
    cluster_labels: &std::collections::HashMap<String, String>,
) -> Option<Color> {
    let token = instance.cluster_id.as_deref().map_or_else(
        || replication_group_token(app, instance),
        |raw_cluster| cluster_labels.get(raw_cluster).cloned(),
    )?;

    Some(cluster_color_for_token(&token))
}

fn replication_group_token(
    app: &AppState,
    instance: &crate::model::InstanceState,
) -> Option<String> {
    match instance.kind {
        crate::model::InstanceType::Primary => app
            .instances
            .values()
            .any(|candidate| candidate.parent_addr.as_deref() == Some(instance.addr.as_str()))
            .then(|| instance.addr.clone()),
        crate::model::InstanceType::Replica => instance
            .parent_addr
            .as_deref()
            .map(|parent| resolve_replication_group_addr(app, parent)),
        crate::model::InstanceType::Standalone | crate::model::InstanceType::Cluster => None,
    }
}

fn resolve_replication_group_addr(app: &AppState, parent: &str) -> String {
    app.instances
        .values()
        .find(|candidate| candidate.key == parent || candidate.addr == parent)
        .map_or_else(|| parent.to_string(), |candidate| candidate.addr.clone())
}

fn cluster_color_for_token(token: &str) -> Color {
    const PALETTE: [Color; 7] = [
        Color::Cyan,
        Color::Yellow,
        Color::Green,
        Color::Magenta,
        Color::Blue,
        Color::Red,
        Color::Gray,
    ];

    let index = token.bytes().fold(0usize, |acc, byte| {
        acc.wrapping_mul(33).wrapping_add(usize::from(byte))
    });
    PALETTE[index % PALETTE.len()]
}

fn style_from_emphasis(emphasis_style: EmphasisStyle) -> Style {
    let mut style = Style::default();
    if emphasis_style.bold {
        style = style.add_modifier(Modifier::BOLD);
    }
    if emphasis_style.italic {
        style = style.add_modifier(Modifier::ITALIC);
    }
    if emphasis_style.underlined {
        style = style.add_modifier(Modifier::UNDERLINED);
    }
    if emphasis_style.dim {
        style = style.add_modifier(Modifier::DIM);
    }
    if emphasis_style.reversed {
        style = style.add_modifier(Modifier::REVERSED);
    }
    if let Some(color) = emphasis_style.foreground {
        style = style.fg(color.to_ratatui_color());
    }
    style
}

#[allow(clippy::too_many_lines)]
fn draw_overview(frame: &mut ratatui::Frame<'_>, app: &AppState, area: Rect) {
    const TABLE_COLUMN_SPACING: u16 = 1;

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Min(5)])
        .split(area);

    let header = Paragraph::new(format!(
        "redis-top  refresh={}  view={:?}  sort={} {}  host={}  filter={}{}",
        humantime::format_duration(app.settings.refresh_interval),
        app.view_mode,
        app.sort_label(),
        sort_direction_symbol(app.sort_direction),
        if app.force_show_host {
            "shown"
        } else if app.should_omit_host_in_rendering() {
            "omitted(auto)"
        } else {
            "shown(auto)"
        },
        if app.filter.is_empty() {
            "<none>"
        } else {
            &app.filter
        },
        if app.is_filtering { " (editing)" } else { "" }
    ))
    .style(base_style(app))
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title("Overview")
            .style(base_style(app)),
    );
    frame.render_widget(header, chunks[0]);

    let rows = app.visible_rows();
    let column_keys = app.visible_column_keys();
    let cluster_labels = app.cluster_labels();
    let emphasized = app.emphasized_rows_by_column(&rows);
    let columns: Vec<_> = column_keys
        .iter()
        .filter_map(|key| app.column_registry.column(key.as_str()))
        .collect();
    let widths = compute_column_widths(
        chunks[1].width.saturating_sub(2),
        &columns,
        TABLE_COLUMN_SPACING,
    );

    let table_rows: Vec<Row<'_>> = rows
        .iter()
        .map(|row| {
            let mut cells = Vec::with_capacity(column_keys.len() + 1);
            cells.push(overview_cluster_gutter_cell(app, row, &cluster_labels));
            cells.extend(column_keys.iter().enumerate().map(|(idx, key)| {
                let width = widths[idx];
                let align = columns[idx].align();
                let raw = app.render_cell(row, key).unwrap_or_default();
                let fitted = fit_cell_text(&raw, width as usize, align);
                let emphasis_style = emphasized
                    .get(key)
                    .filter(|winner| *winner == &row.key)
                    .map(|_| {
                        columns[idx]
                            .emphasis_style()
                            .unwrap_or_else(|| app.column_registry.overview_emphasis_style())
                    });
                overview_cell(fitted, emphasis_style)
            }));

            let base = Row::new(cells);
            if row.stale {
                base.style(base_style(app).add_modifier(Modifier::DIM))
            } else {
                base.style(base_style(app))
            }
        })
        .collect();

    let constraints: Vec<Constraint> = std::iter::once(Constraint::Length(1))
        .chain(widths.iter().copied().map(Constraint::Length))
        .collect();
    let header = Row::new(
        std::iter::once(Cell::from(" ")).chain(
            columns
                .iter()
                .zip(column_keys.iter())
                .zip(widths.iter())
                .map(|((column, key), width)| {
                    let label = sortable_header(column.header(), app, key);
                    Cell::from(fit_cell_text(&label, *width as usize, column.align()))
                }),
        ),
    )
    .style(base_style(app).add_modifier(Modifier::BOLD));

    let selected_style = Style::default().bg(background_color(app));
    let table = Table::new(table_rows, constraints)
        .header(header)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .style(base_style(app)),
        )
        .column_spacing(TABLE_COLUMN_SPACING)
        .row_highlight_style(selected_style)
        .highlight_symbol("> ");

    let mut state = ratatui::widgets::TableState::default().with_selected(Some(app.selected_index));
    frame.render_stateful_widget(table, chunks[1], &mut state);
}

fn compute_column_widths(
    table_width: u16,
    columns: &[&std::sync::Arc<dyn crate::column::Column>],
    column_spacing: u16,
) -> Vec<u16> {
    if columns.is_empty() {
        return Vec::new();
    }

    let gaps = u16::try_from(columns.len().saturating_sub(1)).unwrap_or(u16::MAX);
    let spacing_total = column_spacing.saturating_mul(gaps);
    let content_width = table_width.saturating_sub(spacing_total);
    let mut widths = vec![0u16; columns.len()];
    let mut remaining = content_width;

    for (idx, column) in columns.iter().enumerate() {
        let hint = column.width_hint();
        if let Some(fixed) = hint.fixed {
            widths[idx] = fixed;
            remaining = remaining.saturating_sub(fixed);
        }
    }

    for (idx, column) in columns.iter().enumerate() {
        if widths[idx] > 0 {
            continue;
        }
        let min = column.width_hint().min;
        widths[idx] = min;
        remaining = remaining.saturating_sub(min);
    }

    let used = widths.iter().copied().sum::<u16>();
    if used > content_width {
        shrink_widths_to_fit(&mut widths, content_width);
        remaining = 0;
    }

    loop {
        if remaining == 0 {
            break;
        }
        let mut progressed = false;
        for (idx, column) in columns.iter().enumerate() {
            let hint = column.width_hint();
            if hint.fixed.is_some() {
                continue;
            }
            let max = hint.max.unwrap_or(u16::MAX);
            let ideal = hint.ideal.min(max);
            if widths[idx] < ideal {
                widths[idx] = widths[idx].saturating_add(1);
                remaining = remaining.saturating_sub(1);
                progressed = true;
                if remaining == 0 {
                    break;
                }
            }
        }
        if !progressed {
            break;
        }
    }

    widths
}

fn shrink_widths_to_fit(widths: &mut [u16], target: u16) {
    while widths.iter().copied().sum::<u16>() > target {
        let Some((idx, _)) = widths.iter().enumerate().max_by_key(|(_, width)| **width) else {
            break;
        };
        if widths[idx] <= 1 {
            break;
        }
        widths[idx] -= 1;
    }
}

fn fit_cell_text(text: &str, width: usize, align: Align) -> String {
    if width == 0 {
        return String::new();
    }
    let mut chars = text.chars().collect::<Vec<char>>();
    if chars.len() > width {
        chars.truncate(width);
    }
    let truncated: String = chars.into_iter().collect();
    let len = truncated.chars().count();
    if len >= width {
        return truncated;
    }
    let pad = width - len;
    match align {
        Align::Left => format!("{truncated}{:pad$}", "", pad = pad),
        Align::Right => format!("{:pad$}{truncated}", "", pad = pad),
        Align::Center => {
            let left = pad / 2;
            let right = pad - left;
            format!(
                "{:left$}{truncated}{:right$}",
                "",
                "",
                left = left,
                right = right
            )
        }
    }
}

#[allow(clippy::too_many_lines)]
fn draw_detail(frame: &mut ratatui::Frame<'_>, app: &AppState, area: Rect) {
    let Some(selected_key) = app.selected_key() else {
        frame.render_widget(
            Paragraph::new("No instance selected")
                .style(base_style(app))
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title("Detail")
                        .style(base_style(app)),
                ),
            area,
        );
        return;
    };
    let Some(instance) = app.instances.get(&selected_key) else {
        return;
    };

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(5),
        ])
        .split(area);

    let title = format!(
        "{} ({})  role={}  status={}  version={} uptime={}s",
        instance
            .alias
            .clone()
            .unwrap_or_else(|| instance.addr.clone()),
        instance.addr,
        instance.kind.as_str(),
        instance.status.as_str(),
        instance
            .detail
            .redis_version
            .clone()
            .unwrap_or_else(|| "-".to_string()),
        instance
            .detail
            .uptime_seconds
            .map_or_else(|| "-".to_string(), format_with_commas)
    );
    frame.render_widget(
        Paragraph::new(title).style(base_style(app)).block(
            Block::default()
                .borders(Borders::ALL)
                .title("Instance")
                .style(base_style(app)),
        ),
        chunks[0],
    );

    let tabs = Tabs::new(vec!["Summary", "Latency", "Info Raw"])
        .style(base_style(app))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .style(base_style(app)),
        )
        .select(app.detail_tab)
        .highlight_style(
            Style::default()
                .fg(carat_color(app))
                .bg(background_color(app))
                .add_modifier(Modifier::BOLD),
        );
    frame.render_widget(tabs, chunks[1]);

    match app.detail_tab {
        0 => {
            let hits = instance.detail.keyspace_hits.unwrap_or(0);
            let misses = instance.detail.keyspace_misses.unwrap_or(0);
            let hit_rate = if hits + misses == 0 {
                0.0
            } else {
                crate::column::u64_to_f64(hits) / crate::column::u64_to_f64(hits + misses) * 100.0
            };
            let replication_source = match (
                instance.detail.master_host.as_deref(),
                instance.detail.master_port,
            ) {
                (Some(host), Some(port)) => format!("{host}:{port}"),
                (Some(host), None) => host.to_string(),
                _ => "-".to_string(),
            };
            let body = format_aligned_rows(&[
                (
                    "used_memory",
                    format_optional_bytes(instance.used_memory_bytes),
                ),
                (
                    "used_memory_rss",
                    format_optional_bytes(instance.detail.used_memory_rss),
                ),
                ("maxmemory", format_optional_bytes(instance.maxmemory_bytes)),
                ("ops_per_sec", format_optional_u64(instance.ops_per_sec)),
                (
                    "commands",
                    format_optional_u64(instance.detail.total_commands_processed),
                ),
                (
                    "connected_clients",
                    format_optional_u64(instance.detail.connected_clients),
                ),
                (
                    "blocked_clients",
                    format_optional_u64(instance.detail.blocked_clients),
                ),
                ("hits", format_with_commas(hits)),
                ("misses", format_with_commas(misses)),
                ("hit_rate", format!("{hit_rate:.1}%")),
                (
                    "evicted_keys",
                    format_optional_u64(instance.detail.evicted_keys),
                ),
                (
                    "expired_keys",
                    format_optional_u64(instance.detail.expired_keys),
                ),
                ("master", replication_source),
            ]);
            frame.render_widget(
                Paragraph::new(body)
                    .style(base_style(app))
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .title("Summary")
                            .style(base_style(app)),
                    )
                    .wrap(Wrap { trim: false }),
                chunks[2],
            );
        }
        1 => {
            let body = format_aligned_rows(&[
                (
                    "last_latency_ms",
                    instance
                        .last_latency_ms
                        .map_or_else(|| "-".to_string(), |v| format!("{v:.2}")),
                ),
                ("max_latency_ms", format!("{:.2}", instance.max_latency_ms)),
                ("avg_latency_ms", format!("{:.2}", instance.avg_latency_ms)),
                (
                    "window_samples",
                    format_with_commas(instance.latency_window.len() as u64),
                ),
            ]);
            frame.render_widget(
                Paragraph::new(body).style(base_style(app)).block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title("Latency")
                        .style(base_style(app)),
                ),
                chunks[2],
            );
        }
        _ => {
            let body = instance
                .detail
                .raw_info
                .clone()
                .unwrap_or_else(|| "INFO not available".to_string());
            frame.render_widget(
                Paragraph::new(body)
                    .style(base_style(app))
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .title("Info Raw")
                            .style(base_style(app)),
                    )
                    .wrap(Wrap { trim: false }),
                chunks[2],
            );
        }
    }
}

fn format_aligned_rows(rows: &[(&str, String)]) -> String {
    let width = rows.iter().map(|(label, _)| label.len()).max().unwrap_or(0);
    rows.iter()
        .map(|(label, value)| format!("{label:width$} : {value}"))
        .collect::<Vec<String>>()
        .join("\n")
}

fn format_optional_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "-".to_string(), format_with_commas)
}

fn format_optional_bytes(value: Option<u64>) -> String {
    value.map_or_else(
        || "-".to_string(),
        |bytes| format!("{} ({})", format_with_commas(bytes), human_bytes(bytes)),
    )
}

fn format_with_commas(value: u64) -> String {
    let digits = value.to_string();
    let rev = digits.chars().rev().collect::<Vec<char>>();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (idx, ch) in rev.iter().enumerate() {
        if idx > 0 && idx % 3 == 0 {
            out.push(',');
        }
        out.push(*ch);
    }
    out.chars().rev().collect()
}

fn human_bytes(bytes: u64) -> String {
    crate::column::format_bytes(bytes)
}

fn draw_help_page(frame: &mut ratatui::Frame<'_>, app: &AppState, area: Rect) {
    let rows: Vec<Row<'_>> = help_bindings()
        .iter()
        .map(|(keys, action)| Row::new(vec![Cell::from(*keys), Cell::from(*action)]))
        .collect();
    let table = Table::new(rows, [Constraint::Length(24), Constraint::Min(20)])
        .header(
            Row::new(vec!["Keys", "Action"]).style(base_style(app).add_modifier(Modifier::BOLD)),
        )
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Help (Esc to go back)")
                .style(base_style(app)),
        );
    frame.render_widget(table, area);
}

fn draw_status_bar(frame: &mut ratatui::Frame<'_>, app: &AppState, area: Rect) {
    let lines = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Length(1)])
        .split(area);

    let prompt = if app.is_filtering {
        format!("{}: {}", app.filter_prompt_mode.label(), app.filter)
    } else if let Some(key) = app.selected_key() {
        app.instances
            .get(&key)
            .and_then(|instance| instance.last_error.clone())
            .unwrap_or_default()
    } else {
        String::new()
    };
    frame.render_widget(Paragraph::new(prompt).style(base_style(app)), lines[0]);

    frame.render_widget(
        Paragraph::new("F1Help  F3Search  F4Filter  F5Tree  F6SortBy")
            .style(base_style(app).add_modifier(Modifier::BOLD)),
        lines[1],
    );
}

const fn help_bindings() -> &'static [(&'static str, &'static str)] {
    &[
        ("q / Ctrl+C", "Quit"),
        ("F1", "Open full help page"),
        ("H", "Open this help page"),
        ("Esc", "Back from detail/help or stop filter editing"),
        ("Enter", "Open detail view from overview"),
        ("Tab/Right", "Next detail panel"),
        ("Left", "Previous detail panel"),
        ("Up/Down", "Move selection in overview"),
        ("?", "Toggle help overlay"),
        ("r", "Refresh now"),
        ("F3", "Start search input in overview"),
        (
            "F4",
            "Start filter input in overview (clears existing filter)",
        ),
        ("F5", "Toggle flat/tree view in overview"),
        ("F6", "Choose sort column in overview"),
        ("t", "Toggle flat/tree view in overview"),
        ("s", "Cycle sort column in overview"),
        (
            "h",
            "Toggle host rendering (auto hide when all hosts are the same)",
        ),
        ("/", "Start filter input in overview"),
        ("Backspace", "Delete filter character while editing"),
    ]
}

fn draw_help_overlay(frame: &mut ratatui::Frame<'_>, app: &AppState, area: Rect) {
    let width = area.width.saturating_mul(80) / 100;
    let height = area.height.saturating_mul(70) / 100;
    let popup = Rect {
        x: area.x + (area.width.saturating_sub(width)) / 2,
        y: area.y + (area.height.saturating_sub(height)) / 2,
        width,
        height,
    };

    frame.render_widget(Clear, popup);
    let text = "q or Ctrl+C quit\nF1 or H open help page\nEsc back\nEnter open detail\nTab/Left/Right cycle detail panels\nUp/Down move selection\n? toggle help overlay\nr refresh now\nF3 search\nF4 filter\nF5 toggle flat/tree\nF6 open sort picker\nh toggle host rendering\n/ filter in overview";
    frame.render_widget(
        Paragraph::new(text)
            .style(base_style(app))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Help")
                    .style(base_style(app)),
            )
            .wrap(Wrap { trim: false }),
        popup,
    );
}

const fn sort_direction_symbol(direction: crate::model::SortDirection) -> &'static str {
    match direction {
        crate::model::SortDirection::Asc => "↑",
        crate::model::SortDirection::Desc => "↓",
    }
}

fn sortable_header(label: &str, app: &AppState, column_key: &str) -> String {
    if app.sort_by == column_key {
        format!("{label} {}", sort_direction_symbol(app.sort_direction))
    } else {
        label.to_string()
    }
}

fn draw_sort_picker(frame: &mut ratatui::Frame<'_>, area: Rect, app: &AppState) {
    let width = area.width.saturating_mul(45) / 100;
    let height = area.height.saturating_mul(55) / 100;
    let popup = Rect {
        x: area.x + (area.width.saturating_sub(width)) / 2,
        y: area.y + (area.height.saturating_sub(height)) / 2,
        width,
        height,
    };
    let columns = app.sortable_columns();
    let rows: Vec<Row<'_>> = columns
        .iter()
        .map(|column_key| {
            let direction = if *column_key == app.sort_by {
                format!(" ({})", sort_direction_symbol(app.sort_direction))
            } else {
                String::new()
            };
            let label = app
                .column_registry
                .column(column_key)
                .map_or_else(|| column_key.clone(), |column| column.header().to_string());
            Row::new(vec![Cell::from(format!("{label}{direction}"))])
        })
        .collect();
    let table = Table::new(rows, [Constraint::Percentage(100)])
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Sort By (Enter select, Esc cancel)")
                .style(base_style(app)),
        )
        .style(base_style(app))
        .row_highlight_style(
            Style::default()
                .fg(carat_color(app))
                .bg(background_color(app))
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("> ");
    let mut state =
        ratatui::widgets::TableState::default().with_selected(Some(app.sort_picker_index));

    frame.render_widget(Clear, popup);
    frame.render_stateful_widget(table, popup, &mut state);
}

fn base_style(app: &AppState) -> Style {
    Style::default()
        .fg(foreground_color(app))
        .bg(background_color(app))
}

const fn background_color(app: &AppState) -> Color {
    app.settings.ui_theme.background.to_ratatui_color()
}

const fn foreground_color(app: &AppState) -> Color {
    app.settings.ui_theme.foreground.to_ratatui_color()
}

const fn carat_color(app: &AppState) -> Color {
    app.settings.ui_theme.carat.to_ratatui_color()
}

fn setup_terminal() -> Result<Terminal<CrosstermBackend<Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    Ok(Terminal::new(backend)?)
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<Stdout>>) -> Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use ratatui::{
        Terminal,
        backend::TestBackend,
        style::{Color, Modifier},
    };

    use super::{
        Align, cluster_color_for_token, compute_column_widths, draw, fit_cell_text,
        format_aligned_rows, format_with_commas, help_bindings, is_quit_key,
    };
    use crate::column::{CellText, Column, RenderCtx, SortCtx, SortKey, WidthHint};
    use crate::config::default_settings;
    use crate::model::{InstanceState, Status, ViewMode};
    use crate::registry::ColumnRegistry;

    fn buffer_lines(buffer: &ratatui::buffer::Buffer) -> Vec<String> {
        let width = usize::from(buffer.area.width);
        buffer
            .content()
            .chunks(width)
            .map(|row| {
                row.iter()
                    .map(ratatui::buffer::Cell::symbol)
                    .collect::<String>()
            })
            .collect()
    }

    fn char_column(line: &str, needle: &str) -> usize {
        let byte_idx = line.find(needle).expect("needle rendered in line");
        line[..byte_idx].chars().count()
    }

    #[test]
    fn format_with_commas_groups_digits() {
        assert_eq!(format_with_commas(0), "0");
        assert_eq!(format_with_commas(12), "12");
        assert_eq!(format_with_commas(1_234), "1,234");
        assert_eq!(format_with_commas(12_345_678), "12,345,678");
    }

    #[test]
    fn format_aligned_rows_uses_consistent_label_column() {
        let body = format_aligned_rows(&[("a", "1".to_string()), ("long_name", "2".to_string())]);
        assert_eq!(body, "a         : 1\nlong_name : 2");
    }

    #[test]
    fn help_bindings_include_help_page_shortcut() {
        assert!(help_bindings().iter().any(|(keys, _)| *keys == "H"));
    }

    #[test]
    fn help_bindings_include_function_keys() {
        assert!(help_bindings().iter().any(|(keys, _)| *keys == "F1"));
        assert!(help_bindings().iter().any(|(keys, _)| *keys == "F3"));
        assert!(help_bindings().iter().any(|(keys, _)| *keys == "F4"));
        assert!(help_bindings().iter().any(|(keys, _)| *keys == "F5"));
        assert!(help_bindings().iter().any(|(keys, _)| *keys == "F6"));
    }

    #[test]
    fn quit_key_matches_q_and_ctrl_c() {
        assert!(is_quit_key(KeyEvent::new(
            KeyCode::Char('q'),
            KeyModifiers::NONE
        )));
        assert!(is_quit_key(KeyEvent::new(
            KeyCode::Char('c'),
            KeyModifiers::CONTROL,
        )));
    }

    #[test]
    fn quit_key_does_not_match_plain_c() {
        assert!(!is_quit_key(KeyEvent::new(
            KeyCode::Char('c'),
            KeyModifiers::NONE,
        )));
    }

    #[test]
    fn fit_cell_text_right_aligns_headers_and_values_consistently() {
        assert_eq!(fit_cell_text("Ops/s", 8, Align::Right), "   Ops/s");
        assert_eq!(fit_cell_text("123", 8, Align::Right), "     123");
    }

    struct TestColumn {
        hint: WidthHint,
    }

    impl Column for TestColumn {
        fn header(&self) -> &'static str {
            ""
        }

        fn align(&self) -> Align {
            Align::Left
        }

        fn width_hint(&self) -> WidthHint {
            self.hint
        }

        fn render_cell(&self, _ctx: &RenderCtx<'_>) -> CellText {
            CellText::plain(String::new())
        }

        fn sort_key(&self, _ctx: &SortCtx<'_>) -> SortKey {
            SortKey::Null
        }
    }

    #[test]
    fn compute_column_widths_reserves_space_for_spacing() {
        let a: Arc<dyn Column> = Arc::new(TestColumn {
            hint: WidthHint {
                min: 5,
                ideal: 8,
                max: None,
                fixed: None,
            },
        });
        let b: Arc<dyn Column> = Arc::new(TestColumn {
            hint: WidthHint {
                min: 5,
                ideal: 8,
                max: None,
                fixed: None,
            },
        });
        let c: Arc<dyn Column> = Arc::new(TestColumn {
            hint: WidthHint {
                min: 5,
                ideal: 8,
                max: None,
                fixed: None,
            },
        });
        let columns = vec![&a, &b, &c];

        let widths = compute_column_widths(20, &columns, 1);

        assert_eq!(widths, vec![6, 6, 6]);
        assert_eq!(widths.iter().sum::<u16>() + 2, 20);
    }

    #[test]
    fn compute_column_widths_shrinks_below_min_when_required() {
        let a: Arc<dyn Column> = Arc::new(TestColumn {
            hint: WidthHint {
                min: 4,
                ideal: 4,
                max: None,
                fixed: None,
            },
        });
        let b: Arc<dyn Column> = Arc::new(TestColumn {
            hint: WidthHint {
                min: 4,
                ideal: 4,
                max: None,
                fixed: None,
            },
        });
        let c: Arc<dyn Column> = Arc::new(TestColumn {
            hint: WidthHint {
                min: 4,
                ideal: 4,
                max: None,
                fixed: None,
            },
        });
        let columns = vec![&a, &b, &c];

        let widths = compute_column_widths(8, &columns, 1);

        assert_eq!(widths.iter().sum::<u16>() + 2, 8);
        assert!(widths.iter().all(|width| *width >= 1));
    }

    #[test]
    fn overview_renders_cluster_gutter_with_stable_color() {
        let mut app = crate::app::AppState::new(
            default_settings(),
            ColumnRegistry::load(None, true, crate::model::SortMode::Address),
        );
        app.view_mode = ViewMode::Flat;

        let mut a = InstanceState::new("a".into(), "127.0.0.1:6379".into());
        a.cluster_id = Some("cluster-b".into());
        a.last_updated = Some(std::time::Instant::now());

        let mut b = InstanceState::new("b".into(), "127.0.0.1:6380".into());
        b.cluster_id = Some("cluster-a".into());
        b.last_updated = Some(std::time::Instant::now());

        app.apply_update(a);
        app.apply_update(b);
        app.selected_index = 1;

        let backend = TestBackend::new(100, 12);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        terminal
            .draw(|frame| draw(frame, &app))
            .expect("overview draw succeeds");

        let buffer = terminal.backend().buffer().clone();
        let lines = buffer_lines(&buffer);
        let row = lines
            .iter()
            .position(|line| line.contains("6379"))
            .expect("cluster row rendered");
        let alias_col = char_column(&lines[row], "6379");
        let gutter_col = alias_col - 2;
        let width = usize::from(buffer.area.width);
        let gutter_idx = row * width + gutter_col;

        assert_eq!(buffer.content()[gutter_idx].symbol(), "│");
        assert_eq!(
            buffer.content()[gutter_idx].fg,
            cluster_color_for_token("2"),
            "gutter color should be derived from the logical cluster label"
        );
    }

    #[test]
    fn overview_renders_default_emphasis_without_underline() {
        let mut app = crate::app::AppState::new(
            default_settings(),
            ColumnRegistry::load(None, true, crate::model::SortMode::Address),
        );
        app.view_mode = ViewMode::Flat;

        let mut a = InstanceState::new("a".into(), "127.0.0.1:6379".into());
        a.ops_per_sec = Some(4);
        a.last_latency_ms = Some(0.25);
        a.max_latency_ms = 1.4;
        a.status = Status::Ok;
        a.last_updated = Some(std::time::Instant::now());

        let mut b = InstanceState::new("b".into(), "127.0.0.1:6380".into());
        b.ops_per_sec = Some(99);
        b.last_latency_ms = Some(0.95);
        b.max_latency_ms = 0.8;
        b.status = Status::Ok;
        b.last_updated = Some(std::time::Instant::now());

        let mut c = InstanceState::new("c".into(), "127.0.0.1:6381".into());
        c.ops_per_sec = Some(3);
        c.last_latency_ms = Some(0.40);
        c.max_latency_ms = 2.1;
        c.status = Status::Ok;
        c.last_updated = Some(std::time::Instant::now());

        app.apply_update(a);
        app.apply_update(b);
        app.apply_update(c);

        let backend = TestBackend::new(100, 12);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        terminal
            .draw(|frame| draw(frame, &app))
            .expect("overview draw succeeds");

        let buffer = terminal.backend().buffer().clone();
        let lines = buffer_lines(&buffer);
        let ops_row = lines
            .iter()
            .position(|line| line.contains("6380") && line.contains("99"))
            .expect("ops winner row rendered");
        let ops_col = char_column(&lines[ops_row], "99");
        let lat_row = lines
            .iter()
            .position(|line| line.contains("6381") && line.contains("2.10"))
            .expect("latency max row rendered");
        let lat_col = char_column(&lines[lat_row], "2.10");
        let width = usize::from(buffer.area.width);
        let ops_idx = ops_row * width + ops_col;
        let lat_max_idx = lat_row * width + lat_col;

        assert!(
            !buffer.content()[ops_idx].modifier.contains(Modifier::BOLD),
            "ops winner should use the shipped non-bold default emphasis style"
        );
        assert!(
            !buffer.content()[lat_max_idx]
                .modifier
                .contains(Modifier::BOLD),
            "latency max winner should use the shipped non-bold default emphasis style"
        );
        assert!(
            !buffer.content()[lat_max_idx]
                .modifier
                .contains(Modifier::UNDERLINED),
            "latency max winner should not be underlined by default"
        );
    }

    #[test]
    fn overview_renders_configured_emphasis_modifiers_and_color() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            r#"
[view.overview.emphasis_style]
italic = true
foreground_color = "yellow"

[columns.ops.emphasis_style]
underlined = true
"#,
        )
        .expect("write config");

        let mut app = crate::app::AppState::new(
            default_settings(),
            ColumnRegistry::load(Some(&path), false, crate::model::SortMode::Address),
        );
        app.view_mode = ViewMode::Flat;

        let mut a = InstanceState::new("a".into(), "127.0.0.1:6379".into());
        a.ops_per_sec = Some(4);
        a.last_latency_ms = Some(0.25);
        a.max_latency_ms = 1.4;
        a.status = Status::Ok;
        a.last_updated = Some(std::time::Instant::now());

        let mut b = InstanceState::new("b".into(), "127.0.0.1:6380".into());
        b.ops_per_sec = Some(99);
        b.last_latency_ms = Some(0.95);
        b.max_latency_ms = 0.8;
        b.status = Status::Ok;
        b.last_updated = Some(std::time::Instant::now());

        let mut c = InstanceState::new("c".into(), "127.0.0.1:6381".into());
        c.ops_per_sec = Some(3);
        c.last_latency_ms = Some(0.40);
        c.max_latency_ms = 2.1;
        c.status = Status::Ok;
        c.last_updated = Some(std::time::Instant::now());

        app.apply_update(a);
        app.apply_update(b);
        app.apply_update(c);

        let backend = TestBackend::new(100, 12);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        terminal
            .draw(|frame| draw(frame, &app))
            .expect("overview draw succeeds");

        let buffer = terminal.backend().buffer().clone();
        let lines = buffer_lines(&buffer);
        let ops_row = lines
            .iter()
            .position(|line| line.contains("6380") && line.contains("99"))
            .expect("ops winner row rendered");
        let ops_col = char_column(&lines[ops_row], "99");
        let lat_row = lines
            .iter()
            .position(|line| line.contains("6381") && line.contains("2.10"))
            .expect("latency max row rendered");
        let lat_col = char_column(&lines[lat_row], "2.10");
        let width = usize::from(buffer.area.width);
        let ops_idx = ops_row * width + ops_col;
        let lat_max_idx = lat_row * width + lat_col;

        assert!(
            !buffer.content()[ops_idx].modifier.contains(Modifier::BOLD),
            "ops winner should preserve the default non-bold emphasis unless explicitly enabled"
        );
        assert!(
            buffer.content()[ops_idx]
                .modifier
                .contains(Modifier::UNDERLINED),
            "ops winner should apply per-column underline"
        );
        assert!(
            buffer.content()[ops_idx]
                .modifier
                .contains(Modifier::ITALIC),
            "ops winner should inherit global italic"
        );
        assert_eq!(buffer.content()[ops_idx].fg, Color::Yellow);

        assert!(
            buffer.content()[lat_max_idx]
                .modifier
                .contains(Modifier::ITALIC),
            "latency winner should inherit global italic"
        );
        assert_eq!(buffer.content()[lat_max_idx].fg, Color::Yellow);
    }
}
