use std::io::{self, Stdout};
use std::time::Duration;

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, Tabs, Wrap};

use crate::app::{ActiveView, AppState};
use crate::cli::LaunchConfig;
use crate::poller;

pub async fn run(launch: LaunchConfig) -> Result<()> {
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
    let result = run_loop(&mut terminal, launch).await;
    restore_terminal(&mut terminal)?;
    result
}

async fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    launch: LaunchConfig,
) -> Result<()> {
    let mut app = AppState::new(launch.settings.clone());
    let (mut updates_rx, refresh_tx) = poller::start(launch.targets, launch.settings.clone());

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
                    _ => {}
                }
                continue;
            }

            match key.code {
                KeyCode::Char('q') => app.should_quit = true,
                KeyCode::Char('H') => app.open_help_view(),
                KeyCode::Char('?') => app.show_help = !app.show_help,
                KeyCode::Char('t') if app.active_view == ActiveView::Overview => {
                    app.view_mode = app.view_mode.toggle();
                    app.clamp_selection();
                }
                KeyCode::Char('s') if app.active_view == ActiveView::Overview => {
                    app.sort_mode = app.sort_mode.next();
                    app.clamp_selection();
                }
                KeyCode::Char('/') if app.active_view == ActiveView::Overview => {
                    app.is_filtering = true
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
                    app.active_view = ActiveView::Overview
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

fn draw(frame: &mut ratatui::Frame<'_>, app: &AppState) {
    match app.active_view {
        ActiveView::Overview => draw_overview(frame, app),
        ActiveView::Detail => draw_detail(frame, app),
        ActiveView::Help => draw_help_page(frame),
    }

    if app.show_help {
        draw_help_overlay(frame, frame.area());
    }
}

fn draw_overview(frame: &mut ratatui::Frame<'_>, app: &AppState) {
    let area = frame.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Min(5),
            Constraint::Length(1),
        ])
        .split(area);

    let header = Paragraph::new(format!(
        "redis-top  refresh={}  view={:?}  sort={:?}  filter={}{}",
        humantime::format_duration(app.settings.refresh_interval),
        app.view_mode,
        app.sort_mode,
        if app.filter.is_empty() {
            "<none>"
        } else {
            &app.filter
        },
        if app.is_filtering { " (editing)" } else { "" }
    ))
    .block(Block::default().borders(Borders::ALL).title("Overview"));
    frame.render_widget(header, chunks[0]);

    let rows = app.visible_rows();
    let show_address = app.show_address_column();
    let table_rows: Vec<Row<'_>> = rows
        .iter()
        .map(|row| {
            let mut cells = vec![Cell::from(row.alias_or_addr.clone())];
            if show_address {
                cells.push(Cell::from(row.address.clone()));
            }
            cells.extend([
                Cell::from(row.node_type.clone()),
                Cell::from(row.cluster.clone()),
                Cell::from(row.mem_used.clone()),
                Cell::from(row.maxmem_pct.clone()),
                Cell::from(row.ops.clone()),
                Cell::from(row.lat_last.clone()),
                Cell::from(row.lat_max.clone()),
                Cell::from(row.status.clone()),
            ]);

            let base = Row::new(cells);
            if row.stale {
                base.style(Style::default().add_modifier(Modifier::DIM))
            } else {
                base
            }
        })
        .collect();

    let constraints = if show_address {
        vec![
            Constraint::Length(20),
            Constraint::Length(21),
            Constraint::Length(10),
            Constraint::Length(12),
            Constraint::Length(11),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(10),
        ]
    } else {
        vec![
            Constraint::Length(29),
            Constraint::Length(10),
            Constraint::Length(12),
            Constraint::Length(11),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(10),
        ]
    };

    let header = if show_address {
        Row::new(vec![
            "Alias", "Address", "Type", "Cluster", "Mem", "Max%", "Ops/s", "Lat", "LatMax",
            "Status",
        ])
    } else {
        Row::new(vec![
            "Alias", "Type", "Cluster", "Mem", "Max%", "Ops/s", "Lat", "LatMax", "Status",
        ])
    };

    let selected_style = Style::default().add_modifier(Modifier::REVERSED);
    let table = Table::new(table_rows, constraints)
        .header(header)
        .block(Block::default().borders(Borders::ALL))
        .row_highlight_style(selected_style)
        .highlight_symbol("> ");

    let mut state = ratatui::widgets::TableState::default().with_selected(Some(app.selected_index));
    frame.render_stateful_widget(table, chunks[1], &mut state);

    let status_line = if let Some(key) = app.selected_key() {
        app.instances
            .get(&key)
            .and_then(|instance| instance.last_error.clone())
            .unwrap_or_default()
    } else {
        String::new()
    };
    frame.render_widget(Paragraph::new(status_line), chunks[2]);
}

fn draw_detail(frame: &mut ratatui::Frame<'_>, app: &AppState) {
    let Some(selected_key) = app.selected_key() else {
        frame.render_widget(
            Paragraph::new("No instance selected")
                .block(Block::default().borders(Borders::ALL).title("Detail")),
            frame.area(),
        );
        return;
    };
    let Some(instance) = app.instances.get(&selected_key) else {
        return;
    };

    let area = frame.area();
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
            .map(format_with_commas)
            .unwrap_or_else(|| "-".to_string())
    );
    frame.render_widget(
        Paragraph::new(title).block(Block::default().borders(Borders::ALL).title("Instance")),
        chunks[0],
    );

    let tabs = Tabs::new(vec!["Summary", "Latency", "Info Raw"])
        .block(Block::default().borders(Borders::ALL))
        .select(app.detail_tab)
        .highlight_style(Style::default().add_modifier(Modifier::BOLD));
    frame.render_widget(tabs, chunks[1]);

    match app.detail_tab {
        0 => {
            let hits = instance.detail.keyspace_hits.unwrap_or(0);
            let misses = instance.detail.keyspace_misses.unwrap_or(0);
            let hit_rate = if hits + misses == 0 {
                0.0
            } else {
                hits as f64 / (hits + misses) as f64 * 100.0
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
                    .block(Block::default().borders(Borders::ALL).title("Summary"))
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
                        .map(|v| format!("{v:.2}"))
                        .unwrap_or_else(|| "-".to_string()),
                ),
                ("max_latency_ms", format!("{:.2}", instance.max_latency_ms)),
                ("avg_latency_ms", format!("{:.2}", instance.avg_latency_ms)),
                (
                    "window_samples",
                    format_with_commas(instance.latency_window.len() as u64),
                ),
            ]);
            frame.render_widget(
                Paragraph::new(body).block(Block::default().borders(Borders::ALL).title("Latency")),
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
                    .block(Block::default().borders(Borders::ALL).title("Info Raw"))
                    .wrap(Wrap { trim: false }),
                chunks[2],
            );
        }
    }
}

fn format_aligned_rows(rows: &[(&str, String)]) -> String {
    let width = rows.iter().map(|(label, _)| label.len()).max().unwrap_or(0);
    rows.iter()
        .map(|(label, value)| format!("{label:width$} : {value}", width = width))
        .collect::<Vec<String>>()
        .join("\n")
}

fn format_optional_u64(value: Option<u64>) -> String {
    value
        .map(format_with_commas)
        .unwrap_or_else(|| "-".to_string())
}

fn format_optional_bytes(value: Option<u64>) -> String {
    value
        .map(|bytes| format!("{} ({})", format_with_commas(bytes), human_bytes(bytes)))
        .unwrap_or_else(|| "-".to_string())
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
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut idx = 0;
    while value >= 1024.0 && idx + 1 < UNITS.len() {
        value /= 1024.0;
        idx += 1;
    }

    if idx == 0 {
        format!("{bytes} {}", UNITS[idx])
    } else {
        format!("{value:.1} {}", UNITS[idx])
    }
}

fn draw_help_page(frame: &mut ratatui::Frame<'_>) {
    let rows: Vec<Row<'_>> = help_bindings()
        .iter()
        .map(|(keys, action)| Row::new(vec![Cell::from(*keys), Cell::from(*action)]))
        .collect();
    let table = Table::new(rows, [Constraint::Length(24), Constraint::Min(20)])
        .header(
            Row::new(vec!["Keys", "Action"]).style(Style::default().add_modifier(Modifier::BOLD)),
        )
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Help (Esc to go back)"),
        );
    frame.render_widget(table, frame.area());
}

fn help_bindings() -> &'static [(&'static str, &'static str)] {
    &[
        ("q", "Quit"),
        ("H", "Open this help page"),
        ("Esc", "Back from detail/help or stop filter editing"),
        ("Enter", "Open detail view from overview"),
        ("Tab/Right", "Next detail panel"),
        ("Left", "Previous detail panel"),
        ("Up/Down", "Move selection in overview"),
        ("?", "Toggle help overlay"),
        ("r", "Refresh now"),
        ("t", "Toggle flat/tree view in overview"),
        ("s", "Cycle sort mode in overview"),
        ("/", "Start filter input in overview"),
        ("Backspace", "Delete filter character while editing"),
    ]
}

fn draw_help_overlay(frame: &mut ratatui::Frame<'_>, area: Rect) {
    let width = area.width.saturating_mul(80) / 100;
    let height = area.height.saturating_mul(70) / 100;
    let popup = Rect {
        x: area.x + (area.width.saturating_sub(width)) / 2,
        y: area.y + (area.height.saturating_sub(height)) / 2,
        width,
        height,
    };

    frame.render_widget(Clear, popup);
    let text = "q quit\nH open help page\nEsc back\nEnter open detail\nTab/Left/Right cycle detail panels\nUp/Down move selection\n? toggle help overlay\nr refresh now\nt toggle flat/tree\ns toggle sort\n/ filter in overview";
    frame.render_widget(
        Paragraph::new(text)
            .block(Block::default().borders(Borders::ALL).title("Help"))
            .wrap(Wrap { trim: false }),
        popup,
    );
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
    use super::{format_aligned_rows, format_with_commas, help_bindings};

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
}
