use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::model::{RuntimeSettings, SortMode, Target, TargetProtocol, UiColor, UiTheme, ViewMode};
use crate::target_addr::normalize_tcp_addr;

#[derive(Debug, Deserialize, Default)]
struct FileConfig {
    global: Option<GlobalConfig>,
    theme: Option<ThemeConfig>,
    targets: Option<Vec<ConfigTarget>>,
}

#[derive(Debug, Deserialize, Default)]
struct GlobalConfig {
    refresh_interval_ms: Option<u64>,
    connect_timeout_ms: Option<u64>,
    command_timeout_ms: Option<u64>,
    concurrency_limit: Option<usize>,
    view_default: Option<String>,
    sort_default: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct ConfigTarget {
    alias: Option<String>,
    addr: Option<String>,
    protocol: Option<String>,
    username: Option<String>,
    password: Option<String>,
    tags: Option<Vec<String>>,
    enabled: Option<bool>,
}

#[allow(clippy::struct_field_names)]
#[derive(Debug, Deserialize, Default)]
struct ThemeConfig {
    background_color: Option<String>,
    foreground_color: Option<String>,
    carat_color: Option<String>,
    caret_color: Option<String>,
    warning_color: Option<String>,
    critical_color: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct RuntimeOverrides {
    pub refresh_interval_ms: Option<u64>,
    pub connect_timeout_ms: Option<u64>,
    pub command_timeout_ms: Option<u64>,
    pub concurrency_limit: Option<usize>,
    pub view_default: Option<ViewMode>,
    pub sort_default: Option<SortMode>,
    pub ui_theme: Option<UiTheme>,
}

pub fn default_settings() -> RuntimeSettings {
    RuntimeSettings {
        refresh_interval: std::time::Duration::from_secs(1),
        connect_timeout: std::time::Duration::from_millis(300),
        command_timeout: std::time::Duration::from_millis(500),
        concurrency_limit: 16,
        default_view: ViewMode::Tree,
        default_sort: SortMode::Address,
        ui_theme: UiTheme::default(),
    }
}

pub fn load_config(
    path: Option<&Path>,
    no_default_config: bool,
) -> Result<(RuntimeOverrides, Vec<Target>)> {
    let maybe_path = path.map_or_else(
        || {
            if no_default_config {
                None
            } else {
                find_default_config_path()
            }
        },
        |explicit| Some(explicit.to_path_buf()),
    );

    let Some(path) = maybe_path else {
        return Ok((RuntimeOverrides::default(), Vec::new()));
    };

    let content = fs::read_to_string(&path)
        .with_context(|| format!("failed to read config file {}", path.display()))?;
    let parsed: FileConfig = toml::from_str(&content)
        .with_context(|| format!("failed to parse TOML config {}", path.display()))?;

    let mut targets = Vec::new();
    if let Some(entries) = parsed.targets {
        for entry in entries {
            if entry.enabled == Some(false) {
                continue;
            }

            let Some(addr) = entry.addr else {
                eprintln!(
                    "warning: skipping target with missing addr in {}",
                    path.display()
                );
                continue;
            };
            let addr = addr.trim().to_string();
            if addr.is_empty() {
                eprintln!(
                    "warning: skipping target with empty addr in {}",
                    path.display()
                );
                continue;
            }

            let protocol = parse_protocol(entry.protocol.as_deref(), &addr)?;
            let addr = match protocol {
                TargetProtocol::Tcp => normalize_tcp_addr(&addr)?,
                TargetProtocol::Unix => addr,
            };
            targets.push(Target {
                alias: entry.alias,
                addr,
                protocol,
                username: entry.username,
                password: entry.password,
                tags: entry.tags.unwrap_or_default(),
            });
        }
    }

    let global = parsed.global.unwrap_or_default();
    Ok((
        RuntimeOverrides {
            refresh_interval_ms: global.refresh_interval_ms,
            connect_timeout_ms: global.connect_timeout_ms,
            command_timeout_ms: global.command_timeout_ms,
            concurrency_limit: global.concurrency_limit,
            view_default: parse_view(global.view_default.as_deref())?,
            sort_default: parse_sort(global.sort_default.as_deref())?,
            ui_theme: parse_theme(parsed.theme)?,
        },
        targets,
    ))
}

pub fn apply_overrides(mut base: RuntimeSettings, overrides: &RuntimeOverrides) -> RuntimeSettings {
    if let Some(ms) = overrides.refresh_interval_ms {
        base.refresh_interval = std::time::Duration::from_millis(ms);
    }
    if let Some(ms) = overrides.connect_timeout_ms {
        base.connect_timeout = std::time::Duration::from_millis(ms);
    }
    if let Some(ms) = overrides.command_timeout_ms {
        base.command_timeout = std::time::Duration::from_millis(ms);
    }
    if let Some(limit) = overrides.concurrency_limit {
        base.concurrency_limit = limit.max(1);
    }
    if let Some(view) = overrides.view_default {
        base.default_view = view;
    }
    if let Some(sort) = overrides.sort_default {
        base.default_sort = sort;
    }
    if let Some(theme) = overrides.ui_theme {
        base.ui_theme = theme;
    }
    base
}

fn find_default_config_path() -> Option<PathBuf> {
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

    candidates.into_iter().find(|path| path.exists())
}

fn parse_protocol(raw: Option<&str>, addr: &str) -> Result<TargetProtocol> {
    let proto = raw.unwrap_or_else(|| if addr.contains('/') { "unix" } else { "tcp" });

    match proto {
        "tcp" => Ok(TargetProtocol::Tcp),
        "unix" => Ok(TargetProtocol::Unix),
        other => bail!("unsupported target protocol: {other}"),
    }
}

fn parse_view(raw: Option<&str>) -> Result<Option<ViewMode>> {
    Ok(match raw {
        None => None,
        Some("flat") => Some(ViewMode::Flat),
        Some("tree") => Some(ViewMode::Tree),
        Some(other) => bail!("invalid view_default: {other}"),
    })
}

fn parse_sort(raw: Option<&str>) -> Result<Option<SortMode>> {
    Ok(match raw {
        None => None,
        Some("alias") => Some(SortMode::Alias),
        Some("address") => Some(SortMode::Address),
        Some("type") => Some(SortMode::Type),
        Some("cluster") => Some(SortMode::Cluster),
        Some("memory" | "mem") => Some(SortMode::Mem),
        Some("ops") => Some(SortMode::Ops),
        Some("latency" | "lat") => Some(SortMode::Lat),
        Some("latmax") => Some(SortMode::LatMax),
        Some("status") => Some(SortMode::Status),
        Some(other) => bail!("invalid sort_default: {other}"),
    })
}

fn parse_theme(raw: Option<ThemeConfig>) -> Result<Option<UiTheme>> {
    let Some(theme_raw) = raw else {
        return Ok(None);
    };

    let mut theme = UiTheme::default();
    if let Some(raw_color) = theme_raw.background_color.as_deref() {
        theme.background = parse_color(raw_color, "theme.background_color")?;
    }
    if let Some(raw_color) = theme_raw.foreground_color.as_deref() {
        theme.foreground = parse_color(raw_color, "theme.foreground_color")?;
    }
    if let Some(raw_color) = theme_raw.warning_color.as_deref() {
        theme.warning = parse_color(raw_color, "theme.warning_color")?;
    }
    if let Some(raw_color) = theme_raw.critical_color.as_deref() {
        theme.critical = parse_color(raw_color, "theme.critical_color")?;
    }
    if let Some(raw_color) = theme_raw.caret_color.as_deref() {
        theme.carat = parse_color(raw_color, "theme.caret_color")?;
    }
    if let Some(raw_color) = theme_raw.carat_color.as_deref() {
        theme.carat = parse_color(raw_color, "theme.carat_color")?;
    }
    Ok(Some(theme))
}

fn parse_color(raw: &str, field: &str) -> Result<UiColor> {
    let normalized = raw.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "black" => Ok(UiColor::Black),
        "red" => Ok(UiColor::Red),
        "green" => Ok(UiColor::Green),
        "yellow" => Ok(UiColor::Yellow),
        "blue" => Ok(UiColor::Blue),
        "magenta" => Ok(UiColor::Magenta),
        "cyan" => Ok(UiColor::Cyan),
        "gray" | "grey" => Ok(UiColor::Gray),
        "white" => Ok(UiColor::White),
        _ => bail!(
            "invalid color for {field}: {raw} (supported: black, red, green, yellow, blue, magenta, cyan, gray, white)"
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{apply_overrides, default_settings, load_config};
    use crate::model::{UiColor, UiTheme};

    #[test]
    fn default_settings_include_default_theme() {
        let settings = default_settings();
        assert_eq!(settings.ui_theme, UiTheme::default());
    }

    #[test]
    fn load_config_parses_theme_colors() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            r#"
[theme]
background_color = "blue"
foreground_color = "gray"
carat_color = "yellow"
warning_color = "magenta"
critical_color = "red"
"#,
        )
        .expect("write config");

        let (overrides, targets) = load_config(Some(&path), false).expect("load config");
        assert!(targets.is_empty());

        let settings = apply_overrides(default_settings(), &overrides);
        assert_eq!(settings.refresh_interval, Duration::from_secs(1));
        assert_eq!(settings.ui_theme.background, UiColor::Blue);
        assert_eq!(settings.ui_theme.foreground, UiColor::Gray);
        assert_eq!(settings.ui_theme.carat, UiColor::Yellow);
        assert_eq!(settings.ui_theme.warning, UiColor::Magenta);
        assert_eq!(settings.ui_theme.critical, UiColor::Red);
    }

    #[test]
    fn load_config_theme_defaults_missing_values() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            r#"
[theme]
foreground_color = "cyan"
"#,
        )
        .expect("write config");

        let (overrides, _) = load_config(Some(&path), false).expect("load config");
        let settings = apply_overrides(default_settings(), &overrides);
        assert_eq!(settings.ui_theme.background, UiColor::Black);
        assert_eq!(settings.ui_theme.foreground, UiColor::Cyan);
        assert_eq!(settings.ui_theme.carat, UiColor::White);
    }
}
