use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::{Parser, ValueEnum};

use crate::config;
use crate::model::{RuntimeSettings, SortMode, Target, TargetProtocol, ViewMode};
use crate::target_addr::normalize_tcp_addr;

const VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " [",
    env!("REDITOP_BUILD_DATE"),
    "] (",
    env!("REDITOP_GIT_SHA"),
    ")"
);

#[derive(Debug, Clone)]
pub struct LaunchConfig {
    pub settings: RuntimeSettings,
    pub targets: Vec<Target>,
    pub verbose: bool,
}

#[derive(Debug, Parser)]
#[command(
    name = "reditop",
    version = VERSION,
    about = "htop-like TUI for Redis/Valkey"
)]
struct Cli {
    #[arg(value_name = "TARGETS")]
    targets: Vec<String>,

    #[arg(long = "unix", value_name = "PATH")]
    unix_targets: Vec<String>,

    #[arg(long = "tcp", value_name = "HOST:PORT")]
    tcp_targets: Vec<String>,

    #[arg(short = 'c', long = "config", value_name = "PATH")]
    config: Option<PathBuf>,

    #[arg(long = "refresh", value_name = "DURATION")]
    refresh: Option<String>,

    #[arg(long = "connect-timeout", value_name = "DURATION")]
    connect_timeout: Option<String>,

    #[arg(long = "command-timeout", value_name = "DURATION")]
    command_timeout: Option<String>,

    #[arg(short = 'n', long = "concurrency", value_name = "N")]
    concurrency: Option<usize>,

    #[arg(long = "view", value_enum)]
    view: Option<CliViewMode>,

    #[arg(long = "sort", value_enum)]
    sort: Option<CliSortMode>,

    #[arg(long = "no-config")]
    no_config: bool,

    #[arg(short = 'a', long = "auth", value_name = "PASSWORD")]
    auth: Option<String>,

    #[arg(long = "user", value_name = "USERNAME")]
    user: Option<String>,

    #[arg(short = 'v', long = "verbose")]
    verbose: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliViewMode {
    Flat,
    Tree,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliSortMode {
    Address,
    Mem,
    Ops,
    Lat,
    Status,
}

pub fn build_launch_config() -> Result<LaunchConfig> {
    let args = Cli::parse();

    let base_settings = config::default_settings();
    let (file_overrides, mut merged_targets) =
        config::load_config(args.config.as_deref(), args.no_config)?;

    let mut settings = config::apply_overrides(base_settings, &file_overrides);

    if let Some(refresh) = args.refresh.as_deref() {
        settings.refresh_interval = humantime::parse_duration(refresh)?;
    }
    if let Some(timeout) = args.connect_timeout.as_deref() {
        settings.connect_timeout = humantime::parse_duration(timeout)?;
    }
    if let Some(timeout) = args.command_timeout.as_deref() {
        settings.command_timeout = humantime::parse_duration(timeout)?;
    }
    if let Some(limit) = args.concurrency {
        settings.concurrency_limit = limit.max(1);
    }
    if let Some(view) = args.view {
        settings.default_view = match view {
            CliViewMode::Flat => ViewMode::Flat,
            CliViewMode::Tree => ViewMode::Tree,
        };
    }
    if let Some(sort) = args.sort {
        settings.default_sort = match sort {
            CliSortMode::Address => SortMode::Address,
            CliSortMode::Mem => SortMode::Mem,
            CliSortMode::Ops => SortMode::Ops,
            CliSortMode::Lat => SortMode::Lat,
            CliSortMode::Status => SortMode::Status,
        };
    }

    let mut cli_targets = Vec::new();
    cli_targets.extend(
        args.targets
            .into_iter()
            .map(|raw| parse_target_string(&raw)),
    );
    cli_targets.extend(args.unix_targets.into_iter().map(|raw| {
        Ok(Target {
            alias: None,
            addr: raw,
            protocol: TargetProtocol::Unix,
            username: args.user.clone(),
            password: args.auth.clone(),
            tags: Vec::new(),
        })
    }));
    cli_targets.extend(args.tcp_targets.into_iter().map(|raw| {
        let addr = normalize_tcp_addr(&raw)?;
        Ok(Target {
            alias: None,
            addr,
            protocol: TargetProtocol::Tcp,
            username: args.user.clone(),
            password: args.auth.clone(),
            tags: Vec::new(),
        })
    }));

    for item in &mut merged_targets {
        if item.username.is_none() {
            item.username = args.user.clone();
        }
        if item.password.is_none() {
            item.password = args.auth.clone();
        }
    }

    for maybe in cli_targets {
        let mut parsed = maybe?;
        if parsed.username.is_none() {
            parsed.username = args.user.clone();
        }
        if parsed.password.is_none() {
            parsed.password = args.auth.clone();
        }
        merged_targets.push(parsed);
    }

    merged_targets = dedupe_targets(merged_targets);

    if merged_targets.is_empty() {
        bail!("no Redis targets provided (CLI/config)");
    }

    Ok(LaunchConfig {
        settings,
        targets: merged_targets,
        verbose: args.verbose,
    })
}

fn parse_target_string(raw: &str) -> Result<Target> {
    let (protocol, addr) = if let Some(path) = raw.strip_prefix("unix:") {
        (TargetProtocol::Unix, path.to_string())
    } else if raw.contains('/') {
        (TargetProtocol::Unix, raw.to_string())
    } else {
        (TargetProtocol::Tcp, normalize_tcp_addr(raw)?)
    };

    Ok(Target {
        alias: None,
        addr,
        protocol,
        username: None,
        password: None,
        tags: Vec::new(),
    })
}

fn dedupe_targets(input: Vec<Target>) -> Vec<Target> {
    let mut by_key: HashMap<(String, TargetProtocol), Target> = HashMap::new();
    for target in input {
        by_key.insert((target.addr.clone(), target.protocol), target);
    }
    let mut out: Vec<Target> = by_key.into_values().collect();
    out.sort_by(|a, b| a.addr.cmp(&b.addr));
    out
}

#[cfg(test)]
mod tests {
    use super::VERSION;

    #[test]
    fn version_string_contains_build_metadata() {
        assert!(VERSION.starts_with(env!("CARGO_PKG_VERSION")));
        assert!(VERSION.contains(" ["));
        assert!(VERSION.contains("] ("));
        assert!(VERSION.ends_with(')'));
    }
}
