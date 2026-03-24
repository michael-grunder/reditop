use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, ValueEnum};

use crate::config;
use crate::discovery::DiscoveryTarget;
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
    pub discovery_targets: Vec<DiscoveryTarget>,
    pub once: bool,
    pub verbose: bool,
    pub config_path: Option<PathBuf>,
    pub no_default_config: bool,
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

    #[arg(long = "cluster", value_name = "HOST:PORT")]
    cluster_targets: Vec<String>,

    #[arg(long = "host", value_name = "HOST")]
    discovery_hosts: Vec<String>,

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

    #[arg(long = "once")]
    once: bool,

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
    Alias,
    Address,
    Type,
    Cluster,
    Memory,
    Mem,
    Ops,
    Lat,
    Latmax,
    Status,
}

#[allow(clippy::too_many_lines)]
pub fn build_launch_config() -> Result<LaunchConfig> {
    build_launch_config_from(Cli::parse())
}

#[allow(clippy::too_many_lines)]
fn build_launch_config_from(args: Cli) -> Result<LaunchConfig> {
    let base_settings = config::default_settings();
    let loaded_config = config::load_config(args.config.as_deref(), args.no_config)?;
    let mut merged_targets = loaded_config.targets.clone();
    let config_target_count = merged_targets.len();

    let mut settings = config::apply_overrides(base_settings, &loaded_config.overrides);

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
            CliSortMode::Alias => SortMode::Alias,
            CliSortMode::Address => SortMode::Address,
            CliSortMode::Type => SortMode::Type,
            CliSortMode::Cluster => SortMode::Cluster,
            CliSortMode::Memory | CliSortMode::Mem => SortMode::Mem,
            CliSortMode::Ops => SortMode::Ops,
            CliSortMode::Lat => SortMode::Lat,
            CliSortMode::Latmax => SortMode::LatMax,
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
    let mut parsed_cluster_seeds: Vec<Target> = args
        .cluster_targets
        .into_iter()
        .map(|raw| {
            let addr = normalize_tcp_addr(&raw)?;
            Ok(Target {
                alias: None,
                addr,
                protocol: TargetProtocol::Tcp,
                username: args.user.clone(),
                password: args.auth.clone(),
                tags: Vec::new(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    for item in &mut merged_targets {
        if item.username.is_none() {
            item.username.clone_from(&args.user);
        }
        if item.password.is_none() {
            item.password.clone_from(&args.auth);
        }
    }

    for maybe in cli_targets {
        let mut parsed = maybe?;
        if parsed.username.is_none() {
            parsed.username.clone_from(&args.user);
        }
        if parsed.password.is_none() {
            parsed.password.clone_from(&args.auth);
        }
        merged_targets.push(parsed);
    }

    for seed in &mut parsed_cluster_seeds {
        if seed.username.is_none() {
            seed.username.clone_from(&args.user);
        }
        if seed.password.is_none() {
            seed.password.clone_from(&args.auth);
        }
    }

    merged_targets.extend(parsed_cluster_seeds);

    merged_targets = dedupe_targets(merged_targets);
    let discovery_targets = dedupe_discovery_targets(build_discovery_targets(
        &merged_targets,
        config_target_count,
        loaded_config.still_autodiscover,
        &args.discovery_hosts,
        args.user.clone(),
        args.auth.clone(),
    ));

    Ok(LaunchConfig {
        settings,
        targets: merged_targets,
        discovery_targets,
        once: args.once,
        verbose: args.verbose,
        config_path: args.config,
        no_default_config: args.no_config,
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

fn build_discovery_targets(
    explicit_targets: &[Target],
    config_target_count: usize,
    config_still_autodiscover: bool,
    discovery_hosts: &[String],
    username: Option<String>,
    password: Option<String>,
) -> Vec<DiscoveryTarget> {
    let mut out = discovery_hosts
        .iter()
        .map(|host| DiscoveryTarget {
            host: host.trim().to_string(),
            username: username.clone(),
            password: password.clone(),
        })
        .filter(|target| !target.host.is_empty())
        .collect::<Vec<_>>();

    out.extend(
        explicit_targets
            .iter()
            .filter(|target| target.protocol == TargetProtocol::Tcp)
            .filter_map(|target| {
                crate::target_addr::tcp_host(&target.addr).map(|host| DiscoveryTarget {
                    host,
                    username: target.username.clone(),
                    password: target.password.clone(),
                })
            }),
    );

    let cli_has_explicit_targets = explicit_targets.len() > config_target_count;
    let only_config_targets = !explicit_targets.is_empty() && !cli_has_explicit_targets;
    let allow_default_autodiscovery = config_still_autodiscover || !only_config_targets;

    if out.is_empty() && !cli_has_explicit_targets && allow_default_autodiscovery {
        out.push(DiscoveryTarget::localhost(username, password));
    }

    out
}

fn dedupe_discovery_targets(input: Vec<DiscoveryTarget>) -> Vec<DiscoveryTarget> {
    let mut by_host: HashMap<String, DiscoveryTarget> = HashMap::new();
    for target in input {
        let localhost_probe = DiscoveryTarget {
            host: target.host.clone(),
            username: None,
            password: None,
        };
        let key = if localhost_probe.is_localhost() {
            "127.0.0.1".to_string()
        } else {
            target.host.to_ascii_lowercase()
        };
        by_host
            .entry(key)
            .and_modify(|existing| {
                if existing.username.is_none() {
                    existing.username.clone_from(&target.username);
                }
                if existing.password.is_none() {
                    existing.password.clone_from(&target.password);
                }
            })
            .or_insert(target);
    }
    let mut out: Vec<_> = by_host.into_values().collect();
    out.sort_by(|left, right| left.host.cmp(&right.host));
    out
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{DiscoveryTarget, VERSION, build_discovery_targets, dedupe_discovery_targets};
    use crate::model::{Target, TargetProtocol};

    #[test]
    fn version_string_contains_build_metadata() {
        assert!(VERSION.starts_with(env!("CARGO_PKG_VERSION")));
        assert!(VERSION.contains(" ["));
        assert!(VERSION.contains("] ("));
        assert!(VERSION.ends_with(')'));
    }

    #[test]
    fn discovery_defaults_to_localhost_when_only_config_targets_are_present() {
        let targets = vec![Target {
            alias: Some("saved".to_string()),
            addr: "127.0.0.1:6380".to_string(),
            protocol: TargetProtocol::Tcp,
            username: Some("default".to_string()),
            password: Some("secret".to_string()),
            tags: Vec::new(),
        }];

        let discovered = build_discovery_targets(&targets, targets.len(), true, &[], None, None);

        assert_eq!(discovered.len(), 1);
        assert_eq!(
            discovered[0],
            DiscoveryTarget::localhost(Some("default".to_string()), Some("secret".to_string()))
        );
    }

    #[test]
    fn discovery_can_be_disabled_when_only_config_targets_are_present() {
        let targets = vec![Target {
            alias: Some("saved".to_string()),
            addr: "127.0.0.1:6380".to_string(),
            protocol: TargetProtocol::Tcp,
            username: Some("default".to_string()),
            password: Some("secret".to_string()),
            tags: Vec::new(),
        }];

        let discovered = dedupe_discovery_targets(build_discovery_targets(
            &targets,
            targets.len(),
            false,
            &[],
            None,
            None,
        ));

        assert_eq!(discovered.len(), 1);
        assert_eq!(
            discovered[0],
            DiscoveryTarget::localhost(Some("default".to_string()), Some("secret".to_string()))
        );
    }

    #[test]
    fn explicit_cli_targets_still_disable_default_discovery() {
        let config_target = Target {
            alias: Some("saved".to_string()),
            addr: "127.0.0.1:6380".to_string(),
            protocol: TargetProtocol::Tcp,
            username: Some("default".to_string()),
            password: Some("secret".to_string()),
            tags: Vec::new(),
        };
        let cli_target = Target {
            alias: None,
            addr: "127.0.0.1:6379".to_string(),
            protocol: TargetProtocol::Tcp,
            username: None,
            password: None,
            tags: Vec::new(),
        };

        let discovered = dedupe_discovery_targets(build_discovery_targets(
            &[config_target, cli_target],
            1,
            true,
            &[],
            None,
            None,
        ));

        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered[0].host, "127.0.0.1");
    }

    #[test]
    fn once_flag_enables_non_interactive_launch() {
        let cli = super::Cli::parse_from(["reditop", "--once"]);
        let launch = super::build_launch_config_from(cli).expect("launch config should parse");

        assert!(launch.once);
    }
}
