use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use redis::{AsyncConnectionConfig, Client, Value};

use crate::model::{RuntimeSettings, Target, TargetProtocol};
use crate::parse::collect_cluster_shard_addresses;

pub async fn discover_cluster_targets(
    seeds: &[Target],
    settings: &RuntimeSettings,
) -> Result<Vec<Target>> {
    if seeds.is_empty() {
        return Ok(Vec::new());
    }

    let mut discovered = Vec::new();
    let mut errors = Vec::new();

    for seed in seeds {
        match discover_from_seed(seed, settings).await {
            Ok(nodes) => {
                if nodes.is_empty() {
                    errors.push(format!(
                        "{}: CLUSTER SHARDS returned no node addresses",
                        seed.addr
                    ));
                    continue;
                }
                discovered.extend(nodes.into_iter().map(|addr| Target {
                    alias: None,
                    addr,
                    protocol: TargetProtocol::Tcp,
                    username: seed.username.clone(),
                    password: seed.password.clone(),
                    tags: Vec::new(),
                }));
            }
            Err(err) => errors.push(format!("{}: {err}", seed.addr)),
        }
    }

    if discovered.is_empty() {
        bail!(
            "failed to discover cluster nodes from --cluster seed(s): {}",
            errors.join("; ")
        );
    }

    Ok(discovered)
}

async fn discover_from_seed(seed: &Target, settings: &RuntimeSettings) -> Result<Vec<String>> {
    if seed.protocol != TargetProtocol::Tcp {
        bail!("cluster discovery only supports TCP seeds");
    }

    let client = Client::open(redis_url(seed))
        .with_context(|| format!("invalid redis URL for seed {}", seed.addr))?;

    let config = AsyncConnectionConfig::new()
        .set_connection_timeout(settings.connect_timeout)
        .set_response_timeout(settings.command_timeout);

    let mut conn = client
        .get_multiplexed_async_connection_with_config(&config)
        .await
        .with_context(|| format!("failed to connect to {}", seed.addr))?;

    let shards: Value = redis::cmd("CLUSTER")
        .arg("SHARDS")
        .query_async(&mut conn)
        .await
        .with_context(|| format!("CLUSTER SHARDS failed on {}", seed.addr))?;

    let out: BTreeSet<String> = collect_cluster_shard_addresses(&shards);
    Ok(out.into_iter().collect())
}

fn redis_url(target: &Target) -> String {
    if let (Some(user), Some(pass)) = (&target.username, &target.password) {
        format!(
            "redis://{}:{}@{}/",
            url_encode(user),
            url_encode(pass),
            target.addr
        )
    } else if let Some(pass) = &target.password {
        format!("redis://:{}@{}/", url_encode(pass), target.addr)
    } else {
        format!("redis://{}/", target.addr)
    }
}

fn url_encode(raw: &str) -> String {
    raw.replace('%', "%25")
        .replace(':', "%3A")
        .replace('@', "%40")
        .replace('/', "%2F")
        .replace('?', "%3F")
        .replace('&', "%26")
        .replace('=', "%3D")
        .replace(' ', "%20")
}
