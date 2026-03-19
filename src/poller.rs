use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use redis::{AsyncConnectionConfig, Client, ErrorKind, Value};
use tokio::sync::{Semaphore, mpsc};

use crate::model::{InstanceState, InstanceType, RuntimeSettings, Status, Target, TargetProtocol};
use crate::parse::{ClusterShard, parse_cluster_shards, parse_commandstats, parse_info};
use crate::target_addr::{canonical_host, strip_host};

pub fn start(
    targets: Vec<Target>,
    settings: RuntimeSettings,
) -> (mpsc::Receiver<InstanceState>, mpsc::Sender<()>) {
    let (update_tx, update_rx) = mpsc::channel(1024);
    let (refresh_tx, mut refresh_rx) = mpsc::channel::<()>(8);

    tokio::spawn(async move {
        let semaphore = Arc::new(Semaphore::new(settings.concurrency_limit.max(1)));
        let mut known_states: HashMap<String, InstanceState> = HashMap::new();
        let mut ticker = tokio::time::interval(settings.refresh_interval);

        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                maybe = refresh_rx.recv() => {
                    if maybe.is_none() {
                        break;
                    }
                }
            }

            let mut set = tokio::task::JoinSet::new();
            for target in &targets {
                let target = target.clone();
                let settings = settings.clone();
                let semaphore = semaphore.clone();
                let prior = known_states.get(&target.addr).cloned();
                set.spawn(async move {
                    let _permit = semaphore.acquire_owned().await.ok();
                    poll_one(&target, &settings, prior).await
                });
            }

            while let Some(result) = set.join_next().await {
                if let Ok(state) = result {
                    known_states.insert(state.key.clone(), state.clone());
                    if update_tx.send(state).await.is_err() {
                        return;
                    }
                }
            }
        }
    });

    (update_rx, refresh_tx)
}

async fn poll_one(
    target: &Target,
    settings: &RuntimeSettings,
    prior: Option<InstanceState>,
) -> InstanceState {
    let mut state =
        prior.unwrap_or_else(|| InstanceState::new(target.addr.clone(), target.addr.clone()));
    state.alias = target.alias.clone();
    state.addr = target.addr.clone();
    state.tags = target.tags.clone();

    let client = match Client::open(redis_url(target)) {
        Ok(client) => client,
        Err(err) => {
            apply_failure(&mut state, Status::Error, err.to_string());
            return state;
        }
    };

    let config = AsyncConnectionConfig::new()
        .set_connection_timeout(Some(settings.connect_timeout))
        .set_response_timeout(Some(settings.command_timeout));

    let mut conn = match client
        .get_multiplexed_async_connection_with_config(&config)
        .await
    {
        Ok(conn) => conn,
        Err(err) => {
            let (status, message) = classify_error(&err);
            apply_failure(&mut state, status, message);
            return state;
        }
    };

    let ping_start = Instant::now();
    if let Err(err) = redis::cmd("PING").query_async::<String>(&mut conn).await {
        let (status, message) = classify_error(&err);
        apply_failure(&mut state, status, message);
        return state;
    }
    let latency_ms = ping_start.elapsed().as_secs_f64() * 1000.0;

    let info: String = match redis::cmd("INFO").query_async(&mut conn).await {
        Ok(info) => info,
        Err(err) => {
            let (status, message) = classify_error(&err);
            apply_failure(&mut state, status, message);
            return state;
        }
    };

    let commandstats_info = redis::cmd("INFO")
        .arg("COMMANDSTATS")
        .query_async::<String>(&mut conn)
        .await
        .ok();

    apply_info_to_state(&mut state, &info, commandstats_info.as_deref());

    if state.detail.cluster_enabled
        && let Ok(shards) = redis::cmd("CLUSTER")
            .arg("SHARDS")
            .query_async::<Value>(&mut conn)
            .await
    {
        apply_cluster_shards_to_state(&mut state, target, &shards);
    }

    state.push_latency_sample(latency_ms);
    state.last_latency_ms = Some(latency_ms);
    state.status = Status::Ok;
    state.last_error = None;
    state.last_updated = Some(Instant::now());

    state
}

fn apply_info_to_state(state: &mut InstanceState, info_raw: &str, commandstats_raw: Option<&str>) {
    let info = parse_info(info_raw);
    state.info = info.flat_map();

    state.used_memory_bytes = info.get_u64("memory", "used_memory");
    state.maxmemory_bytes = info.get_u64("memory", "maxmemory");
    state.ops_per_sec = info.get_u64("stats", "instantaneous_ops_per_sec");

    state.detail.redis_version = info.get("server", "redis_version").map(str::to_string);
    state.detail.uptime_seconds = info.get_u64("server", "uptime_in_seconds");
    state.detail.used_memory_rss = info.get_u64("memory", "used_memory_rss");
    state.detail.total_commands_processed = info.get_u64("stats", "total_commands_processed");
    state.detail.connected_clients = info.get_u64("clients", "connected_clients");
    state.detail.blocked_clients = info.get_u64("clients", "blocked_clients");
    state.detail.keyspace_hits = info.get_u64("stats", "keyspace_hits");
    state.detail.keyspace_misses = info.get_u64("stats", "keyspace_misses");
    state.detail.evicted_keys = info.get_u64("stats", "evicted_keys");
    state.detail.expired_keys = info.get_u64("stats", "expired_keys");
    state.detail.role = info.get("replication", "role").map(str::to_string);
    state.detail.master_host = info.get("replication", "master_host").map(str::to_string);
    state.detail.master_port = info
        .get("replication", "master_port")
        .and_then(|v| v.parse::<u16>().ok());
    state.detail.cluster_enabled = info.get_bool_01("cluster", "cluster_enabled");
    state.detail.commandstats = commandstats_raw
        .map(parse_info)
        .map(|parsed| parse_commandstats(&parsed))
        .filter(|stats| !stats.is_empty())
        .unwrap_or_else(|| parse_commandstats(&info));
    state.detail.raw_info = Some(info_raw.to_string());

    if state.detail.cluster_enabled {
        state.kind = InstanceType::Cluster;
    } else {
        match state.detail.role.as_deref() {
            Some("master") => {
                state.kind = InstanceType::Primary;
                state.parent_addr = None;
            }
            Some("slave" | "replica") => {
                state.kind = InstanceType::Replica;
                state.parent_addr = match (&state.detail.master_host, state.detail.master_port) {
                    (Some(host), Some(port)) => Some(format!("{host}:{port}")),
                    _ => None,
                };
            }
            _ => {
                state.kind = InstanceType::Standalone;
                state.parent_addr = None;
            }
        }
    }
}

fn apply_failure(state: &mut InstanceState, status: Status, message: String) {
    state.status = status;
    state.last_error = Some(truncate_string(message, 80));
}

fn classify_error(error: &redis::RedisError) -> (Status, String) {
    let msg = error.to_string();

    if let Some(code) = error.code() {
        if code == "NOAUTH" || code == "WRONGPASS" {
            return (Status::AuthFail, msg);
        }
        if code == "LOADING" {
            return (Status::Loading, msg);
        }
    }

    match error.kind() {
        ErrorKind::AuthenticationFailed => (Status::AuthFail, msg),
        ErrorKind::Io if error.is_timeout() => (Status::Timeout, msg),
        ErrorKind::Io => (Status::Down, msg),
        _ => (Status::Error, msg),
    }
}

fn redis_url(target: &Target) -> String {
    match target.protocol {
        TargetProtocol::Tcp => {
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
        TargetProtocol::Unix => {
            let mut out = format!("redis+unix://{}", target.addr);
            let mut query = Vec::new();
            if let Some(user) = &target.username {
                query.push(format!("user={}", url_encode(user)));
            }
            if let Some(pass) = &target.password {
                query.push(format!("pass={}", url_encode(pass)));
            }
            if !query.is_empty() {
                out.push('?');
                out.push_str(&query.join("&"));
            }
            out
        }
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

fn truncate_string(input: String, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input;
    }
    input.chars().take(max_chars).collect()
}

fn apply_cluster_shards_to_state(state: &mut InstanceState, target: &Target, value: &Value) {
    let shards = parse_cluster_shards(value);
    if shards.is_empty() {
        return;
    }

    state.cluster_id = cluster_signature(&shards);

    let myself = shards.iter().find_map(|shard| {
        shard
            .nodes
            .iter()
            .find(|node| addresses_match(&node.addr, &target.addr))
            .map(|node| (shard, node))
    });

    let Some((shard, myself)) = myself else {
        return;
    };

    if myself.is_replica() {
        state.kind = InstanceType::Replica;
        state.parent_addr = shard
            .nodes
            .iter()
            .find(|node| node.is_primary())
            .map(|node| node.addr.clone());
    } else if myself.is_primary() {
        state.kind = InstanceType::Primary;
        state.parent_addr = None;
    } else {
        state.kind = InstanceType::Cluster;
    }
}

fn cluster_signature(shards: &[ClusterShard]) -> Option<String> {
    let mut ids: Vec<&str> = shards
        .iter()
        .flat_map(|shard| shard.nodes.iter())
        .filter_map(|node| node.node_id.as_deref())
        .collect();
    ids.sort_unstable();
    ids.dedup();
    if let Some(id) = ids.first() {
        return Some((*id).to_string());
    }

    let mut addrs: Vec<&str> = shards
        .iter()
        .flat_map(|shard| shard.nodes.iter())
        .map(|node| node.addr.as_str())
        .collect();
    addrs.sort_unstable();
    addrs.dedup();
    addrs.first().map(|addr| (*addr).to_string())
}

fn addresses_match(left: &str, right: &str) -> bool {
    if left == right {
        return true;
    }

    let left_host = canonical_host(left);
    let right_host = canonical_host(right);
    let left_port = strip_host(left);
    let right_port = strip_host(right);
    left_host.is_some() && left_host == right_host && left_port == right_port
}

#[cfg(test)]
mod tests {
    use redis::Value;

    use super::cluster_signature;
    use crate::parse::{ClusterShard, ClusterShardNode, ClusterShardRole, parse_cluster_shards};

    #[test]
    fn cluster_signature_is_stable_for_same_membership() {
        let shards_a = vec![ClusterShard {
            nodes: vec![
                ClusterShardNode {
                    node_id: Some("bbbb".to_string()),
                    addr: "127.0.0.1:6380".to_string(),
                    role: ClusterShardRole::Primary,
                },
                ClusterShardNode {
                    node_id: Some("aaaa".to_string()),
                    addr: "127.0.0.1:6379".to_string(),
                    role: ClusterShardRole::Primary,
                },
            ],
        }];
        let shards_b = vec![ClusterShard {
            nodes: vec![
                ClusterShardNode {
                    node_id: Some("aaaa".to_string()),
                    addr: "127.0.0.1:6379".to_string(),
                    role: ClusterShardRole::Primary,
                },
                ClusterShardNode {
                    node_id: Some("bbbb".to_string()),
                    addr: "127.0.0.1:6380".to_string(),
                    role: ClusterShardRole::Primary,
                },
            ],
        }];

        assert_eq!(cluster_signature(&shards_a), Some("aaaa".to_string()));
        assert_eq!(cluster_signature(&shards_a), cluster_signature(&shards_b));
    }

    #[test]
    fn cluster_signature_falls_back_to_addr_without_node_ids() {
        let response = Value::Array(vec![Value::Map(vec![(
            Value::BulkString(b"nodes".to_vec()),
            Value::Array(vec![
                Value::Map(vec![
                    (
                        Value::BulkString(b"endpoint".to_vec()),
                        Value::BulkString(b"10.0.0.2".to_vec()),
                    ),
                    (Value::BulkString(b"port".to_vec()), Value::Int(7001)),
                ]),
                Value::Map(vec![
                    (
                        Value::BulkString(b"endpoint".to_vec()),
                        Value::BulkString(b"10.0.0.1".to_vec()),
                    ),
                    (Value::BulkString(b"port".to_vec()), Value::Int(7000)),
                ]),
            ]),
        )])]);

        let shards = parse_cluster_shards(&response);
        assert_eq!(
            cluster_signature(&shards),
            Some("10.0.0.1:7000".to_string())
        );
    }
}
