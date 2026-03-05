use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use redis::{AsyncConnectionConfig, Client, ErrorKind};
use tokio::sync::{Semaphore, mpsc};

use crate::model::{InstanceState, InstanceType, RuntimeSettings, Status, Target, TargetProtocol};
use crate::parse::{parse_cluster_nodes, parse_info};

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
        .set_connection_timeout(settings.connect_timeout)
        .set_response_timeout(settings.command_timeout);

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

    apply_info_to_state(&mut state, &info);

    if state.detail.cluster_enabled
        && let Ok(nodes_text) = redis::cmd("CLUSTER")
            .arg("NODES")
            .query_async::<String>(&mut conn)
            .await
    {
        let nodes = parse_cluster_nodes(&nodes_text);
        if let Some(myself) = nodes
            .iter()
            .find(|node| node.is_myself() || node.addr == target.addr)
        {
            state.cluster_id = cluster_signature(&nodes);
            if myself.is_replica() {
                state.kind = InstanceType::Replica;
                state.parent_addr = myself
                    .master_id
                    .as_ref()
                    .and_then(|master_id| nodes.iter().find(|node| node.node_id == *master_id))
                    .map(|node| node.addr.clone());
            } else if myself.is_master() {
                state.kind = InstanceType::Primary;
                state.parent_addr = None;
            } else {
                state.kind = InstanceType::Cluster;
            }
        }
    }

    state.push_latency_sample(latency_ms);
    state.last_latency_ms = Some(latency_ms);
    state.status = Status::Ok;
    state.last_error = None;
    state.last_updated = Some(Instant::now());

    state
}

fn apply_info_to_state(state: &mut InstanceState, info_raw: &str) {
    let info = parse_info(info_raw);

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
    state.detail.raw_info = Some(info_raw.to_string());

    if state.detail.cluster_enabled {
        state.kind = InstanceType::Cluster;
    } else {
        match state.detail.role.as_deref() {
            Some("master") => {
                state.kind = InstanceType::Primary;
                state.parent_addr = None;
            }
            Some("slave") | Some("replica") => {
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
        ErrorKind::IoError => {
            if msg.to_ascii_lowercase().contains("timed out") {
                (Status::Timeout, msg)
            } else {
                (Status::Down, msg)
            }
        }
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

fn cluster_signature(nodes: &[crate::parse::ClusterNode]) -> Option<String> {
    let mut ids: Vec<&str> = nodes.iter().map(|node| node.node_id.as_str()).collect();
    ids.sort_unstable();
    ids.first().map(|id| (*id).to_string())
}

#[cfg(test)]
mod tests {
    use super::cluster_signature;
    use crate::parse::parse_cluster_nodes;

    #[test]
    fn cluster_signature_is_stable_for_same_membership() {
        let input_a = "bbbb 127.0.0.1:6380@16380 master - 0 0 1 connected\n\
                       aaaa 127.0.0.1:6379@16379 myself,master - 0 0 1 connected\n";
        let input_b = "aaaa 127.0.0.1:6379@16379 master - 0 0 1 connected\n\
                       bbbb 127.0.0.1:6380@16380 myself,master - 0 0 1 connected\n";

        let nodes_a = parse_cluster_nodes(input_a);
        let nodes_b = parse_cluster_nodes(input_b);

        assert_eq!(cluster_signature(&nodes_a), Some("aaaa".to_string()));
        assert_eq!(cluster_signature(&nodes_a), cluster_signature(&nodes_b));
    }
}
