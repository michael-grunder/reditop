use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use redis::{AsyncConnectionConfig, Client, ErrorKind, Value};
use tokio::sync::{Semaphore, mpsc};

use crate::model::{
    BigkeyEntry, BigkeyTypeSummary, BigkeysMetrics, BigkeysScanStatus, InstanceState, InstanceType,
    RuntimeSettings, Status, Target, TargetProtocol,
};
use crate::parse::{ClusterShard, parse_cluster_shards, parse_commandstats, parse_info};
use crate::target_addr::{canonical_host, strip_host};

const BIGKEYS_SCAN_COUNT: usize = 256;
const BIGKEYS_TOP_N: usize = 20;

#[derive(Debug, Clone)]
pub enum PollerRequest {
    RefreshAll,
    RefreshBigkeys { key: String, force: bool },
}

pub fn start(
    targets: Vec<Target>,
    settings: RuntimeSettings,
) -> (mpsc::Receiver<InstanceState>, mpsc::Sender<PollerRequest>) {
    let (update_tx, update_rx) = mpsc::channel(1024);
    let (request_tx, mut request_rx) = mpsc::channel::<PollerRequest>(32);

    tokio::spawn(async move {
        let semaphore = Arc::new(Semaphore::new(settings.concurrency_limit.max(1)));
        let mut known_states: HashMap<String, InstanceState> = HashMap::new();
        let mut ticker = tokio::time::interval(settings.refresh_interval);

        loop {
            let request = tokio::select! {
                _ = ticker.tick() => Some(PollerRequest::RefreshAll),
                maybe = request_rx.recv() => maybe,
            };

            let Some(request) = request else {
                break;
            };

            match request {
                PollerRequest::RefreshAll => {
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
                PollerRequest::RefreshBigkeys { key, force } => {
                    let Some(target) = targets.iter().find(|candidate| candidate.addr == key)
                    else {
                        continue;
                    };
                    let Some(prior) = known_states.get(&key).cloned() else {
                        continue;
                    };
                    if !force
                        && matches!(
                            prior.detail.bigkeys.status,
                            BigkeysScanStatus::Running | BigkeysScanStatus::Ready
                        )
                    {
                        continue;
                    }

                    let mut running = prior.clone();
                    running.detail.bigkeys.status = BigkeysScanStatus::Running;
                    running.detail.bigkeys.last_error = None;
                    if update_tx.send(running.clone()).await.is_err() {
                        return;
                    }
                    known_states.insert(key.clone(), running);

                    let updated = {
                        let _permit = semaphore.clone().acquire_owned().await.ok();
                        poll_bigkeys(target, &settings, prior).await
                    };
                    known_states.insert(updated.key.clone(), updated.clone());
                    if update_tx.send(updated).await.is_err() {
                        return;
                    }
                }
            }
        }
    });

    (update_rx, request_tx)
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

async fn poll_bigkeys(
    target: &Target,
    settings: &RuntimeSettings,
    mut state: InstanceState,
) -> InstanceState {
    let client = match Client::open(redis_url(target)) {
        Ok(client) => client,
        Err(err) => {
            apply_bigkeys_failure(&mut state, err.to_string());
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
            let (_, message) = classify_error(&err);
            apply_bigkeys_failure(&mut state, message);
            return state;
        }
    };

    match scan_bigkeys(&mut conn).await {
        Ok(bigkeys) => state.detail.bigkeys = bigkeys,
        Err(err) => apply_bigkeys_failure(&mut state, err.to_string()),
    }

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

fn apply_bigkeys_failure(state: &mut InstanceState, message: String) {
    state.detail.bigkeys.status = BigkeysScanStatus::Failed;
    state.detail.bigkeys.last_error = Some(truncate_string(message, 120));
    state.detail.bigkeys.last_completed = Some(Instant::now());
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

async fn scan_bigkeys(
    conn: &mut impl redis::aio::ConnectionLike,
) -> redis::RedisResult<BigkeysMetrics> {
    let (memory_usage_checked, memory_usage_supported) = detect_memory_usage_support(conn).await?;
    let mut cursor = 0u64;
    let mut scanned_keys = 0u64;
    let mut largest_keys = Vec::new();
    let mut type_summaries: HashMap<String, BigkeyTypeSummary> = HashMap::new();

    loop {
        let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .cursor_arg(cursor)
            .arg("COUNT")
            .arg(BIGKEYS_SCAN_COUNT)
            .query_async(conn)
            .await?;

        if !keys.is_empty() {
            scanned_keys += u64::try_from(keys.len()).unwrap_or(u64::MAX);
            let entries = fetch_bigkey_entries(conn, &keys, memory_usage_supported).await?;
            for entry in entries {
                update_bigkey_type_summary(&mut type_summaries, &entry);
                insert_bigkey_entry(&mut largest_keys, entry);
            }
        }

        if next_cursor == 0 {
            break;
        }
        cursor = next_cursor;
    }

    largest_keys.sort_by(bigkey_entry_cmp);
    let mut type_summaries = type_summaries.into_values().collect::<Vec<_>>();
    type_summaries.sort_by(type_summary_cmp);

    Ok(BigkeysMetrics {
        status: BigkeysScanStatus::Ready,
        last_error: None,
        scanned_keys,
        memory_usage_checked,
        memory_usage_supported,
        largest_keys,
        type_summaries,
        last_completed: Some(Instant::now()),
    })
}

async fn detect_memory_usage_support(
    conn: &mut impl redis::aio::ConnectionLike,
) -> redis::RedisResult<(bool, bool)> {
    match redis::cmd("MEMORY")
        .arg("USAGE")
        .arg("__reditop_bigkeys_probe__")
        .query_async::<Option<u64>>(conn)
        .await
    {
        Ok(_) => Ok((true, true)),
        Err(err) if is_unknown_command(&err) => Ok((true, false)),
        Err(err) => Err(err),
    }
}

async fn fetch_bigkey_entries(
    conn: &mut impl redis::aio::ConnectionLike,
    keys: &[String],
    memory_usage_supported: bool,
) -> redis::RedisResult<Vec<BigkeyEntry>> {
    let mut type_pipe = redis::pipe();
    for key in keys {
        type_pipe.cmd("TYPE").arg(key);
    }
    let types: Vec<String> = type_pipe.query_async(conn).await?;

    let mut size_pipe = redis::pipe();
    size_pipe.ignore_errors();
    let mut size_indexes = Vec::new();
    for (idx, (key, key_type)) in keys.iter().zip(types.iter()).enumerate() {
        if let Some(command) = key_type_size_command(key_type) {
            size_pipe.cmd(command).arg(key);
            size_indexes.push(idx);
        }
    }
    let size_results = if size_indexes.is_empty() {
        Vec::new()
    } else {
        size_pipe
            .query_async::<Vec<redis::RedisResult<u64>>>(conn)
            .await?
    };

    let memory_results = if memory_usage_supported {
        let mut memory_pipe = redis::pipe();
        for key in keys {
            memory_pipe.cmd("MEMORY").arg("USAGE").arg(key);
        }
        memory_pipe.query_async::<Vec<Option<u64>>>(conn).await?
    } else {
        Vec::new()
    };

    let mut sizes = vec![None; keys.len()];
    for (result_idx, key_idx) in size_indexes.into_iter().enumerate() {
        sizes[key_idx] = size_results
            .get(result_idx)
            .and_then(|result| result.as_ref().ok().copied());
    }

    let mut entries = Vec::with_capacity(keys.len());
    for (idx, key) in keys.iter().enumerate() {
        let key_type = types
            .get(idx)
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        if key_type == "none" {
            continue;
        }
        entries.push(BigkeyEntry {
            key: key.clone(),
            key_type,
            size: sizes[idx],
            memory_usage: memory_results.get(idx).copied().flatten(),
        });
    }
    Ok(entries)
}

fn key_type_size_command(key_type: &str) -> Option<&'static str> {
    match key_type {
        "string" => Some("STRLEN"),
        "list" => Some("LLEN"),
        "set" => Some("SCARD"),
        "zset" => Some("ZCARD"),
        "hash" => Some("HLEN"),
        "stream" => Some("XLEN"),
        _ => None,
    }
}

fn update_bigkey_type_summary(
    summaries: &mut HashMap<String, BigkeyTypeSummary>,
    entry: &BigkeyEntry,
) {
    let summary = summaries
        .entry(entry.key_type.clone())
        .or_insert_with(|| BigkeyTypeSummary {
            key_type: entry.key_type.clone(),
            count: 0,
            biggest_key: None,
            biggest_size: None,
        });
    summary.count += 1;

    if let Some(size) = entry.size {
        let should_replace = summary.biggest_size.is_none_or(|current| size > current)
            || (summary.biggest_size == Some(size)
                && summary
                    .biggest_key
                    .as_deref()
                    .is_none_or(|current| entry.key.as_str() < current));
        if should_replace {
            summary.biggest_size = Some(size);
            summary.biggest_key = Some(entry.key.clone());
        }
    }
}

fn insert_bigkey_entry(entries: &mut Vec<BigkeyEntry>, entry: BigkeyEntry) {
    entries.push(entry);
    entries.sort_by(bigkey_entry_cmp);
    if entries.len() > BIGKEYS_TOP_N {
        entries.truncate(BIGKEYS_TOP_N);
    }
}

fn bigkey_entry_cmp(left: &BigkeyEntry, right: &BigkeyEntry) -> std::cmp::Ordering {
    right
        .size
        .unwrap_or(0)
        .cmp(&left.size.unwrap_or(0))
        .then_with(|| {
            right
                .memory_usage
                .unwrap_or(0)
                .cmp(&left.memory_usage.unwrap_or(0))
        })
        .then_with(|| left.key_type.cmp(&right.key_type))
        .then_with(|| left.key.cmp(&right.key))
}

fn type_summary_cmp(left: &BigkeyTypeSummary, right: &BigkeyTypeSummary) -> std::cmp::Ordering {
    right
        .biggest_size
        .unwrap_or(0)
        .cmp(&left.biggest_size.unwrap_or(0))
        .then_with(|| right.count.cmp(&left.count))
        .then_with(|| left.key_type.cmp(&right.key_type))
}

fn is_unknown_command(error: &redis::RedisError) -> bool {
    error
        .code()
        .is_some_and(|code| code.eq_ignore_ascii_case("ERR"))
        && error
            .to_string()
            .to_ascii_lowercase()
            .contains("unknown command")
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

    use super::{
        BIGKEYS_TOP_N, BigkeyEntry, cluster_signature, insert_bigkey_entry, key_type_size_command,
        update_bigkey_type_summary,
    };
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

    #[test]
    fn key_type_size_command_maps_supported_types() {
        assert_eq!(key_type_size_command("string"), Some("STRLEN"));
        assert_eq!(key_type_size_command("list"), Some("LLEN"));
        assert_eq!(key_type_size_command("set"), Some("SCARD"));
        assert_eq!(key_type_size_command("zset"), Some("ZCARD"));
        assert_eq!(key_type_size_command("hash"), Some("HLEN"));
        assert_eq!(key_type_size_command("stream"), Some("XLEN"));
        assert_eq!(key_type_size_command("module"), None);
    }

    #[test]
    fn bigkey_entry_list_keeps_largest_keys_only() {
        let mut entries = Vec::new();
        for idx in 0..=BIGKEYS_TOP_N {
            insert_bigkey_entry(
                &mut entries,
                BigkeyEntry {
                    key: format!("key{idx}"),
                    key_type: "string".into(),
                    size: Some(u64::try_from(idx).expect("non-negative")),
                    memory_usage: None,
                },
            );
        }

        assert_eq!(entries.len(), BIGKEYS_TOP_N);
        assert_eq!(
            entries.first().and_then(|entry| entry.size),
            Some(BIGKEYS_TOP_N as u64)
        );
        assert_eq!(entries.last().and_then(|entry| entry.size), Some(1));
    }

    #[test]
    fn bigkey_type_summary_tracks_largest_entry() {
        let mut summaries = std::collections::HashMap::new();
        update_bigkey_type_summary(
            &mut summaries,
            &BigkeyEntry {
                key: "beta".into(),
                key_type: "list".into(),
                size: Some(10),
                memory_usage: None,
            },
        );
        update_bigkey_type_summary(
            &mut summaries,
            &BigkeyEntry {
                key: "alpha".into(),
                key_type: "list".into(),
                size: Some(10),
                memory_usage: None,
            },
        );
        update_bigkey_type_summary(
            &mut summaries,
            &BigkeyEntry {
                key: "gamma".into(),
                key_type: "list".into(),
                size: Some(25),
                memory_usage: None,
            },
        );

        let summary = summaries.get("list").expect("list summary exists");
        assert_eq!(summary.count, 3);
        assert_eq!(summary.biggest_key.as_deref(), Some("gamma"));
        assert_eq!(summary.biggest_size, Some(25));
    }
}
