use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use redis::{AsyncConnectionConfig, Client, Value};

use crate::model::{RuntimeSettings, Target, TargetProtocol};

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

    let mut out = BTreeSet::new();
    collect_cluster_shard_addresses(&shards, &mut out);
    Ok(out.into_iter().collect())
}

fn collect_cluster_shard_addresses(value: &Value, out: &mut BTreeSet<String>) {
    if let Some((host, port)) = extract_node_host_port(value)
        && let Some(addr) = compose_addr(&host, port)
    {
        out.insert(addr);
    }

    match value {
        Value::Array(items) | Value::Set(items) => {
            for item in items {
                collect_cluster_shard_addresses(item, out);
            }
        }
        Value::Map(entries) => {
            for (_, value) in entries {
                collect_cluster_shard_addresses(value, out);
            }
        }
        Value::Attribute { data, attributes } => {
            collect_cluster_shard_addresses(data, out);
            for (_, value) in attributes {
                collect_cluster_shard_addresses(value, out);
            }
        }
        Value::Push { data, .. } => {
            for value in data {
                collect_cluster_shard_addresses(value, out);
            }
        }
        _ => {}
    }
}

fn extract_node_host_port(value: &Value) -> Option<(String, u16)> {
    let kv = kv_pairs(value)?;
    let mut endpoint = None;
    let mut hostname = None;
    let mut ip = None;
    let mut host = None;
    let mut port = None;

    for (k, v) in kv {
        let key = value_to_string(k)?.to_ascii_lowercase();
        match key.as_str() {
            "endpoint" => endpoint = value_to_string(v),
            "hostname" => hostname = value_to_string(v),
            "ip" => ip = value_to_string(v),
            "host" => host = value_to_string(v),
            "port" => port = value_to_u16(v),
            _ => {}
        }
    }

    let host = [endpoint, hostname, ip, host]
        .into_iter()
        .flatten()
        .find(|value| !value.is_empty() && value != "?" && value != "-")?;
    let port = port?;

    Some((host, port))
}

fn kv_pairs(value: &Value) -> Option<Vec<(&Value, &Value)>> {
    match value {
        Value::Map(entries) => Some(entries.iter().map(|(k, v)| (k, v)).collect()),
        Value::Array(items) => {
            if items.len() % 2 != 0 {
                return None;
            }
            Some(
                items
                    .chunks_exact(2)
                    .map(|chunk| (&chunk[0], &chunk[1]))
                    .collect(),
            )
        }
        _ => None,
    }
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::BulkString(bytes) => Some(String::from_utf8_lossy(bytes).to_string()),
        Value::SimpleString(text) => Some(text.clone()),
        Value::VerbatimString { text, .. } => Some(text.clone()),
        Value::Int(num) => Some(num.to_string()),
        Value::Double(num) => Some(num.to_string()),
        Value::BigNumber(num) => Some(num.to_string()),
        _ => None,
    }
}

fn value_to_u16(value: &Value) -> Option<u16> {
    match value {
        Value::Int(num) => (*num).try_into().ok(),
        _ => value_to_string(value)?.parse::<u16>().ok(),
    }
}

fn compose_addr(host: &str, port: u16) -> Option<String> {
    let mut trimmed = host.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Some(without_port) = strip_port_suffix(trimmed) {
        trimmed = without_port;
    }
    if let Some(inner) = trimmed
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
    {
        return Some(format!("[{inner}]:{port}"));
    }
    if trimmed.contains(':') {
        return Some(format!("[{trimmed}]:{port}"));
    }
    Some(format!("{trimmed}:{port}"))
}

fn strip_port_suffix(host: &str) -> Option<&str> {
    if let Some(inner) = host.strip_prefix('[') {
        let (addr, suffix) = inner.split_once(']')?;
        if suffix
            .strip_prefix(':')
            .map(|port| !port.is_empty() && port.chars().all(|ch| ch.is_ascii_digit()))
            .unwrap_or(false)
        {
            return Some(addr);
        }
        return None;
    }

    let (prefix, suffix) = host.rsplit_once(':')?;
    if !prefix.contains(':') && !suffix.is_empty() && suffix.chars().all(|ch| ch.is_ascii_digit()) {
        return Some(prefix);
    }

    None
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

#[cfg(test)]
mod tests {
    use super::collect_cluster_shard_addresses;
    use redis::Value;
    use std::collections::BTreeSet;

    #[test]
    fn extracts_cluster_nodes_from_resp2_array_shape() {
        let response = Value::Array(vec![Value::Array(vec![
            Value::BulkString(b"slots".to_vec()),
            Value::Array(vec![Value::Int(0), Value::Int(5460)]),
            Value::BulkString(b"nodes".to_vec()),
            Value::Array(vec![
                Value::Array(vec![
                    Value::BulkString(b"id".to_vec()),
                    Value::BulkString(b"node-a".to_vec()),
                    Value::BulkString(b"endpoint".to_vec()),
                    Value::BulkString(b"10.0.0.11".to_vec()),
                    Value::BulkString(b"port".to_vec()),
                    Value::Int(7000),
                    Value::BulkString(b"role".to_vec()),
                    Value::BulkString(b"master".to_vec()),
                ]),
                Value::Array(vec![
                    Value::BulkString(b"id".to_vec()),
                    Value::BulkString(b"node-b".to_vec()),
                    Value::BulkString(b"ip".to_vec()),
                    Value::BulkString(b"10.0.0.12".to_vec()),
                    Value::BulkString(b"port".to_vec()),
                    Value::BulkString(b"7001".to_vec()),
                    Value::BulkString(b"role".to_vec()),
                    Value::BulkString(b"replica".to_vec()),
                ]),
            ]),
        ])]);

        let mut found = BTreeSet::new();
        collect_cluster_shard_addresses(&response, &mut found);

        assert_eq!(
            found.into_iter().collect::<Vec<_>>(),
            vec!["10.0.0.11:7000".to_string(), "10.0.0.12:7001".to_string()]
        );
    }

    #[test]
    fn extracts_cluster_nodes_from_resp3_map_shape() {
        let response = Value::Array(vec![Value::Map(vec![
            (
                Value::BulkString(b"slots".to_vec()),
                Value::Array(vec![Value::Int(0), Value::Int(5460)]),
            ),
            (
                Value::BulkString(b"nodes".to_vec()),
                Value::Array(vec![
                    Value::Map(vec![
                        (
                            Value::BulkString(b"endpoint".to_vec()),
                            Value::BulkString(b"2001:db8::1".to_vec()),
                        ),
                        (Value::BulkString(b"port".to_vec()), Value::Int(7000)),
                    ]),
                    Value::Map(vec![
                        (
                            Value::BulkString(b"endpoint".to_vec()),
                            Value::BulkString(b"?".to_vec()),
                        ),
                        (
                            Value::BulkString(b"ip".to_vec()),
                            Value::BulkString(b"10.0.0.20".to_vec()),
                        ),
                        (Value::BulkString(b"port".to_vec()), Value::Int(7002)),
                    ]),
                ]),
            ),
        ])]);

        let mut found = BTreeSet::new();
        collect_cluster_shard_addresses(&response, &mut found);

        assert_eq!(
            found.into_iter().collect::<Vec<_>>(),
            vec![
                "10.0.0.20:7002".to_string(),
                "[2001:db8::1]:7000".to_string()
            ]
        );
    }

    #[test]
    fn avoids_double_port_when_endpoint_already_contains_port() {
        let response = Value::Array(vec![Value::Map(vec![
            (
                Value::BulkString(b"endpoint".to_vec()),
                Value::BulkString(b"10.0.0.30:7005".to_vec()),
            ),
            (Value::BulkString(b"port".to_vec()), Value::Int(7005)),
        ])]);

        let mut found = BTreeSet::new();
        collect_cluster_shard_addresses(&response, &mut found);

        assert_eq!(
            found.into_iter().collect::<Vec<_>>(),
            vec!["10.0.0.30:7005".to_string()]
        );
    }
}
