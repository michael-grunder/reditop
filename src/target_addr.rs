use std::net::IpAddr;

use anyhow::{Context, Result};

pub fn normalize_tcp_addr(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if let Some(port) = parse_port_only(trimmed)? {
        return Ok(format!("127.0.0.1:{port}"));
    }
    Ok(trimmed.to_string())
}

pub fn canonical_host(addr: &str) -> Option<String> {
    if addr.contains('/') {
        return None;
    }

    let host = extract_host(addr)?;
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Some(ip.to_string());
    }
    Some(host.to_ascii_lowercase())
}

pub fn strip_host(addr: &str) -> Option<String> {
    if addr.contains('/') {
        return None;
    }

    if let Some(rest) = addr.strip_prefix('[') {
        let (_, suffix) = rest.split_once(']')?;
        if suffix.is_empty() {
            return Some(addr.to_string());
        }
        return suffix.strip_prefix(':').map(str::to_string);
    }

    if let Some((_, port)) = addr.rsplit_once(':')
        && !port.is_empty()
        && port.chars().all(|ch| ch.is_ascii_digit())
    {
        return Some(port.to_string());
    }

    if !addr.contains(':') {
        return Some(addr.to_string());
    }

    None
}

fn parse_port_only(raw: &str) -> Result<Option<u16>> {
    if raw.is_empty() {
        return Ok(None);
    }

    if raw.chars().all(|ch| ch.is_ascii_digit()) {
        let port = raw
            .parse::<u16>()
            .with_context(|| format!("invalid TCP port '{raw}'"))?;
        return Ok(Some(port));
    }

    if let Some(port) = raw.strip_prefix(':')
        && !port.is_empty()
        && port.chars().all(|ch| ch.is_ascii_digit())
    {
        let parsed = port
            .parse::<u16>()
            .with_context(|| format!("invalid TCP port '{raw}'"))?;
        return Ok(Some(parsed));
    }

    Ok(None)
}

fn extract_host(addr: &str) -> Option<&str> {
    if let Some(rest) = addr.strip_prefix('[') {
        let (host, suffix) = rest.split_once(']')?;
        if suffix.is_empty() || suffix.starts_with(':') {
            return Some(host);
        }
        return None;
    }

    if let Some((host, port)) = addr.rsplit_once(':')
        && !port.is_empty()
        && port.chars().all(|ch| ch.is_ascii_digit())
    {
        return Some(host);
    }

    if !addr.contains(':') {
        return Some(addr);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::{canonical_host, normalize_tcp_addr, strip_host};

    #[test]
    fn normalizes_port_only_targets() {
        assert_eq!(
            normalize_tcp_addr("6379").expect("port should parse"),
            "127.0.0.1:6379"
        );
        assert_eq!(
            normalize_tcp_addr(":6380").expect("port should parse"),
            "127.0.0.1:6380"
        );
        assert_eq!(
            normalize_tcp_addr("localhost:6379").expect("host:port should pass"),
            "localhost:6379"
        );
    }

    #[test]
    fn canonicalizes_hosts_for_comparison() {
        assert_eq!(
            canonical_host("LOCALHOST:6379"),
            Some("localhost".to_string())
        );
        assert_eq!(
            canonical_host("127.0.0.1:6379"),
            Some("127.0.0.1".to_string())
        );
        assert_eq!(canonical_host("/tmp/redis.sock"), None);
    }

    #[test]
    fn strips_host_from_host_port_addresses() {
        assert_eq!(strip_host("localhost:6379"), Some("6379".to_string()));
        assert_eq!(strip_host("[::1]:6380"), Some("6380".to_string()));
        assert_eq!(strip_host("/tmp/redis.sock"), None);
    }
}
