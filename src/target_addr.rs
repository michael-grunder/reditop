use std::net::{IpAddr, SocketAddr};

use anyhow::{Context, Result};

pub fn normalize_tcp_addr(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if let Some(port) = parse_port_only(trimmed)? {
        return Ok(format!("127.0.0.1:{port}"));
    }
    Ok(trimmed.to_string())
}

pub fn is_local_address(addr: &str) -> bool {
    if addr.contains('/') {
        return true;
    }

    if let Ok(socket) = addr.parse::<SocketAddr>() {
        return socket.ip().is_loopback();
    }

    let Some(host) = extract_host(addr) else {
        return false;
    };

    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }

    host.parse::<IpAddr>()
        .map(|ip| ip.is_loopback())
        .unwrap_or(false)
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
    use super::{is_local_address, normalize_tcp_addr};

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
    fn detects_local_addresses() {
        assert!(is_local_address("127.0.0.1:6379"));
        assert!(is_local_address("localhost:6379"));
        assert!(is_local_address("[::1]:6379"));
        assert!(is_local_address("/tmp/redis.sock"));
        assert!(!is_local_address("10.0.0.12:6379"));
    }
}
