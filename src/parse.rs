use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct ParsedInfo {
    pub sections: HashMap<String, HashMap<String, String>>,
}

impl ParsedInfo {
    pub fn get(&self, section: &str, key: &str) -> Option<&str> {
        self.sections
            .get(section)
            .and_then(|kv| kv.get(key).map(String::as_str))
    }

    pub fn get_u64(&self, section: &str, key: &str) -> Option<u64> {
        self.get(section, key)?.parse().ok()
    }

    pub fn get_bool_01(&self, section: &str, key: &str) -> bool {
        self.get(section, key)
            .map(|value| value == "1")
            .unwrap_or(false)
    }
}

pub fn parse_info(input: &str) -> ParsedInfo {
    let mut parsed = ParsedInfo::default();
    let mut current_section = String::from("default");

    for line in input.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some(section) = trimmed.strip_prefix('#') {
            current_section = section.trim().to_ascii_lowercase();
            continue;
        }
        if let Some((k, v)) = trimmed.split_once(':') {
            parsed
                .sections
                .entry(current_section.clone())
                .or_default()
                .insert(k.to_string(), v.to_string());
        }
    }

    parsed
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterNode {
    pub node_id: String,
    pub addr: String,
    pub flags: Vec<String>,
    pub master_id: Option<String>,
    pub link_state: String,
}

impl ClusterNode {
    pub fn is_master(&self) -> bool {
        self.flags.iter().any(|flag| flag == "master")
    }

    pub fn is_replica(&self) -> bool {
        self.flags
            .iter()
            .any(|flag| flag == "slave" || flag == "replica")
    }

    pub fn is_myself(&self) -> bool {
        self.flags.iter().any(|flag| flag == "myself")
    }
}

pub fn parse_cluster_nodes(input: &str) -> Vec<ClusterNode> {
    let mut nodes = Vec::new();
    for line in input.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let fields: Vec<&str> = trimmed.split_whitespace().collect();
        if fields.len() < 8 {
            continue;
        }

        let addr = fields[1]
            .split('@')
            .next()
            .map(str::to_string)
            .unwrap_or_else(|| fields[1].to_string());
        let flags = fields[2].split(',').map(str::to_string).collect();
        let master_id = match fields[3] {
            "-" => None,
            value => Some(value.to_string()),
        };

        nodes.push(ClusterNode {
            node_id: fields[0].to_string(),
            addr,
            flags,
            master_id,
            link_state: fields[7].to_string(),
        });
    }
    nodes
}

#[cfg(test)]
mod tests {
    use super::{parse_cluster_nodes, parse_info};

    #[test]
    fn parses_info_sections_and_values() {
        let info = "# Server\nredis_version:7.2.4\nuptime_in_seconds:123\n# Memory\nused_memory:4096\nmaxmemory:0\n# Cluster\ncluster_enabled:1\n";
        let parsed = parse_info(info);

        assert_eq!(parsed.get("server", "redis_version"), Some("7.2.4"));
        assert_eq!(parsed.get_u64("server", "uptime_in_seconds"), Some(123));
        assert_eq!(parsed.get_u64("memory", "used_memory"), Some(4096));
        assert!(parsed.get_bool_01("cluster", "cluster_enabled"));
    }

    #[test]
    fn parses_cluster_nodes_lines() {
        let input = "07c37dfeb2352e66 127.0.0.1:7000@17000 master - 0 1426238317239 1 connected 0-5460\n3c3a1c6f8fd2b1f8 127.0.0.1:7003@17003 slave 07c37dfeb2352e66 0 1426238318240 4 connected\n";
        let nodes = parse_cluster_nodes(input);

        assert_eq!(nodes.len(), 2);
        assert!(nodes[0].is_master());
        assert_eq!(nodes[1].master_id.as_deref(), Some("07c37dfeb2352e66"));
        assert!(nodes[1].is_replica());
    }
}
