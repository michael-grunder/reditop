use std::collections::{BTreeMap, HashMap};

use crate::model::InstanceState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeGroup {
    pub group: String,
    pub roots: Vec<String>,
    pub children: HashMap<String, Vec<String>>,
}

pub fn build_tree_groups(instances: &HashMap<String, InstanceState>) -> Vec<TreeGroup> {
    let mut grouped: BTreeMap<String, Vec<&InstanceState>> = BTreeMap::new();
    for instance in instances.values() {
        let group_name = instance
            .cluster_id
            .clone()
            .unwrap_or_else(|| "Standalone".to_string());
        grouped.entry(group_name).or_default().push(instance);
    }

    let mut out = Vec::with_capacity(grouped.len());
    for (group, nodes) in grouped {
        let mut children: HashMap<String, Vec<String>> = HashMap::new();
        let mut roots = Vec::new();

        for node in &nodes {
            match &node.parent_addr {
                Some(parent) => {
                    children
                        .entry(parent.clone())
                        .or_default()
                        .push(node.key.clone());
                }
                None => roots.push(node.key.clone()),
            }
        }

        roots.sort();
        for value in children.values_mut() {
            value.sort();
        }

        out.push(TreeGroup {
            group,
            roots,
            children,
        });
    }

    out
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::model::InstanceState;

    use super::build_tree_groups;

    #[test]
    fn builds_replication_tree() {
        let mut map = HashMap::new();

        let mut master = InstanceState::new("master".into(), "127.0.0.1:6379".into());
        master.cluster_id = Some("Standalone".into());
        let mut replica = InstanceState::new("replica".into(), "127.0.0.1:6380".into());
        replica.cluster_id = Some("Standalone".into());
        replica.parent_addr = Some("master".into());

        map.insert(master.key.clone(), master);
        map.insert(replica.key.clone(), replica);

        let groups = build_tree_groups(&map);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].roots, vec!["master"]);
        assert_eq!(
            groups[0].children.get("master"),
            Some(&vec!["replica".into()])
        );
    }
}
