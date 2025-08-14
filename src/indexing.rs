use dashmap::DashMap;
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::commands::json_path_to_pointer;

pub type Index = RwLock<BTreeMap<String, HashSet<String>>>;

#[derive(Default, Debug)]
pub struct IndexManager {
    /// Maps an index name (e.g., "user:*|profile.age") to the index data.
    pub indexes: DashMap<String, Arc<Index>>,
    /// Maps a key prefix (e.g., "user:*") to a list of index names that apply to it.
    pub prefix_to_indexes: DashMap<String, Vec<String>>,
}

impl IndexManager {
    pub fn get_indexes_for_key(&self, key: &str) -> Vec<(String, String)> {
        let mut applicable = Vec::new();
        for item in self.prefix_to_indexes.iter() {
            let prefix = item.key();
            let pattern = prefix.strip_suffix('*').unwrap_or(prefix);
            if key.starts_with(pattern) {
                for index_name in item.value() {
                    if let Some(json_path) = index_name.split('|').nth(1) {
                        applicable.push((index_name.clone(), json_path.to_string()));
                    }
                }
            }
        }
        applicable
    }

    pub async fn remove_key_from_indexes(&self, key: &str, old_value: &Value) {
        for (index_name, json_path) in self.get_indexes_for_key(key) {
            if let Some(index) = self.indexes.get(&index_name) {
                if let Some(old_indexed_val) = old_value.pointer(&json_path_to_pointer(&json_path))
                {
                    let index_key = serde_json::to_string(old_indexed_val).unwrap_or_default();
                    let mut index_data = index.write().await;
                    if let Some(keys_set) = index_data.get_mut(&index_key) {
                        keys_set.remove(key);
                        if keys_set.is_empty() {
                            index_data.remove(&index_key);
                        }
                    }
                }
            }
        }
    }

    pub async fn add_key_to_indexes(&self, key: &str, new_value: &Value) {
        for (index_name, json_path) in self.get_indexes_for_key(key) {
            if let Some(index) = self.indexes.get(&index_name) {
                if let Some(new_indexed_val) = new_value.pointer(&json_path_to_pointer(&json_path))
                {
                    let index_key = serde_json::to_string(new_indexed_val).unwrap_or_default();
                    let mut index_data = index.write().await;
                    index_data
                        .entry(index_key)
                        .or_default()
                        .insert(key.to_string());
                }
            }
        }
    }
}









