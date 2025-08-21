use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum EvictionPolicy {
    LRU,
    LFU,
}

impl Default for EvictionPolicy {
    fn default() -> Self {
        EvictionPolicy::LRU
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub requirepass: String,
    pub wal_file: String,
    pub snapshot_file: String,
    pub snapshot_temp_file: String,
    pub wal_size_threshold_mb: u64,
    #[serde(default = "default_maxmemory_mb")]
    pub maxmemory_mb: u64,
    #[serde(default)]
    pub eviction_policy: EvictionPolicy,
    #[serde(default)]
    pub encrypt: bool,
    #[serde(default = "default_cert_file")]
    pub cert_file: String,
    #[serde(default = "default_key_file")]
    pub key_file: String,
}

fn default_maxmemory_mb() -> u64 {
    0
}

fn default_cert_file() -> String {
    "memflux.crt".to_string()
}
fn default_key_file() -> String {
    "memflux.key".to_string()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8360,
            requirepass: "".to_string(),
            wal_file: "memflux.wal".to_string(),
            snapshot_file: "memflux.snapshot".to_string(),
            snapshot_temp_file: "memflux.snapshot.tmp".to_string(),
            wal_size_threshold_mb: 16,
            maxmemory_mb: default_maxmemory_mb(),
            eviction_policy: EvictionPolicy::default(),
            encrypt: false,
            cert_file: default_cert_file(),
            key_file: default_key_file(),
        }
    }
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        if Path::new(path).exists() {
            let config_str = fs::read_to_string(path)?;
            let config: Config = serde_json::from_str(&config_str)?;
            Ok(config)
        } else {
            let config = Config::default();
            let config_str = serde_json::to_string_pretty(&config)?;
            fs::write(path, config_str)?;
            println!("Default configuration file created at '{}'", path);
            Ok(config)
        }
    }
}
