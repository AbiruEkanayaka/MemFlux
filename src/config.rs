use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum DurabilityLevel {
    None,
    Fsync,
    Full,
}

impl Default for DurabilityLevel {
    fn default() -> Self {
        DurabilityLevel::Fsync
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum EvictionPolicy {
    LRU,
    LFU,
    LFRU,
    ARC,
    Random,
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
    #[serde(default = "default_true")]
    pub persistence: bool,
    #[serde(default)]
    pub durability: DurabilityLevel,
    pub wal_file: String,
    #[serde(default = "default_wal_overflow_file")]
    pub wal_overflow_file: String,
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FFIConfig {
    #[serde(default = "default_true")]
    pub persistence: bool,
    pub wal_file: String,
    #[serde(default = "default_wal_overflow_file")]
    pub wal_overflow_file: String,
    pub snapshot_file: String,
    pub snapshot_temp_file: String,
    pub wal_size_threshold_mb: u64,
    #[serde(default = "default_maxmemory_mb")]
    pub maxmemory_mb: u64,
    #[serde(default)]
    pub eviction_policy: EvictionPolicy,
    #[serde(default)]
    pub durability: DurabilityLevel,
}

impl From<FFIConfig> for Config {
    fn from(ffi_config: FFIConfig) -> Self {
        Config {
            host: "127.0.0.1".to_string(),
            port: 0,
            requirepass: "".to_string(),
            persistence: ffi_config.persistence,
            wal_file: ffi_config.wal_file,
            wal_overflow_file: ffi_config.wal_overflow_file,
            snapshot_file: ffi_config.snapshot_file,
            snapshot_temp_file: ffi_config.snapshot_temp_file,
            wal_size_threshold_mb: ffi_config.wal_size_threshold_mb,
            maxmemory_mb: ffi_config.maxmemory_mb,
            eviction_policy: ffi_config.eviction_policy,
            durability: ffi_config.durability,
            encrypt: false,
            cert_file: "".to_string(),
            key_file: "".to_string(),
        }
    }
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

fn default_wal_overflow_file() -> String {
    "memflux.wal.overflow".to_string()
}

fn default_true() -> bool {
    true
}


impl Default for Config {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8360,
            requirepass: "".to_string(),
            persistence: true,
            durability: DurabilityLevel::default(),
            wal_file: "memflux.wal".to_string(),
            wal_overflow_file: "memflux.wal.overflow".to_string(),
            snapshot_file: "memflux.snapshot".to_string(),
            snapshot_temp_file: "memflux.snapshot.tmp".to_string(),
            wal_size_threshold_mb: 128,
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