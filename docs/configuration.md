# Server Configuration

MemFlux is configured using a `config.json` file located in the same directory as the server executable. If the file does not exist on the first run, a default configuration will be created.

## Configuration Options

Here is an example `config.json` with all available options:

```json
{
  "host": "127.0.0.1",
  "port": 8360,
  "requirepass": "",
  "wal_file": "memflux.wal",
  "snapshot_file": "memflux.snapshot",
  "snapshot_temp_file": "memflux.snapshot.tmp",
  "wal_size_threshold_mb": 128,
  "maxmemory_mb": 0,
  "eviction_policy": "lru",
  "encrypt": false,
  "cert_file": "memflux.crt",
  "key_file": "memflux.key"
}
```

### Option Details

-   `host` (string): The IP address to bind the server to. Default: `"127.0.0.1"`.
-   `port` (number): The port to listen on. Default: `8360`.
-   `requirepass` (string): If set to a non-empty string, clients must send the `AUTH <password>` command before any other commands. Default: `""` (disabled).
-   `wal_file` (string): The path to the Write-Ahead Log file. Default: `"memflux.wal"`.
-   `snapshot_file` (string): The path to the database snapshot file. Default: `"memflux.snapshot"`.
-   `snapshot_temp_file` (string): A temporary file used during snapshot creation. Default: `"memflux.snapshot.tmp"`.
-   `wal_size_threshold_mb` (number): The size in megabytes the WAL file must reach to trigger a new snapshot. Default: `128`.
-   `maxmemory_mb` (number): The maximum memory usage limit in megabytes. If set to `0`, the limit is disabled. When the limit is reached, the server will evict keys to make space. Default: `0`.
-   `eviction_policy` (string): The policy to use when evicting keys to stay under `maxmemory`. Can be `"lru"` (Least Recently Used) or `"lfu"` (Least Frequently Used). Default: `"lru"`.
-   `encrypt` (boolean): Enables or disables TLS encryption for client connections. Default: `false`.
-   `cert_file` (string): The path to the TLS certificate file (in PEM format). Default: `"memflux.crt"`.
-   `key_file` (string): The path to the TLS private key file (in PEM format). Default: `"memflux.key"`.

## TLS Encryption

When `encrypt` is set to `true`, MemFlux will listen for TLS connections. If the specified `cert_file` and `key_file` do not exist on startup, the server will automatically generate a new self-signed certificate and key, saving them to the specified paths. This allows for immediate secure connections for testing and development purposes.

For production environments, it is recommended to use a certificate issued by a trusted Certificate Authority (CA).
