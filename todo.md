# MemFlux Production-Grade Roadmap

## Tier 1: Foundational Stability & Security

### Security Hardening
- [x] Implement `AUTH` command for password protection.
- [x] Add TLS support for encrypted client-server communication.

### Memory Management
- [ ] Implement a `maxmemory` configuration setting.
- [ ] Add key eviction policies (e.g., LRU, LFU, TTL).

### Configuration & Operability
- [ ] Move hardcoded settings to a configuration file or command-line arguments.
- [ ] Replace `println!` with a structured logging framework (e.g., `tracing`).

## Tier 2: High Availability & Observability

### Replication
- [ ] Implement primary-replica replication for high availability and read scaling.

### Monitoring & Introspection
- [ ] Expose server metrics via a Prometheus-compatible endpoint.
- [ ] Implement admin commands: `INFO`, `MONITOR`, `CLIENT LIST`.

### WAL Robustness
- [ ] Add checksums to WAL entries to detect corruption.

## Tier 3: Advanced Features & Ecosystem

### Transactions
- [ ] Implement `MULTI` / `EXEC` / `DISCARD` for atomic transactions.

### Clustering
- [ ] Implement horizontal scaling (sharding) via a clustering solution.

### Expanded Data Models & Querying
- [ ] Add a dedicated Hash data type.
- [ ] Enhance the SQL indexing engine to support range queries (`>`, `<`, etc.).
