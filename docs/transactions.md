# Transactions in MemFlux

MemFlux ensures data integrity and concurrency through a Multi-Version Concurrency Control (MVCC) system. This allows for non-blocking reads and safe, concurrent writes. All operations, whether through the Redis-style command API or the SQL engine, are transactional.

## ACID Properties and Isolation

MemFlux provides ACID-like properties. By default, it operates at a **Snapshot Isolation** level. This guarantees that all reads made in a transaction will see a consistent snapshot of the database as it was at the start of the transaction. It prevents dirty reads, non-repeatable reads, and phantom reads.

A stricter **Serializable** isolation level is also available via configuration, which prevents all race conditions, including write skew, but may result in more transaction aborts under high contention.

## How It Works

- **Auto-Commit Mode:** By default, every command you send to MemFlux is treated as its own small transaction. The command is executed, and the transaction is immediately committed. This is simple and ensures that every write is durable (if persistence is enabled).

- **Explicit Transactions:** For multi-statement transactions, you can use the `BEGIN`, `COMMIT`, and `ROLLBACK` commands to manually control the transaction boundaries.

## Transaction Commands

These commands allow you to manage explicit transactions.

#### `BEGIN`
Starts a new transaction. All subsequent commands on the same connection will be executed within this transaction until a `COMMIT` or `ROLLBACK` is issued.
- **Syntax:** `BEGIN`
- **Returns:** `+OK`

#### `COMMIT`
Atomically applies all the changes made within the current transaction, making them visible to all new transactions.
- **Syntax:** `COMMIT`
- **Returns:** `+OK`

#### `ROLLBACK`
Discards all the changes made within the current transaction.
- **Syntax:** `ROLLBACK`
- **Returns:** `+OK`

### Example Flow
```
> BEGIN
+OK
> SET mykey "initial_value"
+OK
> GET mykey
$13
initial_value
> COMMIT
+OK
> GET mykey
$13
initial_value
```

## Savepoints

Savepoints allow you to set markers within a transaction that you can later roll back to, without having to discard the entire transaction.

#### `SAVEPOINT <name>`
Creates a new savepoint with the given name.
- **Syntax:** `SAVEPOINT <name>`
- **Returns:** `+OK`

#### `ROLLBACK TO SAVEPOINT <name>`
Rolls the transaction state back to the specified savepoint. Any changes or new savepoints created after this savepoint are discarded.
- **Syntax:** `ROLLBACK TO SAVEPOINT <name>`
- **Returns:** `+OK`

#### `RELEASE SAVEPOINT <name>`
Removes a savepoint. You can no longer roll back to it.
- **Syntax:** `RELEASE SAVEPOINT <name>`
- **Returns:** `+OK`

### Savepoint Example
```
> BEGIN
+OK
> SET key1 "value1"
+OK
> SAVEPOINT my_savepoint
+OK
> SET key2 "value2"
+OK
> GET key2
$6
value2
> ROLLBACK TO SAVEPOINT my_savepoint
+OK
> GET key2
$-1
> COMMIT
+OK
> GET key1
$6
value1
```
In this example, the write to `key2` is discarded, but the write to `key1` is committed.