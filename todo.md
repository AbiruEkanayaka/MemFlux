# SQP (Standard Query Protocol) Implementation TO-DO

## Phase 0: Foundational Protocol Design & Setup

This phase involves defining the basic structures and choosing the binary serialization method.

-   [x] **0.1. Create `src/sqp/` Module:**
    -   [x] Create a new top-level module `sqp` (e.g., `src/sqp/mod.rs`).
    -   [x] Add `mod sqp;` to `src/main.rs`.
-   [x] **0.2. Define SQP Header (`src/sqp/header.rs`):**
    -   [x] Create a `Header` struct matching the fixed 24-byte specification:
    -   [x] Implement methods for `Header` to serialize to/deserialize from `[u8; 24]`.
-   [x] **0.3. Define Message Types (`src/sqp/message_types.rs`):**
    -   [x] Create an enum `MessageType` mapping to the header's message type field (e.g., `Query = 1`, `Response = 2`, `Error = 3`, `Heartbeat = 4`, `HandshakeRequest = 5`, `HandshakeResponse = 6`, `StreamData = 7`, `Ack = 8`).
    -   [x] Consider using a `num_enum` crate or similar for easy conversion between `u16` and `MessageType`.
-   [x] **0.4. Choose & Implement Binary Serialization for Payloads:**
        -   **Bincode (Simpler Integration, Less Zero-Copy):**
            -   [x] Since `bincode` is already used for WAL, extending it might be quicker initially.
            -   [x] Ensure all data structures (`Command`, `Response` content, `Schema`, etc.) that will be sent over SQP are `#[derive(Serialize, Deserialize)]`.
-   [x] **0.5. Update `src/types.rs` for SQP:**
    -   [x] Define a new `SqpCommand` and `SqpResponse` enum/struct that wraps your existing `Command` and `Response` types but with SQP-specific metadata (e.g., `request_id`).
    -   [x] Update `AppContext` to potentially hold SQP-specific state (e.g., session management).

## Phase 1: Basic SQP Transport Layer (UDP First)

This phase focuses on getting a basic UDP server running and handling initial handshakes.

-   [x] **1.1. UDP Server (`src/sqp/transport.rs`):**
    -   [x] Replace `TcpListener` with `UdpSocket` in `main.rs` for the primary listener.
    -   [x] Implement a UDP message receive loop.
    -   [x] Handle basic packet reception (e.g., `socket.recv_from`).
    -   [x] Store sender addresses for responses.
-   [x] **1.2. Session Management (`src/sqp/session.rs`):**
    -   [x] Define a `Session` struct to hold client state (e.g., `UdpSocket` address, current sequence numbers, last seen heartbeat, preferred serialization format).
    -   [x] Use a `DashMap<u64, Arc<RwLock<Session>>>` (or similar) in `AppContext` to manage active sessions by `session_id`.
    -   [x] Implement session creation and termination logic.
-   [x] **1.3. Handshake Implementation:**
    -   [x] Implement `HandshakeRequest` and `HandshakeResponse` message types with necessary fields (protocol version, capabilities, serialization preferences).
    -   [x] On receiving a `HandshakeRequest` UDP packet:
        -   [x] Validate request.
        -   [x] Generate a new `session_id`.
        -   [x] Create a new `Session` entry.
        -   [x] Send `HandshakeResponse` back to the client via UDP.
-   [ ] **1.4. Basic Client-Side Adaptation (Python `test.py`):**
-   [x] **1.5. Error Handling for Transport Layer:**
    -   [x] Handle UDP errors gracefully.

## Phase 2: Core SQP Query/Response Flow (Unreliable UDP)

-   [x] **2.2. Integrate Query Execution:**
    -   [x] Modify the UDP receive loop in `src/sqp/transport.rs` (or a new handler function).
    -   [x] When a `MessageType::Query` is received:
        -   [x] Extract `session_id` and `request_id` from the header.
        -   [x] Deserialize the payload into the internal `Command` structure.
        -   [x] Call `commands::process_command` (or its SQP-adapted equivalent).
        -   [x] Serialize the `Response` back into the SQP payload format.
        -   [x] Construct a `Response` header with the same `session_id` and `request_id`.
        -   [x] Send the `Response` packet back to the client via UDP.
-   [x] **2.3. Update `main.rs` Connection Handling:**
    -   [x] `main.rs` loop will now primarily manage UDP datagrams, dispatching them to session handlers or the handshake logic.
-   [ ] **2.4. Update `commands.rs` & `query_engine/execution.rs`:**
    -   [ ] `process_command` will now receive and return `SqpCommand` and `SqpResponse` (or convert internally).
    -   [ ] The SQL streaming part in `main.rs` needs to be adapted: `execute` already returns a `Stream`, which is great. Instead of collecting into `Vec<Row>`, you can stream rows directly as `StreamData` message types (see Phase 4). For now, you might still need to collect for a single `Response` message until proper streaming is implemented.

## Phase 3: SQP Reliability: Ordered Delivery & Retransmissions

This is where the "reliable UDP" part truly comes into play.

-   [ ] **3.1. Add Sequence Numbers & ACKs (`src/sqp/reliable_udp.rs`):**
    -   [ ] Introduce `sequence_number` (e.g., `u32`) to the SQP Header (or a new field in flags, or a separate reliability sub-header).
    -   [ ] Implement a sliding window protocol for sending and receiving.
    -   [ ] Define an `Ack` message type (e.g., `Selective ACK` with bitmap for received packets).
    -   [ ] Implement ACK generation on the receiver side.
    -   [ ] Implement ACK processing on the sender side to mark packets as acknowledged.
-   [ ] **3.2. Retransmission Timers:**
    -   [ ] For unacknowledged packets, implement retransmission timers.
    -   [ ] Resend packets if ACK is not received within timeout.
-   [ ] **3.3. Deduplication:**
    -   [ ] On the receiver side, use sequence numbers to identify and discard duplicate packets.
-   [ ] **3.4. Flow & Congestion Control (Basic):**
    -   [ ] Implement a simple send window based on ACKs to prevent overwhelming the receiver.
    -   [ ] A basic exponential backoff for retransmission timers on repeated loss.

## Phase 4: SQP Advanced Features & Multiplexing

-   [ ] **4.1. Multiplexed Requests:**
    -   [ ] Ensure `request_id` in the `Header` is correctly used.
    -   [ ] Maintain a `HashMap<u32, tokio::sync::oneshot::Sender<SqpResponse>>` (or similar) per session on the server to dispatch responses to the correct waiting `Command` task.
    -   [ ] Client-side must also manage multiple outstanding requests and match responses by `request_id`.
-   [ ] **4.2. Streaming & Push Events (`src/sqp/stream.rs`):**
    -   [ ] Define a `StreamData` message type.
    -   [ ] Introduce `stream_id` (e.g., `u32`) in the header or as part of a new `StreamData` message payload.
    -   [ ] Modify `query_engine::execution.rs` to allow execution results to be streamed directly via SQP `StreamData` messages instead of collecting them entirely for `SQL SELECT`.
    -   [ ] Implement server-side logic to initiate and manage `StreamData` messages (e.g., for `SQL` query results, or future changefeeds).
    -   [ ] Client-side logic to reassemble streamed results.
-   [ ] **4.3. Heartbeats:**
    -   [ ] Implement `Heartbeat` message type.
    -   [ ] Periodic sending of heartbeats from both client and server.
    -   [ ] Timeout detection for dead peers.
-   [ ] **4.4. TCP Fallback Integration:**
    -   [ ] **Decision Point**: How to trigger fallback? (e.g., repeated retransmissions, high packet loss, explicit client/server request).
    -   [ ] Reintroduce `TcpListener` as a fallback.
    -   [ ] Implement session migration logic: if UDP degrades, establish a new TCP connection, transfer session state (e.g., last sequence numbers, pending requests), and switch communication.
    -   [ ] Client-side logic for initiating and managing TCP fallback.
-   [ ] **4.5. Graceful Termination:**
    -   [ ] Define `Termination` message type.
    -   [ ] Implement explicit session termination protocol for clean resource cleanup.

## Phase 5: Security by Design (DTLS & Authentication)

This phase adds the crucial security layer.

-   [ ] **5.1. DTLS Encryption (`src/sqp/security.rs`):**
    -   [ ] Add `tokio-rustls` (or similar) with `dtls` features to `Cargo.toml`.
    -   [ ] Integrate DTLS handshake *after* the SQP handshake but *before* query exchange.
    -   [ ] Encrypt/decrypt SQP messages using DTLS.
-   [ ] **5.2. Authentication:**
    -   [ ] Integrate authentication mechanisms (e.g., client tokens, certificates) into the SQP Handshake message.
    -   [ ] Validate authentication credentials on the server.
-   [ ] **5.3. Integrity (Checksums/MACs):**
    -   [ ] Ensure message checksums/MACs are applied to detect tampering (often handled by DTLS, but good to verify).
-   [ ] **5.4. Replay Protection:**
    -   [ ] Implement mechanisms to prevent replay attacks (also often handled by DTLS, but confirm).

## Phase 6: Refinement, Optimization & Testing

-   [ ] **6.1. Comprehensive Error Handling:**
    -   [ ] Define specific SQP `Error` messages for various failure conditions.
    -   [ ] Ensure all layers (transport, parsing, execution) propagate errors correctly using SQP error responses.
-   [ ] **6.2. Resource Management:**
    -   [ ] Ensure proper cleanup of `tokio` tasks, `UdpSocket`s, `TcpStream`s, and session state on disconnect/termination.
-   [ ] **6.3. Performance Tuning:**
    -   [ ] Optimize buffer sizes, retransmission timers, and congestion control algorithms.
    -   [ ] Benchmark SQP performance (latency, throughput) against RESP.
-   [ ] **6.4. Updated Test Suite (`test.py` - new `test_sqp.py`):**
    -   [ ] Create a new Python client specifically for SQP.
    -   [ ] Rewrite `test.py` to use the SQP client.
    -   [ ] Develop comprehensive unit and integration tests for all SQP features:
        -   Handshake success/failure.
        -   Basic Query/Response over UDP.
        -   Reliable message delivery (packet loss simulation).
        -   Multiplexed queries.
        -   Streaming results.
        -   TCP fallback scenarios.
        -   Security features (if implemented).
        -   Error handling.
    -   [ ] Update `tests/benchmark.py` to benchmark SQP.