# Commands and protocol

Recall's Linux prototype accepts RESP2 requests over TCP. This document describes the implemented command surface and its limits; it is not a production-readiness guarantee. Testing requirements are described in [validation and development](docs/validation.md).

Startup settings, literal environment-file syntax, precedence, and manual/container setup are documented in [configuration and deployment](deployment.md).

## Supported operations

| Commands | Exact initial boundary |
| --- | --- |
| [`PING`, `ECHO`, `QUIT`](crates/recall-core/src/command.rs:1) | RESP2 requests; optional ping payload; ordered pipelined execution |
| [`AUTH`, `HELLO`, `SELECT`](crates/recall-core/src/command.rs:1) | Optional configured default-user password; RESP2 only; database zero only |
| [`GET`, `SET`](crates/recall-core/src/command.rs:1) | Binary-safe; assignment supports mutually exclusive conditions and expiration options |
| [`INCR`, `INCRBY`, `DECR`, `DECRBY`](crates/recall-core/src/command.rs:1) | Canonical signed 64-bit decimal integers; checked arithmetic; retain existing expiry |
| [`DEL`, `EXISTS`, `MGET`, `MSET`](crates/recall-core/src/command.rs:1) | Atomic across all affected owners; preserve argument ordering and duplicate-key semantics |
| [`EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`](crates/recall-core/src/command.rs:1) | Basic forms only; explicit command-time expiry; bounded active reclamation |
| [`COMMAND`, `COMMAND INFO`, `COMMAND COUNT`](crates/recall-core/src/command.rs:1) | Real supported command metadata; no advertised unimplemented data types |
| [`CLIENT SETNAME`, `CLIENT GETNAME`, `CLIENT SETINFO`](crates/recall-core/src/command.rs:1) | Bounded client names and library metadata |
| [`INFO`](crates/recall-server/src/session.rs:1) | Recall identity, connection statistics, and owner-level memory/key statistics |

Assignment options: [`NX`, `XX`, `EX`, `PX`, `KEEPTTL`](crates/recall-core/src/command.rs:1). Unsupported or repeated option categories are rejected. The plain form clears expiry. Null conditional-assignment responses mean the requested change did not happen.

## Semantics and resource limits

- RESP2 arrays of non-null bulk-string arguments only. No inline protocol, nested request arrays, RESP3, multiple databases, transactions, scripting, collections, pub/sub, or replication.
- No persistence, snapshots, eviction, or online resharding in this milestone. All data disappears at process exit.
- Request bytes, bulk bytes, argument count, key/value length, response bytes, connection count, owner queue capacity, per-owner key capacity, and payload budgets are bounded. Excessive requests fail or close malformed connections rather than allocate without a limit. Connections beyond the connection limit receive an `ERR max number of clients reached` reply before close (best-effort: the bounded notice queue closes excess rejections silently); rejections are counted in `INFO` and logged on a bounded sample, not per connection.
- Initial memory budgets cover logical key/value payloads per owner. Key limits, startup metadata reservations, and separate network/queue limits constrain other categories, but this is **not an aggregate RSS ceiling or finished allocator accounting**. Replacement values retained by a response remain subject to response/connection bounds, not the live payload counter.
- The conventional table and indexed timers reserve capacity at startup. This reduces routine growth but is not a hard resize/rehash latency guarantee: deletion churn and implementation details can still cause table maintenance. Incremental bounded-growth storage remains a separate gate. Payload credits are partitioned across owners initially; skew can cause local rejection while another owner has space.
- Expiry is sampled once inside command execution. An entry is absent at or after its millisecond deadline. Remaining seconds round to the nearest second. Clock rollback may extend entries not yet removed; removed entries cannot resurrect in this memory-only process.
- Cross-shard commands use whole-owner reservations initially. Single-owner commands bypass the coordinator; unrelated owners continue running. The concurrent key-intent target is not implemented by this reference mechanism.
- Connections execute commands in order, with one executing request per connection. Socket reads and writes have deadlines. A disconnect after admission does not cancel a mutation; retries of counters are not exactly-once.
- Basic operational counters are implemented, not the final owner-local histogram/queue instrumentation. The network path still copies decoded arguments and encodes one response at a time; measured transport batching is pending.
- Authentication is optional for loopback development; nonloopback binds are rejected until the transport-security gate is implemented. Do not expose an unauthenticated instance through a public proxy or container port mapping.

## Protocol details

- Command names and supported option names are ASCII case-insensitive. Keys and values are binary-safe and case-sensitive.
- A request is an array of bulk-string arguments. Responses use simple strings, errors, signed integers, nullable bulk strings, and arrays.
- Signed integers use canonical decimal notation with checked 64-bit arithmetic. Overflow returns an error without changing the stored value.
- Multi-key reads retain argument order. Existence checks count each matching argument, deletion counts each removed key once, and repeated assignments to one key keep the final supplied value.
- Unknown commands and unsupported options return explicit errors. Malformed framing closes the connection after a bounded error response.

