# Recall

**A multithreaded in-memory cache server built in Rust.**

> [!WARNING]
> **Recall is not ready for production.** It is an early prototype that still requires substantial architectural overhaul, performance engineering, security hardening, and reliability testing before it can perform well under production workloads. Use it only for development and evaluation; do not rely on it for production services or critical data.

Recall stores application data in memory and executes commands across dedicated worker threads. Each worker owns a portion of the keyspace, allowing independent operations to run in parallel while keeping conflicting updates ordered.

Applications connect directly to Recall over TCP using RESP2. Storage, command execution, expiration, and connection handling are all part of the server.

## Prototype capabilities

- **Parallel command execution.** Keys are routed to single-owner workers rather than protected by one database-wide lock.
- **Atomic operations.** Conditional writes, checked counters, and multi-key operations use preparation and validation before publication.
- **Automatic expiration.** Millisecond deadlines, lazy expiry on access, and incremental cleanup keep temporary data manageable. Updating a deadline reuses its indexed timer entry.
- **Bounded request handling.** Limits on frame size, arguments, queue bytes, connections, and replies constrain work admitted into the server.
- **Ordered connections.** Pipelined commands execute in connection order; a slow connection does not hold a keyspace reservation while sending its response.
- **Operational visibility.** Server information includes owner-level key counts, payload usage, expiration counts, and basic connection statistics.

See [commands and protocol](docs/commands.md) for supported operations, options, and resource limits.

## How it works

1. The network layer incrementally decodes a bounded request.
2. Key routing selects the worker that owns the data.
3. That worker validates the operation, prepares its effects, and applies them to its local storage.
4. Commands spanning workers coordinate their affected owners and publish only after every participant is ready.
5. The connection receives its response in order.

Protocol parsing, deterministic storage semantics, and server concurrency are separate components. This keeps the storage engine independently testable and makes ownership rules explicit.

## Build

**Linux is Recall's sole Tier 1 development and deployment target.** Windows and other operating systems are not currently supported. Tier 1 identifies the engineering focus, not production readiness.

Use Rust 1.85.1, selected by [the toolchain configuration](rust-toolchain.toml). From the repository root on Linux:

1. If [the dependency lock](Cargo.lock) is absent, run [`cargo generate-lockfile`](Cargo.toml:1). Locked builds require this generated build input.
2. Run [`cargo test --workspace --locked`](Cargo.toml:1).
3. Build with [`cargo build --release -p recall-server --locked`](Cargo.toml:1).

## Run

Start a local instance with [`cargo run --release -p recall-server -- --bind 127.0.0.1:6379 --workers 4 --max-memory-mib 512`](crates/recall-server/src/main.rs:1).

Use [`--help`](crates/recall-server/src/main.rs:1) for available options. Configure optional default-user authentication through [`RECALL_PASSWORD`](crates/recall-server/src/main.rs:1) in the process environment, not command-line arguments.

The memory setting limits live key/value payloads; it is not a process RSS ceiling. Worker metadata, queued requests, and connection buffers have separate limits.

## Development status

The initial server is **memory-only and loopback-only**. Data is lost when the process exits. Durable storage, encrypted remote access, eviction policies, and online checkpoints are planned separately.

The current implementations are a foundation for further development, not production-grade execution or resource management. Formatting, Clippy, and the test suites pass in Linux CI, including negative authorization and adversarial parser, capacity, coordination, and shutdown cases; validation work now prioritizes sustained resource pressure and overload testing on a dedicated instance. See [validation and development](docs/validation.md).

## Configuration and deployment

Copy [the environment template](.env.example) to private local configuration. Startup settings follow command-line options, process environment, the environment file, then defaults. Private configuration is not included in image builds.

[The deployment guide](docs/deployment.md) covers manual execution, an optional Linux service, and a non-root Docker image with native-Linux Compose configuration. Both paths preserve the current loopback-only listener and memory-only behavior.

For a Docker-only first checkout without [the dependency lock](Cargo.lock), generate it with [`docker build --target lockfile --output type=local,dest=. .`](Dockerfile:1), then follow the deployment guide. The server-image build remains locked.

## Static checks

Run [`python -B tools/static_check.py`](tools/static_check.py:1) for dependency-free, read-only repository checks with Python 3.11+. No Rust toolchain is required for this check. [The checker guide](docs/static-checks.md) explains its scope and machine-readable output; it does not replace compilation or runtime tests.

## Contributing

Read [the contribution guide](CONTRIBUTING.md) for setup, testing, pull requests, and responsible AI assistance. [The roadmap](ROADMAP.md) describes the next engineering stages and their acceptance gates.

AI-assisted contributors should use [the shared agent guide](AGENT.md). [The Claude Code entry point](CLAUDE.md) imports the same guidance.

## Project structure

- [Protocol](crates/recall-protocol/src/lib.rs): incremental RESP2 decoding and response encoding.
- [Core](crates/recall-core/src/lib.rs): command semantics, owner-local storage, and indexed expiration.
- [Server](crates/recall-server/src/lib.rs): networking, admission, worker routing, and coordination.
- [Architecture](plans/architecture.md): ownership, atomicity, resource, and recovery design.
- [Performance engineering](plans/performance.md): pipelined execution, fine-grained coordination, bounded growth, and online maintenance.
