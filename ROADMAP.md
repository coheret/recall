# Recall roadmap

Recall's development focuses on parallel execution, predictable resource use, reliable state management, and straightforward operation. Stages describe engineering outcomes and acceptance gates, not release dates.

**Current stage:** Non-production prototype. Substantial architecture, performance, security, and reliability work remains. Formatting, Clippy, test, and release-build gates pass in Linux CI, including the negative authorization and expanded parser, storage, coordination, saturation, and shutdown suites; the remaining priority is sustained stress and overload validation on a dedicated instance; see [validation and development](docs/validation.md).

**Platform:** Linux is the sole Tier 1 development and deployment target. Additional platform support is outside the current roadmap.

## Foundation already in source

- A three-component Rust workspace separating protocol, deterministic engine, and server integration.
- Bounded RESP2 framing, ordered connections, authentication, and basic operational information.
- Strings, conditional writes, checked counters, expiration, and multi-key commands.
- Independent owner threads, bounded item/byte admission, separate control channels, and whole-owner multi-key coordination.
- Indexed expiration, per-owner key/payload limits, and orderly shutdown paths.
- Unit and integration tests plus Linux CI definitions.
- A dependency-free Python repository checker, bounded environment-file settings, and manual/Docker deployment artifacts; Rust/container execution remains a separate validation gate.

Source presence does not mean a milestone is validated. The initial runtime remains memory-only and accepts loopback connections.

## 1. Validate and stabilize the foundation

**State:** In progress; diagnostics, negative authorization coverage, and the expanded stabilization test suites pass in Linux CI. Sustained stress and overload validation remains.

- Use the pinned toolchain and a generated dependency lock for locked builds.
- Run formatting, diagnostics, tests, and release builds on Linux.
- Verify missing/invalid credentials, connection-local authentication state, and negotiation/pipelining without authorization bypass.
- Resolve protocol, semantics, lifetime, and cancellation defects uncovered by execution.
- Expand deterministic/property tests, adversarial parser coverage, multi-key histories, saturated-queue tests, and shutdown tests.

**Exit:** Reproducible passing checks, documented command/resource behavior, and actionable reports for any remaining limitations.

## 2. Improve scheduling and resource control

**State:** Planned; builds on the validated foundation.

- Add bounded transport batching and owner-local queue/latency instrumentation.
- Account for live allocations, queued and prepared work, retained output, temporary growth, and reclamation lifetimes.
- Introduce bounded credit rebalancing across owners and predictable no-eviction admission behavior.
- Model-check atomicity/liveness and introduce key-level reservations so disjoint multi-key operations can overlap.
- Verify fair progress for ordinary requests, coordination, expiration, and control completions under pressure.

**Exit:** No partial publication or leaked reservations/credits; tested resource bounds and measurable latency/throughput behavior across skewed and saturated workloads.

## 3. Add durable state

**State:** Planned; memory-only operation remains an explicit mode.

- Implement a versioned effect log, record validation, replay, and exclusive data-directory ownership.
- Add periodic and strict synchronization modes with explicit acknowledgment boundaries and failure handling.
- Pipeline nonconflicting prepared writes and bounded group commit without exposing uncommitted state.
- Log expiration/deletion effects consistently and verify restart behavior across clock changes and owner counts.

**Exit:** Fault-injection tests establish whole-command recovery and the configured durability contract; disk failures never silently weaken that contract.

## 4. Complete the storage lifecycle

**State:** Planned; depends on durable state.

- Implement maintenance checkpoints, durable manifests, contiguous log coverage, and safe reclamation.
- Add startup-expiry handling, backup/restore validation, and disk-headroom protection.
- Test failures around snapshot writing, synchronization, manifest publication, directory updates, and old-segment deletion.

**Exit:** Validated restore/recovery paths and safe interrupted-checkpoint behavior without silently losing committed state.

## 5. Bound growth and enable online maintenance

**State:** Planned; depends on validated storage/resource primitives.

- Prototype segmented, incremental table growth and verify key visibility through each migration step.
- Add online checkpoints using a consistent log cut and bounded capture/retention budgets.
- Abort incomplete snapshot generations safely under pressure while preserving recoverable log coverage.
- Add opt-in, bounded cache eviction through the normal conflict and persistence paths.
- Validate interactions among retained output, snapshots, expiration, deletion, and actual memory release.

**Exit:** Bounded maintenance work and peak memory, consistent snapshots during traffic, safe cancellation, and workload evidence supporting each optimization.

## 6. Qualify production operation

**State:** Planned; security work can proceed in parallel without enabling public access prematurely.

- Complete transport security, authentication hardening, administration boundaries, and safe remote configuration.
- Add operational metrics, configuration documentation, packaging, and upgrade procedures.
- Qualify Linux filesystem synchronization and publication ordering through appropriate crash experiments.
- Establish workload-specific throughput, tail-latency, memory, recovery, and overload acceptance thresholds.
- Exercise uniform/skewed keys, hot keys, mixed traffic, large values, expiry churn, slow clients, storage stalls, and maintenance under load.

**Exit:** Supported platform/storage assumptions, reproducible operational/performance results, and documented deployment and recovery procedures.

## Later design proposals

Additional data structures, user transactions, scripting, blocking operations, publish/subscribe, RESP3, replication, clustering, and online resharding require separate scope and correctness designs. They are not implicitly included in the initial release.

## How to contribute

Use [the contribution guide](CONTRIBUTING.md) to propose focused work. [The architecture](plans/architecture.md) defines correctness and storage guarantees; [the performance design](plans/performance.md) details the optimization mechanisms.

Advance a stage only when its acceptance evidence exists. Update this roadmap, the command documentation, and validation records together when shipped behavior changes.
