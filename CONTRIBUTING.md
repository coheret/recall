# Contributing to Recall

Recall is a multithreaded in-memory cache server built in Rust. Contributions should strengthen correctness, predictable resource use, useful capabilities, and maintainable performance.

Recall is a non-production prototype. **Linux is the sole Tier 1 development, testing, and deployment target.** Other operating systems are outside the current support scope.

## Before starting

- Read [the overview](README.md), [command contract](docs/commands.md), and [roadmap](ROADMAP.md).
- For changes to execution, memory, or storage, read [the architecture](plans/architecture.md) and [performance design](plans/performance.md).
- Start with a focused issue or a small, clearly described fix. Discuss new public commands, storage formats, dependencies, security boundaries, or substantial architecture changes with maintainers before implementing them.
- Keep each pull request centered on one coherent outcome. Separate unrelated cleanup from behavior changes.

Documentation, reproducible bug reports, targeted tests, and profiling investigations are valuable contributions alongside features.

## Development setup

Use the toolchain selected by [the toolchain configuration](rust-toolchain.toml), currently Rust 1.85.1. Commands below run from the repository root in an authorized Linux workspace.

For an offline first pass, run [`python -B tools/static_check.py`](tools/static_check.py:1) with Python 3.11+; no third-party packages are required. Checker changes also require [`python -B -m unittest discover -s tools -p test_static_check.py -v`](tools/test_static_check.py:1). [The checker guide](docs/static-checks.md) separates these checks from Rust verification.

1. If absent, generate [the dependency lock](Cargo.lock) using [`cargo generate-lockfile`](Cargo.toml:1). It is required by locked builds; do not fabricate or hand-edit dependency resolution.
2. Format with [`cargo fmt --all`](Cargo.toml:1).
3. Verify formatting with [`cargo fmt --all -- --check`](Cargo.toml:1).
4. Run diagnostics with [`cargo clippy --workspace --all-targets --locked -- -D warnings`](Cargo.toml:1).
5. Run tests with [`cargo test --workspace --locked`](Cargo.toml:1).
6. Check the release build with [`cargo build --workspace --release --locked`](Cargo.toml:1).

[CI](.github/workflows/ci.yml) checks formatting and runs Clippy in independent jobs; it also retains Python repository checks, tests, and release builds. Unlike the local Clippy command above, CI omits `--locked`, allowing Cargo to create or update the lock in the runner checkout. See [GitHub Actions validation](docs/validation.md#github-actions-no-local-rust-or-docker) for CI setup, lock handling, and manual formatting corrections.

Run focused tests while developing, then the full relevant set before requesting review. If your environment cannot execute a check, record that limitation in the pull request; do not mark it as passed.

For a local instance, use [the run instructions](README.md) and [configuration/deployment guide](docs/deployment.md). The initial server is memory-only and loopback-only. Copy the tracked environment example rather than sharing a private file. Use synthetic data; never test against a production instance or expose a development instance publicly.

## Code and design expectations

- Preserve component boundaries: [protocol framing](crates/recall-protocol/src/lib.rs), [deterministic core](crates/recall-core/src/lib.rs), and [server integration](crates/recall-server/src/lib.rs).
- Keep mutable keyspace state owner-local. Explain how concurrency changes preserve connection order, atomicity, reservation release, and completion after disconnects.
- Use checked arithmetic and bounded parsing, admission, and background work. Document temporary allocation costs, not just final key/value size.
- Pass time into deterministic engine logic. Make expiry and scheduling changes testable without timing-sensitive sleeps where possible.
- Preserve the [safe-Rust lint policy](Cargo.toml:20). Do not suppress diagnostics or introduce unsafe application code to bypass a design problem.
- Add dependencies deliberately, using pinned workspace configuration and a reviewed lock update. Avoid broad upgrades bundled with unrelated fixes.
- Update command/configuration documentation when observable behavior changes. Keep planned behavior separate from implemented behavior.
- Explain nonobvious invariants in code comments. Prefer small, testable functions and explicit state transitions over speculative abstractions.

## Testing expectations

| Change | Expected coverage |
| --- | --- |
| Parser or protocol | Fragmented frames, binary data, malformed lengths, boundary sizes, pipeline order, bounded allocation |
| Command semantics | Missing keys, duplicate arguments, invalid options, integer boundaries, deadline boundaries, failure without partial mutation |
| Concurrency or coordination | Conflicting/disjoint keys, cross-owner publication, cancellation/disconnects, saturated queues, shutdown, reservation/credit release |
| Memory or expiration | Churn, repeated timer updates, retained response buffers, admission failure, bounded cleanup, skew |
| Persistence or checkpoints | Decision boundaries, short writes, storage errors, crash injection, complete recovery, interrupted publication and reclamation |
| Performance optimization | Reproducible workload and resource configuration; throughput, latency distribution, memory, errors/rejections, and regression checks |
| Documentation only | Accurate status, valid links, correct names/commands, and no implied unimplemented capabilities |

For a bug fix, include a regression test that captures the failure. Do not relax tests solely to accept new behavior; intentional contract changes need discussion. The [validation guide](docs/validation.md) lists the current suites and outstanding checks.

## Pull requests

Use a descriptive title and include:

1. **Problem and scope:** the issue, expected behavior, and what is intentionally unchanged.
2. **Approach:** key design choices and tradeoffs; explain ownership/resource implications where relevant.
3. **Verification:** checks run, a concise result summary, and any checks not run. Private deployment records and raw execution logs are not required.
4. **Compatibility and operations:** command/configuration changes, memory costs, migration needs, or security impact.
5. **Documentation and follow-up:** relevant updates and remaining work without claiming it is complete.
6. **AI assistance, when material:** what the tool helped produce and how you personally reviewed and verified it.

Review the final diff yourself, remove unrelated changes and sensitive data, and respond to review feedback. Passing CI is necessary for code changes but does not replace human design review. Maintainers decide when a change is ready to merge.

## Responsible AI assistance

AI tools may help explain code, investigate failures, draft changes, or develop tests. The contributor must understand, review, and take responsibility for everything submitted.

- Give your assistant [the shared guide](AGENT.md). Claude Code also reads [its entry point](CLAUDE.md); tools using the conventional agent filename can discover [the forwarding guide](AGENTS.md).
- Use only services authorized for the code and data involved. Never submit credentials, private datasets, dumps, or internal conversations to unapproved tools.
- Verify generated code, APIs, assumptions, and reused material. Only submit code and documentation you have the right to contribute; discuss unclear licensing/provenance with maintainers.
- Keep public documentation focused on stable technical guidance. Do not copy internal conversations, terminal sessions, private environment details, or ad hoc test-run narratives into it.
- Do not submit unreviewed bulk output, invented test results, fabricated benchmarks, or generated claims you cannot substantiate.
- Briefly disclose substantial AI-generated code, tests, or documentation in the pull request. A raw transcript is neither required nor appropriate; keep confidential prompts and discussions private.
- AI tools do not approve, merge, publish, or deploy contributions on a maintainer's behalf.

## Bug reports and sensitive issues

For ordinary bugs, describe the expected/actual behavior and provide a minimal synthetic reproduction. Include only the settings or minimal sanitized diagnostic needed to explain the issue. Do not include private host details, deployment inventories, raw execution logs, or private keys/values.

For security-sensitive findings, follow [the security policy](SECURITY.md): use the repository's private reporting facility if enabled, or contact a maintainer privately before posting details. Do not put credentials, exploit-sensitive production details, or customer data into a public issue.
