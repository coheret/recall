# Security policy

Recall is a non-production prototype. It has no supported production releases, and no version currently receives guaranteed security maintenance. This document explains how to report vulnerabilities and which boundaries the current implementation does and does not enforce.

## Reporting a vulnerability

- **Do not open a public issue for a security-sensitive finding.** Use the repository's private vulnerability reporting facility if it is enabled, or contact a maintainer privately before posting details. [The contribution guide](CONTRIBUTING.md) describes the same expectations for sensitive issues.
- Include the expected and actual behavior, a minimal synthetic reproduction, and the commit or version tested. Never include real credentials, private environment files, production details, or customer data; use synthetic values only.
- Test only against instances you own. Recall's checks target dedicated loopback instances with synthetic data, as described in [the validation guide](docs/validation.md); do not run security probes against third-party or shared deployments.

Reports are reviewed by volunteers without a response-time commitment. Maintainers coordinate any fix and disclosure timeline with the reporter.

## Current security boundaries

The initial milestone deliberately restricts exposure. Deployments must preserve these boundaries rather than weaken them:

- **Loopback only.** The server rejects non-loopback bind addresses at startup ([configuration validation](crates/recall-server/src/config.rs:1)). There is no TLS or remote-access mode, and the loopback check must not be removed to work around connectivity problems. Do not expose an instance through a public proxy or container port mapping.
- **Optional default-user authentication.** A single configured password gates every command on a connection ([command contract](docs/commands.md)); there are no per-user ACLs. Password comparison is constant-time, empty or oversized configured passwords fail startup instead of disabling authentication, and replies and diagnostics are written to avoid echoing credentials or protected values.
- **Memory-only state.** Nothing is persisted; stopping or crashing the process loses the dataset. Durability claims belong to future [roadmap](ROADMAP.md) stages.
- **Bounded resources.** Frames, arguments, queues, connections, and replies carry fixed byte and item limits so overload is rejected rather than absorbed; see [the architecture](plans/architecture.md).
- **Non-root deployment artifacts.** The provided [service unit](deploy/recall.service) and [container recipe](compose.yaml) run unprivileged and keep private environment files out of build inputs and image metadata; see [the deployment guide](docs/deployment.md).

## Known gaps and out-of-scope work

Transport encryption, multi-user authorization, administrative command boundaries, audit logging, and safe remote configuration are not implemented. They are scheduled under [production qualification](ROADMAP.md) and must land, with their own correctness gates, before any non-loopback exposure. Loopback authentication alone does not make an instance safe to tunnel or proxy to untrusted networks.

Reports are welcome for failures within the current boundaries — for example, authentication bypass, negotiation that applies options without valid credentials, protocol-reachable unbounded allocation, or diagnostics that disclose configured secrets. Gaps already documented above as planned work are not, by themselves, vulnerabilities.
