# Security review

## Boundary and assumptions

RelayFlow is a local, single-trust-domain orchestration engine. It does not authenticate clients, isolate tenants, terminate TLS, store user secrets, or execute arbitrary code. The built-in Compose credentials are development defaults, not production credentials. Any network exposure would first require authentication, authorization, TLS, secret management, and tenant-aware quotas.

## Implemented controls

| Risk | Control |
|---|---|
| Oversized or ambiguous request | 1 MiB body limit, required JSON media type, exactly one JSON document |
| Schema confusion | Unknown top-level and handler-input fields rejected |
| Invalid identifiers | UUID route validation and constrained idempotency/step keys |
| Permanently blocked work | Full dependency validation and cycle rejection before persistence |
| Resource abuse | 100-step maximum, bounded retry/timeout/backoff, bounded worker lanes |
| SQL injection | Parameterized PostgreSQL statements; dynamic user input is never SQL text |
| Arbitrary execution | Fixed handler allowlist; no shell, plugin, or uploaded-code handler |
| Stale owner writes | Lease token plus unexpired-lease fencing on heartbeat and terminal writes |
| Audit tampering | Database trigger rejects event update and delete |
| Slow HTTP clients | header/read/write/idle deadlines and a 32 KiB header limit |
| Information leakage | Internal store errors are logged; API returns stable generic server errors |
| Container privilege | Runtime image uses a dedicated non-root user |

Validation is duplicated intentionally: the HTTP boundary rejects invalid work, and workers defensively decode persisted handler input before execution.

## Review checks

- Source inspection found no embedded API tokens, private keys, or production credentials.
- The only checked-in password is the explicit local Compose credential.
- Dependencies are pinned by `go.mod`/`go.sum`; CI recompiles and tests them.
- API and domain tests cover missing media type, trailing JSON, malformed UUIDs, unknown fields, unsafe keys, invalid policy bounds, and invalid handler payloads.
- Docker images compile with `-trimpath`, contain only the binary and migrations, and run non-root.

## Residual risks

- There is no authentication, authorization, per-tenant isolation, TLS, rate limiting, or request-level quota.
- PostgreSQL is a single availability and trust boundary.
- A compromised database administrator can alter state despite application invariants.
- Downstream side effects require their own idempotency enforcement; fencing only protects RelayFlow state.
- Dependency vulnerability scanning and signed build provenance are appropriate production additions, but are not a substitute for the local threat model.
