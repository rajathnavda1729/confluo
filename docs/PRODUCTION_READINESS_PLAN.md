# Production Readiness Implementation Plan

This plan turns the gaps and recommendations from the [System-Architect Review](PRODUCTION_READINESS.md) into actionable work items. Items are grouped by **phase** (P0 = before production, P1 = soon after rollout, P2 = later) and can be tracked in the [DEVELOPMENT.md](DEVELOPMENT.md) checklist.

---

## Phase P0 — Before production (must-have)

These items should be done before treating the system as production-grade in a demanding environment.

| ID | Item | Source § | Description | Acceptance criteria |
|----|------|----------|-------------|---------------------|
| **P0.1** | Health and readiness endpoints | 3.1 Operability | Add HTTP endpoints for liveness and readiness. **Liveness** (`/health` or `/live`): process is running. **Readiness** (`/ready`): Kafka client connected, Scylla session healthy, and (if configured) Redis reachable; consumer loop considered "running" (e.g. first successful poll or explicit "ready" flag after startup). | GET `/health` returns 200; GET `/ready` returns 200 only when dependencies are connected; orchestrator can use these for liveness/readiness probes. |
| **P0.2** | Failure metrics | 3.1 Operability | Add Prometheus counters for handle errors, commit errors, and egress (produce) failures in consumer, delay queue, and timeout manager. Labels: `config` (join name), optional `reason` or `phase`. | Counters `omni_joiner_handle_errors_total`, `omni_joiner_commit_errors_total`, `omni_joiner_egress_failures_total` (or similar) exposed on `/metrics`; documented in README or OPERATIONS. |
| **P0.3** | Join config validation at startup | 3.2 Durability | Validate join config after load: `stream_ids` non-empty; every `projection` entry references a stream in `stream_ids`; key field or key fields present and non-empty; TTL > 0 when egress is partial. Fail startup with a clear error message if invalid. | **Done.** Invalid config causes process to exit with non-zero code and readable error. `internal/config.ValidateJoinConfig`; tests in `config_test.go`. |
| **P0.4** | Document delay queue limitation | 3.2 Durability | Document in PRODUCTION_READINESS.md and TESTING.md (or OPERATIONS) that the delay queue is in-memory: pending delayed publishes are lost on restart. State is already deleted when scheduling delay, so reprocess may not re-emit; recommend avoiding long post_join_delay in production or accepting best-effort. | **Done.** §3.2.1 in [PRODUCTION_READINESS.md](PRODUCTION_READINESS.md#321-delay-queue-in-memory-limitation); [TESTING.md](TESTING.md) and [ARCHITECTURE.md](ARCHITECTURE.md) §4.3. |
| **P0.5** | Document config and secrets | 3.4 Security | Add a short "Config and secrets" section to OPERATIONS or PRODUCTION_READINESS: config file may contain broker URLs and Redis address; recommend file permissions (e.g. 0600) and, for production, env substitution or secret manager for passwords. | **Done.** [§3.4.1 Config and secrets](PRODUCTION_READINESS.md#341-config-and-secrets) in PRODUCTION_READINESS.md. |

---

## Phase P1 — Soon after rollout (should-have)

Improve operability and observability once the system is running in production.

| ID | Item | Source § | Description | Acceptance criteria |
|----|------|----------|-------------|---------------------|
| **P1.1** | Structured logging | 3.1 Operability | Introduce a small logger interface (e.g. `Logger` with `Info`, `Warn`, `Error`) and optional JSON output. Use it in consumer, delay, timeout, and main. Support log level (e.g. env `LOG_LEVEL=info`). Optionally add a request/correlation ID when processing a record (e.g. partition+offset or trace ID). | **Done.** `internal/logger`; LOG_LEVEL, LOG_FORMAT=json; key events with level and context. |
| **P1.2** | Capacity and partition guidance | 3.3 Scaling | Add an "Operations" or "Capacity" section to ARCHITECTURE.md or new docs/OPERATIONS.md: recommend partition count vs target throughput and consumer count; mention key cardinality impact on Scylla/Redis; Bloom filter capacity and error rate tuning. | **Done.** ARCHITECTURE.md §6 Operations and capacity. |
| **P1.3** | Commit failure retry | 3.5 Resilience | In consumer `Run`, on `CommitRecords` failure, retry a bounded number of times (e.g. 3) with backoff before returning/exiting. Log each retry. | **Done.** `commitWithRetry` in consumer; 3 attempts, 2s backoff; each retry logged. |
| **P1.4** | Bloom operational note | 3.2 Durability | Document in OPERATIONS or ARCHITECTURE: Bloom false positives cause an extra Scylla read; safe but adds load. Optionally add a metric for "first arrival" vs "follow-up" (e.g. counter by path) to tune Bloom. | **Done.** ARCHITECTURE.md §6.3; optional metric left for later. |

---

## Phase P2 — Later (nice-to-have)

Scaling, multi-tenancy, and durability improvements.

| ID | Item | Source § | Description | Acceptance criteria |
|----|------|----------|-------------|---------------------|
| **P2.1** | Durable delay queue | 3.2 Durability | Persist pending delayed items (e.g. Redis sorted set by publish time, or a dedicated "delay" topic with re-enqueue by timestamp). On restart, reload due items and publish. | Restart does not lose pending delayed publishes; behavior documented. |
| **P2.2** | Multiple join configs per process | 3.3 Scaling | Support multiple join configs in one process (e.g. list of config paths or directory). Route incoming messages by `config_id` in header or topic. Each config has its own logical state (same Scylla keyspace/Redis prefix or configurable). | One process can run N join definitions; docs and config schema updated. |
| **P2.3** | Optional auth for /metrics | 3.4 Security | Add optional basic auth or mTLS for the metrics server (e.g. env `METRICS_USER`/`METRICS_PASSWORD` or TLS config). Default remains no auth; document that in production, reverse proxy or mesh can enforce auth. | If configured, /metrics requires auth; doc updated. |
| **P2.4** | Timeout manager backpressure | 3.5 Resilience | Optionally add rate limiting or batching for partial egress when many keys time out in the same window. Document default behavior. | Burst of timeouts does not overwhelm Kafka/Scylla; or documented as acceptable. |

---

## Summary table

| Phase | Focus | Items |
|-------|--------|--------|
| **P0** | Health, metrics, validation, docs | P0.1–P0.5 |
| **P1** | Logging, ops guide, retry, Bloom note | P1.1–P1.4 |
| **P2** | Durable delay, multi-config, metrics auth, backpressure | P2.1–P2.4 |

---

## How to use this plan

1. **Before production:** Complete all P0 items and run through the [Production checklist](PRODUCTION_READINESS.md#4-production-checklist-summary) in PRODUCTION_READINESS.md.
2. **Tracking:** Copy the P0/P1 table into your issue tracker or mark items in DEVELOPMENT.md (Phase 4 checklist).
3. **Order within a phase:** Implement in any order unless a dependency is noted (e.g. P0.1 health/readiness can be done first; P0.2 failure metrics can follow).
4. **Tests and docs:** For each item, add or update tests and documentation per [DEVELOPMENT.md](DEVELOPMENT.md) §3–4.

---

## Reference

- **Source review:** [PRODUCTION_READINESS.md](PRODUCTION_READINESS.md) — §3 Gaps and recommendations.
- **Checklist:** [PRODUCTION_READINESS.md §4](PRODUCTION_READINESS.md#4-production-checklist-summary) — production checklist.
- **Process:** [DEVELOPMENT.md](DEVELOPMENT.md) — phase-wise execution, tests, coding standards.
