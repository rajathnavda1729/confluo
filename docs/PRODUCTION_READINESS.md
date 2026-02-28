# System-Architect Review: Production Readiness

This document is a **system-architect assessment** of Omni-Joiner: whether it is production-grade, what is strong today, and what should be addressed before or after rollout.

---

## 1. Verdict

**Production-capable for a bounded scope**, with clear operational and design limits. The core is sound: at-least-once processing, correct use of Kafka ordering, Scylla for state, and Redis for Bloom/timeouts. For **single join-config, moderate throughput, and idempotent downstream consumers**, it can run in production today **if** you accept the gaps below and operate within the documented constraints. For **high-scale, multi-tenant, or strict SLAs**, treat the “Gaps and recommendations” section as a roadmap.

---

## 2. Architectural View

### 2.1 What the system is

- **N-way stream join engine:** Events from multiple logical streams (identified by `x-stream-id`) arrive on a single Kafka topic; the processor maintains per–join-key state in ScyllaDB and publishes a joined record when all N participants have arrived (inner join) or on TTL (partial egress).
- **Stateful stream processor:** State is keyed by join key (or composite key); Kafka partition = f(join key) so that per-key order is preserved and the consumer group can scale with partitions.
- **Dependencies:** Redpanda (Kafka API), ScyllaDB (durable state), Redis (Bloom filter, timeout ZSET, partial tracker). Optional: Redis + corrections topic for late-arrival corrections.

### 2.2 Design strengths

| Area | Assessment |
|------|------------|
| **Data flow** | Clear: consume → Bloom check → Scylla upsert → completion check → project → produce (or delay/timeout path). No silent drops on the critical path; state is deleted only after successful egress (see [CODE_QUALITY_REVIEW.md](CODE_QUALITY_REVIEW.md)). |
| **Ordering** | Per-key FIFO is delegated to producers (Kafka message key = join key). Consumer group processes partitions in order; no out-of-order commits. Documented in [ORDERING.md](ORDERING.md). |
| **Semantics** | At-least-once: commit only after successful handle. Restart/reprocess can duplicate egress; downstream must be idempotent (upsert by join key). |
| **State store** | ScyllaDB map updates are atomic; first-arrival optimization (Bloom + UpsertParticipantOnly) reduces read load. |
| **Observability** | Prometheus metrics: join success, join latency, state age, events processed. Exposed on `:9090/metrics`. |
| **Configuration** | File-based join and processor config; no secrets in code; config-driven projection and egress policy. |
| **Documentation** | [ARCHITECTURE.md](ARCHITECTURE.md), [ORDERING.md](ORDERING.md), [LATE_ARRIVAL.md](LATE_ARRIVAL.md), [TESTING.md](TESTING.md) give a clear picture of components, flow, and usage. |

### 2.3 Intended deployment model

- **Single process** running one consumer group and one join config (or one config file that defines one join). Scaling is horizontal by adding partitions and more consumer instances.
- **Infra:** Redpanda, ScyllaDB, and Redis are separate services; processor connects via config. Docker Compose is provided for local/dev; production would use your own orchestration (e.g. Kubernetes) and managed or self-hosted Redpanda, Scylla, Redis.

---

## 3. Gaps and Recommendations

### 3.1 Operability

| Gap | Impact | Recommendation |
|-----|--------|-----------------|
| **No health/readiness HTTP** | Orchestrators (e.g. K8s) cannot distinguish “process up” from “consuming and connected”. A pod can be up but stuck on Kafka or Scylla. | Add `/health` (liveness) and `/ready` (readiness: Kafka + Scylla + optional Redis connected, consumer running). |
| **No structured logging** | `log.Printf` is hard to parse in log aggregators; no log levels or request/correlation IDs. | Introduce a small logger interface and use structured (e.g. JSON) logs with levels; add trace/span IDs when processing a record. |
| **Failure metrics missing** | Handle errors, commit errors, and produce failures (delay queue, timeout manager) are only logged. | Add Prometheus counters (e.g. `omni_joiner_handle_errors_total`, `omni_joiner_commit_errors_total`, `omni_joiner_egress_failures_total`) for alerting and SLOs. |

### 3.2 Durability and correctness

| Gap | Impact | Recommendation |
|-----|--------|-----------------|
| **Delay queue in-memory** | Pending delayed publishes are lost on process restart. Joins that completed but were waiting for `post_join_delay` will never be published unless reprocessed (and state was not yet deleted). | Document as a limitation; for production with delay, consider a durable delay store (e.g. Redis or a “delay” topic with consumer that re-enqueues by timestamp) or accept best-effort delay after restart. |
| **Config not validated at startup** | Invalid join config (e.g. projection field referencing unknown stream) can cause runtime errors. | Validate join config on startup (stream_ids, projection stream refs, key fields) and fail fast with a clear message. |
| **Bloom false positives** | Bloom “maybe seen” can force a read when the key was never stored (e.g. after Scylla compaction or key eviction). Extra read is safe but adds load. | Document as an operational consideration; optionally expose a metric for “first arrival vs follow-up” to tune Bloom capacity/error rate. |

### 3.3 Scaling and multi-tenancy

| Gap | Impact | Recommendation |
|-----|--------|-----------------|
| **One join config per process** | Multiple join definitions require multiple processor instances or multiple config files and process groups. | Document as the current model; later, support multiple join configs per process (e.g. by `config_id` in message or topic routing). |
| **No guidance on partition count** | Throughput and parallelism depend on input topic partitions; over-partitioning can increase state spread. | Add an ops doc or ARCHITECTURE section: recommend partition count based on target throughput and consumer count; mention key cardinality and Scylla/Redis capacity. |
| **Shared Scylla/Redis** | All keys and timeouts share the same keyspace and Redis keys; no per-tenant or per-join isolation. | Acceptable for single-tenant or few joins; for multi-tenant, document that keyspace/Redis prefix could be made configurable per join. |

### 3.4 Security and configuration

| Gap | Impact | Recommendation |
|-----|--------|-----------------|
| **Secrets in config file** | Brokers, Redis address, and (if used) passwords are in JSON; no integration with a secret manager. | Document that config may contain sensitive data; recommend file permissions and, for production, support env substitution or a secret backend (e.g. env vars for passwords). |
| **No auth for /metrics** | Metrics endpoint is unauthenticated; anyone with network access can scrape. | In production, put the process behind a reverse proxy or service mesh that enforces auth; or add optional basic auth / mTLS for the metrics server. |

### 3.5 Resilience

| Gap | Impact | Recommendation |
|-----|--------|-----------------|
| **Commit failure stops consumer** | On `CommitRecords` failure, `Run` returns and the process exits (or restarts). Reprocessing of the last batch is expected. | Document; ensure orchestration restarts the process; consider a bounded retry before exit. |
| **No backpressure on timeout burst** | If many keys time out in the same window, the timeout manager can publish and delete in a burst. | Acceptable for moderate TTL and key count; document; if needed, add rate limiting or batching for partial egress. |
| **Single process** | No in-process HA; availability is “restart + rebalance”. | Rely on consumer group rebalance and fast restart; document that at-least-once implies possible duplicate egress after failover. |

---

## 4. Production Checklist (Summary)

Use this as a quick checklist before treating the system as production-grade.

- [ ] **Downstream is idempotent** — Egress (and corrections) are at-least-once; consumers must upsert by join key.
- [ ] **Kafka key = join key** — Producers set message key so that ordering and partitioning are correct ([ORDERING.md](ORDERING.md)).
- [ ] **Config and secrets** — Join and processor config validated; secrets not committed; file permissions or secret manager in use.
- [ ] **Observability** — Metrics scraped (e.g. Prometheus); dashboards for join rate, latency, and (once added) errors; alerting on commit/handle/produce failures.
- [ ] **Health/readiness** — Once implemented, liveness/readiness used by orchestrator; no traffic until ready.
- [ ] **Delay queue** — If `post_join_delay` is used, understand that pending delayed items are lost on restart; accept or add durability.
- [ ] **Capacity** — Partition count, Scylla/Redis sizing, and Bloom parameters tuned for key cardinality and throughput ([ARCHITECTURE.md](ARCHITECTURE.md)).

---

## 5. Conclusion

As a **system architect**, I view Omni-Joiner as a **well-structured, production-capable stream join engine** for the right scope: single join config, at-least-once semantics, idempotent downstream, and clear documentation. The codebase follows production-oriented practices (error handling, no silent drops, metrics, context propagation). To be **fully production-grade** in a demanding environment, prioritize: **health/readiness endpoints**, **structured logging**, **failure metrics**, **config validation**, and **documented limits** (delay queue, one config per process, partition/capacity guidance). With those in place, the system is suitable for production use within the documented constraints.
