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
| **No structured logging** | `log.Printf` is hard to parse in log aggregators; no log levels or request/correlation IDs. | **Done (P1.1):** Logger interface (`internal/logger`) with Info, Warn, Error, optional JSON (`LOG_FORMAT=json`), level from `LOG_LEVEL`; used in consumer, delay, timeout, main. Key events logged with level and context. |
| **Failure metrics missing** | Handle errors, commit errors, and produce failures (delay queue, timeout manager) are only logged. | **Done (P0.2):** Counters `omni_joiner_handle_errors_total`, `omni_joiner_commit_errors_total`, `omni_joiner_egress_failures_total` (labels: config, phase) on `/metrics`. See [TESTING.md](TESTING.md). |

### 3.2 Durability and correctness

| Gap | Impact | Recommendation |
|-----|--------|-----------------|
| **Delay queue in-memory** | Pending delayed publishes are lost on process restart. Joins that completed but were waiting for `post_join_delay` will never be published unless reprocessed (and state was not yet deleted). | **Documented (P0.4):** See §3.2.1 below. Use short delay or accept best-effort; future durable store is P2.1. |
| **Config not validated at startup** | Invalid join config (e.g. projection field referencing unknown stream) can cause runtime errors. | **Done (P0.3):** Validate join config on startup (stream_ids, projection stream refs, key fields, partial+TTL); fail fast with clear message. See `internal/config.ValidateJoinConfig` and tests in `config_test.go`. |
| **Bloom false positives** | Bloom "maybe seen" can force a read when the key was never stored (e.g. after Scylla compaction or key eviction). Extra read is safe but adds load. | **Documented (P1.4):** ARCHITECTURE.md §6.3; size Bloom for key cardinality; optional metric (e.g. first-arrival counter) for tuning is P1.4 optional. |

#### 3.2.1 Delay queue: in-memory limitation

The **post-join delay queue** is **in-memory only**. When a join completes and `post_join_delay` is set, the joined result is scheduled in process memory and published after the delay. If the process restarts or is killed before the delay elapses:

- **Pending delayed items are lost** — they are not persisted to Scylla, Redis, or Kafka.
- **State is already deleted** when the join completes and the item is enqueued, so reprocessing the same input will not re-run the join for that key; the joined record will not be re-emitted unless you replay from before the join completed (e.g. reset offsets), which may not be feasible.

**Recommendation:** For production use of `post_join_delay`:

- Prefer **short delays** (e.g. a few seconds) so the window of loss on restart is small, or
- **Accept best-effort** delivery for delayed publishes (e.g. if downstream can tolerate occasional gaps), or
- Plan for a **durable delay store** later (see P2.1 in [PRODUCTION_READINESS_PLAN.md](PRODUCTION_READINESS_PLAN.md): Redis sorted set or a delay topic with re-enqueue by timestamp).

### 3.3 Scaling and multi-tenancy

| Gap | Impact | Recommendation |
|-----|--------|-----------------|
| **One join config per process** | Multiple join definitions require multiple processor instances or multiple config files and process groups. | Document as the current model; later, support multiple join configs per process (e.g. by `config_id` in message or topic routing). |
| **No guidance on partition count** | Throughput and parallelism depend on input topic partitions; over-partitioning can increase state spread. | **Documented (P1.2):** ARCHITECTURE.md §6 Operations and capacity — partition count, Scylla/Redis, Bloom tuning. |
| **Shared Scylla/Redis** | All keys and timeouts share the same keyspace and Redis keys; no per-tenant or per-join isolation. | Acceptable for single-tenant or few joins; for multi-tenant, document that keyspace/Redis prefix could be made configurable per join. |

### 3.4 Security and configuration

| Gap | Impact | Recommendation |
|-----|--------|-----------------|
| **Secrets in config file** | Brokers, Redis address, and (if used) passwords are in JSON; no integration with a secret manager. | **Documented (P0.5):** See [§3.4.1 Config and secrets](#341-config-and-secrets) below. |
| **No auth for /metrics** | Metrics endpoint is unauthenticated; anyone with network access can scrape. | In production, put the process behind a reverse proxy or service mesh that enforces auth; or add optional basic auth / mTLS for the metrics server. |

#### 3.4.1 Config and secrets

Processor and join config files (JSON) can contain **sensitive data**:

- **Connection details:** `kafka_brokers`, `scylla_hosts`, `redis_addr` — these identify your infrastructure and may be considered sensitive in production.
- **Passwords:** If you add authentication (e.g. Redis password, Scylla credentials, SASL for Kafka), those values would live in config unless you use another mechanism.

**Do not commit production config files that contain secrets** to version control. Use example or template configs with placeholders (e.g. `config/processor_example.json`) and keep real config outside the repo or in a private store.

**File permissions:** Restrict read access to the process user only. For config files that contain secrets or production endpoints, set mode **`0600`** (owner read/write only), e.g.:

```bash
chmod 0600 config/processor.json
```

**For production:**

- **Environment variables:** Prefer injecting secrets via the environment (e.g. `REDIS_PASSWORD`, `KAFKA_SASL_PASSWORD`) and have the processor read them — today the app reads config from JSON only, so you can use a wrapper script or orchestration to substitute env vars into the config file before starting the process, or add optional env support in a future release.
- **Secret manager:** Mount secrets from your platform (e.g. Kubernetes Secrets, Vault) as files with restricted permissions, or populate env vars from the manager at startup.
- **Separate files:** Keep join config (often non-secret) in repo and processor config (with brokers and addresses) generated or mounted per environment.

Operators should know: **where config files live**, **not to commit production config**, and **how to restrict access** (permissions and, where needed, env or secret manager).

### 3.5 Resilience

| Gap | Impact | Recommendation |
|-----|--------|-----------------|
| **Commit failure stops consumer** | On `CommitRecords` failure, `Run` returns and the process exits (or restarts). Reprocessing of the last batch is expected. | **Done (P1.3):** Consumer retries commit up to 3 times with backoff before exiting; each retry is logged. |
| **No backpressure on timeout burst** | If many keys time out in the same window, the timeout manager can publish and delete in a burst. | Acceptable for moderate TTL and key count; document; if needed, add rate limiting or batching for partial egress. |
| **Single process** | No in-process HA; availability is “restart + rebalance”. | Rely on consumer group rebalance and fast restart; document that at-least-once implies possible duplicate egress after failover. |

---

## 4. Production Checklist (Summary)

Use this as a quick checklist before treating the system as production-grade.

- [ ] **Downstream is idempotent** — Egress (and corrections) are at-least-once; consumers must upsert by join key.
- [ ] **Kafka key = join key** — Producers set message key so that ordering and partitioning are correct ([ORDERING.md](ORDERING.md)).
- [ ] **Config and secrets** — Join and processor config validated at startup (P0.3). Do not commit production config; restrict access (e.g. `chmod 0600`); use env or secret manager for passwords. See [§3.4.1 Config and secrets](PRODUCTION_READINESS.md#341-config-and-secrets).
- [x] **Observability** — Metrics scraped (e.g. Prometheus); dashboards for join rate, latency, and errors; alert on `handle_errors_total`, `commit_errors_total`, `egress_failures_total` (see [TESTING.md](TESTING.md)).
- [x] **Health/readiness** — `GET /health` (liveness) and `GET /ready` (readiness) are implemented; use them in your orchestrator (see [TESTING.md](TESTING.md)).
- [ ] **Delay queue** — If `post_join_delay` is used, see [§3.2.1 Delay queue limitation](PRODUCTION_READINESS.md#321-delay-queue-in-memory-limitation): pending delayed items are lost on restart; use short delay or accept best-effort, or plan for durable store (P2.1).
- [ ] **Capacity** — Partition count, Scylla/Redis sizing, and Bloom parameters tuned for key cardinality and throughput ([ARCHITECTURE.md](ARCHITECTURE.md)).

---

## 5. Conclusion

As a **system architect**, I view Omni-Joiner as a **well-structured, production-capable stream join engine** for the right scope: single join config, at-least-once semantics, idempotent downstream, and clear documentation. The codebase follows production-oriented practices (error handling, no silent drops, metrics, context propagation). To be **fully production-grade** in a demanding environment, prioritize: **health/readiness endpoints**, **structured logging**, **failure metrics**, **config validation**, and **documented limits** (delay queue, one config per process, partition/capacity guidance). With those in place, the system is suitable for production use within the documented constraints.
