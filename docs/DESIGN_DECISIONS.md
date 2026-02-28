# Design Decisions

Key design decisions for the Omni-Joiner platform are recorded here with **date** and **rationale** so future work and onboarding are not blocked by lost context.

---

## Format

Each entry should include:

- **Title** — Short name for the decision.
- **Date** — When the decision was made (YYYY-MM-DD).
- **Status** — e.g. Accepted | Superseded | Deferred.
- **Context** — What problem or option was being considered.
- **Decision** — What was chosen.
- **Rationale** — Why this was chosen over alternatives.
- **Consequences** — Notable trade-offs, follow-ups, or docs to update.

---

## Decisions

### D1: Language — Go (not Rust)

- **Date:** 2025-02 (initial implementation)
- **Status:** Accepted
- **Context:** Design doc mentioned "Golang/Rust" for the processing engine.
- **Decision:** Use **Go** for the entire processor (cmd + internal packages).
- **Rationale:** Single language simplifies tooling, debugging, and onboarding; Go’s concurrency and stdlib are sufficient for the target latency and throughput; design table already favored Golang.
- **Consequences:** Hot path is Go; if profiling later shows a bottleneck, consider Rust only for that component with a clear boundary (e.g. FFI or sidecar).

---

### D2: Timeout handling — Timer queue (Redis ZSET) instead of ScyllaDB CDC

- **Date:** 2025-02 (Phase 2 implementation)
- **Status:** Accepted
- **Context:** Design suggested "CDC listener on the State Store" or a "timeout topic / timer queue" for partial egress.
- **Decision:** Use a **Redis sorted set** keyed by `(join_key_hash, deadline)`. A goroutine polls for due keys, runs partial egress (when config allows), then deletes state and removes from the set.
- **Rationale:** Avoids ScyllaDB CDC setup and version constraints; Redis was already in the stack for Bloom; polling every few seconds is acceptable for TTL-based timeouts; same process can own both consumer and timeout loop.
- **Consequences:** Timeout resolution is on the order of the poll interval (e.g. 5s). For sub-second timeout accuracy, a dedicated delay queue or topic would be needed later.

---

### D3: Config storage — File-based join config

- **Date:** 2025-02 (Phase 0)
- **Status:** Accepted
- **Context:** Where should `config_id` and per-join config (N, TTL, projection, egress) live?
- **Decision:** **File-based** JSON: `ProcessorConfig` points to `config_path` (e.g. `config/join_example.json`) or embeds `JoinConfig` inline. No DB or config service in initial scope.
- **Rationale:** Simplest to run and test; no extra infra; easy to version in git. Sufficient for single-tenant or few-join deployments.
- **Consequences:** To support many join configs or dynamic updates, we’d add a config store (DB or service) and reference by `config_id` later.

---

### D4: First-arrival optimization — Bloom "not present" skips read-back

- **Date:** 2025-02 (Phase 2)
- **Status:** Accepted
- **Context:** When Bloom says key is "definitely not present", how to avoid unnecessary DB work?
- **Decision:** On **first arrival** (Bloom says not present): call **UpsertParticipantOnly** (update only, no read-back), then add key to Bloom. Do **not** call GetState, since we know only one participant exists.
- **Rationale:** Saves one ScyllaDB read per first-arrival event; preserves correctness because we only skip the read when the key is new.
- **Consequences:** Store must expose `UpsertParticipantOnly`; handler has two paths (first arrival vs "maybe exists").

---

### D5: Egress schema for idempotent updates — Key + optional timestamp in payload

- **Date:** 2025-02 (Phase 3)
- **Status:** Accepted
- **Context:** Downstream must support late arrivals and corrections (upsert semantics).
- **Decision:** Egress payloads (main and corrections topic) are designed for **upsert by join key**: include join key (and optionally timestamp/version) in the document so downstream can do idempotent replace. Corrections topic uses a wrapper with `kind`, `join_key`, `timestamp`, `payload`.
- **Rationale:** Keeps the processor agnostic of downstream storage; any KV or table store can implement upsert by key; correction events can overwrite earlier partials for the same key.
- **Consequences:** Documented in [LATE_ARRIVAL.md](LATE_ARRIVAL.md); producers of the input topic should set Kafka key = join key for ordering.

---

### D6: Strict per-key ordering — Producer responsibility (Kafka key = join key)

- **Date:** 2025-02 (Phase 3)
- **Status:** Accepted
- **Context:** How to get per-key FIFO processing?
- **Decision:** **No processor code change.** Require that **producers** of the join-input topic set the **Kafka message key** to the join key (or its hash) so all events for the same key land in the same partition. Consumer group processes partitions in order.
- **Rationale:** Standard Kafka pattern; no need for custom partitioning inside the processor; documented in [ORDERING.md](ORDERING.md).
- **Consequences:** Operators and producers must follow the contract; we provide `keys.HashToPartition` for custom producer logic if needed.

---

## Adding New Decisions

When making a change that affects behavior, APIs, or operations:

1. Add an entry above (or append a new section) with the format above.
2. Set **Status** (e.g. Accepted | Superseded | Deferred).
3. Link any new or updated docs in **Consequences**.
