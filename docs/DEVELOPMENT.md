# Omni-Joiner Development Process

This document defines how development is executed: **phase-wise**, with **tests**, **documentation**, and a **checklist** so we don't rely on context alone. Coding standards are production-grade; key design decisions are recorded in [DESIGN_DECISIONS.md](DESIGN_DECISIONS.md).

---

## 1. Execution Model

- **Phases are sequential.** Work only within the current phase from the [Omni-Joiner execution plan](../Omni-Joiner_%20Stream%20Joining%20Platform%20Design.md) (or the plan in `.cursor/plans/`). Do not implement a later phase until the current one is complete.
- **One step at a time.** For each step:
  1. Mark the step **in progress** in the checklist below.
  2. Implement the change.
  3. Add or update tests.
  4. Update documentation (comments, README, or docs under `docs/`).
  5. Mark the step **done** and add a short summary.

---

## 2. Execution Checklist

Use this checklist as the source of truth. Update it as steps are completed.

### Phase 0: Foundation and Infra


| Step | Item                                                                           | Status | Notes |
| ---- | ------------------------------------------------------------------------------ | ------ | ----- |
| 0.1  | Go module, repo layout (`cmd/`, `internal/`, `config/`)                        | Done   |       |
| 0.2  | Docker Compose (Redpanda, ScyllaDB, Redis+RedisBloom)                          | Done   |       |
| 0.3  | Config model (join config, processor config, key def, projection, TTL, egress) | Done   |       |
| 0.4  | Key hashing library (`internal/keys`: composite key, hash, partition)          | Done   |       |
| 0.5  | README: run stack, config, build                                               | Done   |       |


### Phase 1: Core Engine


| Step | Item                                                      | Status | Notes |
| ---- | --------------------------------------------------------- | ------ | ----- |
| 1.1  | ScyllaDB schema + atomic update + return state            | Done   |       |
| 1.2  | Consumer loop, completion check, projection, inner egress | Done   |       |
| 1.3  | Prometheus metrics, basic benchmarks                      | Done   |       |


### Phase 2: Performance Layer


| Step | Item                                                 | Status | Notes |
| ---- | ---------------------------------------------------- | ------ | ----- |
| 2.1  | RedisBloom "maybe exists" check, wire into processor | Done   |       |
| 2.2  | Timeout manager (timer queue), partial egress        | Done   |       |
| 2.3  | Post-join delay queue                                | Done   |       |


### Phase 3: Reliability and Ordering


| Step | Item                                                                     | Status | Notes |
| ---- | ------------------------------------------------------------------------ | ------ | ----- |
| 3.1  | Partition-by-key consumption, strict ordering (docs + producer guidance) | Done   |       |
| 3.2  | Late arrival / correction events and docs                                | Done   |       |


### Phase 4: Production Readiness (from system-architect review)

Driven by [docs/PRODUCTION_READINESS.md](PRODUCTION_READINESS.md) and [docs/PRODUCTION_READINESS_PLAN.md](PRODUCTION_READINESS_PLAN.md). Complete **P0** before production; **P1** soon after; **P2** as needed.

| Step | Item                                                                 | Status   | Notes |
| ---- | -------------------------------------------------------------------- | -------- | ----- |
| P0.1 | Health and readiness HTTP endpoints (`/health`, `/ready`)            | Done    | §3.1  |
| P0.2 | Failure metrics (handle, commit, egress errors)                       | Done    | §3.1  |
| P0.3 | Join config validation at startup                                    | Done    | §3.2; ValidateJoinConfig in internal/config; tests in config_test.go |
| P0.4 | Document delay queue in-memory limitation                            | Pending  | §3.2  |
| P0.5 | Document config and secrets (file permissions, env/secrets)          | Pending  | §3.4  |
| P1.1 | Structured logging (levels, optional JSON)                           | Pending  | §3.1  |
| P1.2 | Capacity and partition guidance (ops doc)                            | Pending  | §3.3  |
| P1.3 | Commit failure retry (bounded) before exit                           | Pending  | §3.5  |
| P1.4 | Bloom false-positive operational note (and optional metric)         | Pending  | §3.2  |
| P2.1 | Durable delay queue (Redis or topic)                                 | Pending  | §3.2  |
| P2.2 | Multiple join configs per process                                    | Pending  | §3.3  |
| P2.3 | Optional auth for /metrics                                           | Pending  | §3.4  |
| P2.4 | Timeout manager backpressure / rate limiting                         | Pending  | §3.5  |


### Testing and Evaluation (ongoing)


| Item                                                                                   | Status  | Notes                                                                                                 |
| -------------------------------------------------------------------------------------- | ------- | ----------------------------------------------------------------------------------------------------- |
| Unit tests: key hashing, projection, config                                            | Done    | `internal/keys/hash_test.go`, `internal/projection/project_test.go`, `internal/config/config_test.go` |
| Unit tests: engine (completion, projection, process)                                   | Done    | `internal/engine/join_test.go`                                                                        |
| Unit tests: delay queue, egress (correction, partial tracker), timeout, metrics, bloom | Done    | `*_test.go` in each package                                                                           |
| Unit tests: consumer (extractKey)                                                      | Done    | `internal/consumer/consumer_test.go`                                                                  |
| Unit tests: store (zero value; integration for real DB)                                | Done    | `internal/store/scylla_test.go` (integration skips if no Scylla)                                      |
| Integration: store join flow                                                           | Done    | `tests/integration/integration_test.go` (run without `-short`)                                        |
| Scenario: Slow stream                                                                  | Pending |                                                                                                       |
| Scenario: Orphan (partial egress)                                                      | Pending |                                                                                                       |
| Scenario: Race (atomic update)                                                         | Pending |                                                                                                       |
| Benchmarks: 50k/s, latency, Bloom FP rate                                              | Pending |                                                                                                       |


**Running tests:** `go test ./...` runs all unit tests (integration tests skip if services unavailable or use `-short`). Full integration: `go test -v ./tests/integration/...` (requires ScyllaDB). Redis-backed tests (bloom, timeout, egress tracker) skip if Redis is not at `localhost:6379`.

---

## 3. Test Requirements

### Per step

- **New or changed packages** must have at least one `*_test.go` file.
- **Pure logic** (hashing, projection, config parsing): **unit tests** with table-driven cases where appropriate.
- **External systems** (ScyllaDB, Kafka, Redis): **integration tests** or tests with mocks; document how to run (e.g. `-tags=integration`, or Docker required).

### Scenario tests (from design)

- **Slow stream:** Stream A at T=0, Stream B at T=9min → join completes before TTL.
- **Orphan:** Only Stream A; Stream B never arrives → partial egress at TTL (Phase 2).
- **Race:** Same key, two streams on two workers → single joined event, no duplicate or lost participant (atomic update).

### Quality bar

- No merge of step code without tests for the new behavior.
- Use `go test ./...` (and integration tags if applicable) before marking a step done.

---

## 4. Documentation Requirements

### As you go

- **Public APIs:** Doc comments on exported types and functions (what they do, parameters, return values, errors).
- **Config:** Any new or changed config fields must be described in README or in a config example/comments.
- **Behavior changes:** Update the relevant doc (e.g. [ORDERING.md](ORDERING.md), [LATE_ARRIVAL.md](LATE_ARRIVAL.md)) or README.

### To avoid context burden

- Prefer **short, focused docs** (one concern per file) and link from README.
- **Design decisions** that affect behavior, APIs, or operations go in [DESIGN_DECISIONS.md](DESIGN_DECISIONS.md) with date and rationale.
- **Code quality:** After major fixes or migrations, a system-architect review is captured in [CODE_QUALITY_REVIEW.md](CODE_QUALITY_REVIEW.md); it aligns with the Go and Omni-Joiner rules above.

---

## 5. Coding Standards

- **Production standard:** No TODOs that skip error handling; no silent error ignores unless documented; no hardcoded secrets or non-default ports in library code.
- **Go:** Follow [.cursor/rules/go-standards.mdc](../.cursor/rules/go-standards.mdc): formatting, error wrapping, interfaces, context, cleanup.
- **Naming:** Match existing project style (e.g. `JoinConfig`, `UpsertParticipant`, `EgressTopic`).

### 5.5 Linting

We use **[golangci-lint](https://golangci-lint.run/)** to enforce structure and catch common issues. Config: [.golangci.yml](../.golangci.yml) at repo root.

**Why it wasn’t used earlier:** The project relied on Cursor rules, manual review, and tests first; no CI or automated lint gate was in place. Linting is now added so style and error-handling rules are enforced automatically.

**How to run:**

```bash
make lint
```

No install needed: the Makefile runs the linter via `go run`, so the first run may download golangci-lint. For faster repeated runs you can run `make lint-install` and ensure `$(go env GOPATH)/bin` is on your `PATH`.

To auto-fix import order and formatting (goimports) then run the linter:

```bash
make lint-fix
```

Enabled linters align with the Go standards: **goimports** (import order), **errcheck** (no unchecked errors; use `//nolint:errcheck` with a short comment for intentional ignores), **errorlint** (prefer `%w`), **contextcheck** (context usage), **staticcheck** / **gosimple** / **unused**. Fix any new issues before merging, or add a justified `//nolint` where the rule doesn’t apply.

---

## 6. Where Things Live


| Concern                           | Location                                                                                                                      |
| --------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| Execution plan                    | [Omni-Joiner_ Stream Joining Platform Design.md](../Omni-Joiner_%20Stream%20Joining%20Platform%20Design.md), `.cursor/plans/` |
| Development process & checklist   | This file (`docs/DEVELOPMENT.md`)                                                                                             |
| Design decisions                  | [docs/DESIGN_DECISIONS.md](DESIGN_DECISIONS.md)                                                                               |
| Ordering / partitioning           | [docs/ORDERING.md](ORDERING.md)                                                                                               |
| Late arrival / corrections        | [docs/LATE_ARRIVAL.md](LATE_ARRIVAL.md)                                                                                       |
| Cursor rules (phase, tests, docs) | [.cursor/rules/](../.cursor/rules/)                                                                                           |
| Linting (golangci-lint)          | [.golangci.yml](../.golangci.yml); run `make lint` — see §5.5 above.                                                         |
| Production readiness review      | [docs/PRODUCTION_READINESS.md](PRODUCTION_READINESS.md) — verdict, gaps, checklist.                                          |
| Production readiness plan        | [docs/PRODUCTION_READINESS_PLAN.md](PRODUCTION_READINESS_PLAN.md) — P0/P1/P2 implementation plan from the review.            |


