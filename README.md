# Confluo / Omni-Joiner

A high-performance, distributed **N-way stream joining platform** designed for sub-10ms temporal joins across heterogeneous data sources.

## Prerequisites

- Go 1.22+
- Docker and Docker Compose

## Running the stack locally

1. **Start infrastructure** (Redpanda, ScyllaDB, Redis with RedisBloom):

   ```bash
   docker compose up -d
   ```

2. **Wait for services** to be healthy (especially ScyllaDB, which can take ~30s):

   ```bash
   docker compose ps
   ```

3. **Create ScyllaDB keyspace and table** (after Phase 1 schema is defined, or run the processor once with migrations).

4. **Run the processor** (with config that points at localhost):

   ```bash
   go run ./cmd/processor -config config/processor.json
   ```

### Service endpoints

| Service   | Port(s)        | Purpose                          |
|----------|----------------|----------------------------------|
| Redpanda | 19092 (Kafka)  | Input and egress topics          |
| ScyllaDB | 9042 (CQL)     | Join state store                 |
| Redis    | 6379, 8001     | Bloom filter (Redis Stack)      |

### Configuration

- **Processor:** `config/processor.json` — brokers, topics, Scylla/Redis addresses.
- **Join definition:** `config/join_example.json` or path in `config_path` — streams, key, TTL, projection, egress policy.

## Build

```bash
go build -o bin/processor ./cmd/processor
```

## Test

- **Unit tests (no external services):** `go test -short ./...`
- **All tests (Redis/Scylla tests skip if services unavailable):** `go test ./...`
- **Integration test (requires ScyllaDB):** `go test -v ./tests/integration/...`

For **running the application** and **configuration files** for testing join functionality (inner, partial, delay, 3-way, composite key), see [docs/TESTING.md](docs/TESTING.md).

For the full test checklist and scenario tests, see [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md).

## Project layout

- `cmd/processor` — main stream join processor binary
- `internal/config` — join and processor config types
- `internal/keys` — composite key hashing for join keys
- `internal/store` — ScyllaDB state store (Phase 1)
- `internal/engine` — join completion and projection (Phase 1)
- `internal/projection` — projection engine
- `config/` — example JSON configs

## Design

See [Omni-Joiner_ Stream Joining Platform Design.md](Omni-Joiner_%20Stream%20Joining%20Platform%20Design.md) for requirements, architecture, and phased execution plan.

**Architecture (components, flow, join types):** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — role of Redpanda, ScyllaDB, Redis; flow and sequence diagrams (Mermaid); inner, partial, delay, composite-key joins with examples.

**Production readiness (system-architect review):** [docs/PRODUCTION_READINESS.md](docs/PRODUCTION_READINESS.md) — verdict, strengths, gaps, and checklist for production use.

**Implementation plan (from review):** [docs/PRODUCTION_READINESS_PLAN.md](docs/PRODUCTION_READINESS_PLAN.md) — P0/P1/P2 work items; Phase 4 checklist in [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md).

**Development process:** [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) — phase-wise execution, checklist, test requirements, coding standards.  
**Design decisions:** [docs/DESIGN_DECISIONS.md](docs/DESIGN_DECISIONS.md) — key choices and rationale.

For **strict per-key ordering**, see [docs/ORDERING.md](docs/ORDERING.md).

For **late arrival** and **correction events** (partial egress + idempotent upserts), see [docs/LATE_ARRIVAL.md](docs/LATE_ARRIVAL.md).
