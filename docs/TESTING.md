## Testing Omni-Joiner Locally

This guide explains how to run the Omni-Joiner test suite on your machine: fast unit tests, full tests, and integration tests that exercise ScyllaDB and Redis.

---

### 1. Prerequisites

- **Go** 1.22+ installed and on your `PATH`.
- **Docker & Docker Compose** (recommended) to run:
  - **ScyllaDB** (CQL: `9042`)
  - **Redis Stack** (Redis + RedisBloom: `6379`)
  - **Redpanda** (Kafka: `19092`) – only needed for full end-to-end testing. We use **Redpanda v24** with the **franz-go** Go client (see [docs/KAFKA_REDPANDA.md](KAFKA_REDPANDA.md)).

From the repo root (`confluo`), start infra:

```bash
docker compose up -d
docker compose ps
```

---

### 2. Running the Application

**Option A: Run everything in Docker (recommended if you see Scylla/Kafka EOF from the host)**

Start infra and the processor in one network so the processor talks to Scylla, Redpanda, and Redis by hostname (no host port/DNS issues):

```bash
docker compose up -d
# Wait for redpanda, scylla, redis to be healthy, then:
docker compose up -d processor
# Or build and run in one go:
docker compose up -d --build
```

Create Kafka topics from the host (rpk inside the redpanda container):

```bash
docker compose exec redpanda rpk topic create join-input --brokers localhost:19092
docker compose exec redpanda rpk topic create join-output --brokers localhost:19092
```

The processor uses `config/processor_docker.json` (Kafka at `redpanda:19092`, Scylla at `scylla`, Redis at `redis`). Metrics: `http://localhost:9090/metrics`. **Health:** `GET http://localhost:9090/health` (liveness) and `GET http://localhost:9090/ready` (readiness: Scylla, Kafka, optional Redis).

**Option B: Run the processor on the host**

From the repo root:

```bash
# Build (optional)
go build -o bin/processor ./cmd/processor

# Run with default processor config (uses config/join_example.json)
go run ./cmd/processor -config config/processor.json

# Or run with a specific processor config
go run ./cmd/processor -config config/processor_local.json
go run ./cmd/processor -config config/processor_partial.json
```

The processor expects:

- **Kafka:** `input_topic` and `egress_topic` to exist (create them if needed).
- **ScyllaDB:** It will create the keyspace and `join_state` table on startup if missing.
- **Redis:** Optional; used for Bloom filter and timeout/partial tracking when `redis_addr` is set.

**Create Kafka topics (Redpanda):**

```bash
# Using rpk (install from Redpanda docs or use the container)
docker exec -it confluo-redpanda-1 rpk topic create join-input --brokers localhost:19092
docker exec -it confluo-redpanda-1 rpk topic create join-output --brokers localhost:19092
docker exec -it confluo-redpanda-1 rpk topic create join-corrections --brokers localhost:19092
```

If your Redpanda container name differs, use `docker compose ps` to get the service name, then:

```bash
docker compose exec redpanda rpk topic create join-input
docker compose exec redpanda rpk topic create join-output
docker compose exec redpanda rpk topic create join-corrections
```

**Producing test events**

Each message on `join-input` must include the header **`x-stream-id`** (e.g. `orders` or `shipments`) and a JSON body that contains the join key field (for `config/join_inner.json`, the key is **`order_id`**).

**One-off commands with rpk (Docker):**

From the repo root, with Redpanda and topics already created:

```bash
# Orders stream
echo '{"order_id":"ord-1","price":99.99}' | docker compose exec -T redpanda rpk topic produce join-input --brokers localhost:19092 -H "x-stream-id:orders" -k "ord-1"

# Shipments stream (same key to complete the join)
echo '{"order_id":"ord-1","status":"shipped"}' | docker compose exec -T redpanda rpk topic produce join-input --brokers localhost:19092 -H "x-stream-id:shipments" -k "ord-1"
```

Joined results appear on **`join-output`**. Consume them with:

```bash
docker compose exec -T redpanda rpk topic consume join-output --brokers localhost:19092 -n 10
```

**Script to push a full set of sample events:**

From the repo root:

```bash
./scripts/produce_join_events.sh
```

This sends two orders (`ord-1`, `ord-2`) with both `orders` and `shipments` events so the inner join emits two records on `join-output`. Optional: pass brokers as first argument, e.g. `./scripts/produce_join_events.sh localhost:19092`.

**Quick end-to-end check:**

```bash
# Terminal 1: start processor (Docker)
docker compose up -d && docker compose up -d processor

# Terminal 2: produce events, then consume output
./scripts/produce_join_events.sh
docker compose exec -T redpanda rpk topic consume join-output --brokers localhost:19092 -n 4
```

**Metrics:** When the processor is running, Prometheus metrics are exposed at `http://localhost:9090/metrics`. Counters include: `omni_joiner_join_success_total`, `omni_joiner_events_processed_total`, `omni_joiner_handle_errors_total`, `omni_joiner_commit_errors_total`, `omni_joiner_egress_failures_total` (labels: `config`, and for egress `phase`: main, delay, timeout, correction). Use these for alerting and SLOs. **Liveness:** `GET /health` or `GET /live` returns 200 when the process is up. **Readiness:** `GET /ready` returns 200 when Scylla, Kafka, and (if configured) Redis are reachable; otherwise 503 with a JSON body listing failures.

**Troubleshooting**

- **Redpanda "unsupported tx_snapshot_header version 6" (or similar)**  
  You switched Redpanda versions and on-disk state is incompatible. Remove Redpanda's data and start clean:
  ```bash
  docker compose stop redpanda processor
  docker compose rm -f redpanda
  docker volume rm confluo_redpanda_data 2>/dev/null || true
  docker compose up -d redpanda
  ```
  Wait for Redpanda to be healthy, then create topics again and start the processor.

- **"Unsupported version X for metadata API"**  
  The app uses **franz-go** (not Sarama) and works with Redpanda v24. Ensure you have rebuilt the processor (`docker compose build processor`) and are not using an old image.

- **"run out of available brokers" / "EOF"**
  The processor cannot reach Kafka/Redpanda. Try:
  1. Start infra: `docker compose up -d`
  2. Check Redpanda: `docker compose ps` (redpanda should be healthy)
  3. **Processor in Docker:** uses `redpanda:19092`; Redpanda advertises `redpanda:19092`, so it should connect. Rebuild and restart: `docker compose up -d --build processor`.
  4. **Processor on host:** use `127.0.0.1:19092` in config. Redpanda advertises `redpanda:19092`, so add `127.0.0.1 redpanda` to `/etc/hosts` so metadata resolves. Or run the processor in Docker (Option A).

- **Scylla "failed to connect ... EOF" from host**  
  The store uses CQL protocol version 4 and disables initial host lookup. If you still see EOF, run the processor in Docker (Option A) so it connects to `scylla:9042` on the same network.

- **"Keyspace 'omni_joiner' does not exist"**  
  This should no longer occur: the processor creates the keyspace and table before connecting. If you still see it, ensure ScyllaDB is running (`docker compose ps`) and that no other code path creates the store before `EnsureKeyspaceAndTable` runs.

---

### 3. Configuration Files for Join Testing

| File | Purpose |
|------|--------|
| **Processor configs** | |
| `config/processor.json` | Default: inner join, no corrections topic. |
| `config/processor_local.json` | Local run with inner join (`config/join_inner.json`). |
| `config/processor_docker.json` | For **Docker** run: Kafka at `redpanda:9092`, Scylla at `scylla`, Redis at `redis`. |
| `config/processor_partial.json` | Partial egress + corrections topic (`config/join_partial.json`). |
| **Join configs** | |
| `config/join_example.json` | 2-way inner join (orders + shipments), single key `order_id`. |
| `config/join_inner.json` | Same as above; explicit inner, 10m TTL. |
| `config/join_partial.json` | 2-way with **partial** egress; 60s TTL — use to test timeout/partial and late-arrival corrections. |
| `config/join_with_delay.json` | 2-way inner with **post_join_delay** 2s — use to test delayed publication. |
| `config/join_three_way.json` | **3-way** inner join (orders + shipments + invoices). |
| `config/join_composite_key.json` | 2-way join on **composite** key `tenant_id` + `order_id`. |

**Override join config without changing processor JSON:** Edit `config_path` in the processor config to point at any of the join configs above (e.g. `config/join_three_way.json`).

**Example: test partial egress**

1. Start infra and create topics (see above).
2. Run with partial config: `go run ./cmd/processor -config config/processor_partial.json`
3. Produce only one stream (e.g. `orders` with `order_id: ord-1`). Wait 60s; a partial result should appear on `join-output` and the key recorded for late-arrival. If you then produce the other stream (`shipments` for `ord-1`), a correction event should go to `join-corrections`.

---

### 4. Fast Feedback: Unit Tests Only

Run all unit tests in “short” mode:

```bash
go test -short ./...
```

Notes:

- This runs tests in all packages under `./...`.
- Integration-style tests that check `testing.Short()` will **skip** in this mode.
- Redis/Scylla-dependent tests also skip automatically if the services are unreachable.

Use this during development for quick feedback.

---

### 5. Full Suite: Unit + Integration-Style Tests

To run the full suite (unit + integration-style tests in `internal/*`), use:

```bash
go test ./...
```

Behavior:

- **Redis-backed tests** (Bloom filter, partial tracker, timeout manager):
  - Expect Redis at `localhost:6379`.
  - If Redis is not available, these tests **skip** with a clear message.
- **Scylla-backed tests** (store, timeout integration):
  - Expect ScyllaDB at `127.0.0.1:9042`.
  - If ScyllaDB is not available, they **skip** instead of failing hard.

This is a good “pre-commit” or “pre-merge” check.

---

### 6. Store Integration Test

To explicitly run the Scylla integration test only:

```bash
go test -v ./tests/integration/...
```

What it does:

- Uses `internal/store` against a real Scylla keyspace (`omni_joiner`).
- Verifies:
  - `CreateKeyspaceAndTable`
  - `UpsertParticipant` for two streams
  - `DeleteState` + `GetState` returns nil

If Scylla is not reachable, the test **skips** with a message.

---

### 7. What’s Covered by Tests

High-level coverage (see individual `*_test.go` files for details):

- **Keys** (`internal/keys`)  
  Composite key hashing, deterministic serialization, and partition mapping.

- **Config** (`internal/config`)  
  Duration JSON parsing, join config loading, `N()` for stream counts.

- **Engine & Projection** (`internal/engine`, `internal/projection`)  
  Join completion logic, projection of per-stream fields into the final document, `Process` behavior.

- **Delay queue** (`internal/delay`)  
  Scheduling, flushing due items, shutting down cleanly on context cancel.

- **Egress** (`internal/egress`)  
  Correction event JSON encoding and Redis-based partial tracker (Add/Contains/Remove).

- **Bloom** (`internal/bloom`)  
  RedisBloom-backed exists/add semantics.

- **Timeout manager** (`internal/timeout`)  
  Default configuration, scheduling timeouts into a Redis ZSET.

- **Consumer** (`internal/consumer`)  
  `extractKey` for single and composite keys; error cases and empty payload behavior; join-config `config_id` parsing.

- **Store** (`internal/store`, `tests/integration`)  
  Zero-value safety and full join-state lifecycle (upsert, read, delete) against ScyllaDB.

- **Metrics** (`internal/metrics`)  
  Basic registration and use of Prometheus counters/histograms.

---

### 8. Typical Local Workflow

1. **Start infra** (once per dev session, optional but recommended):

   ```bash
   docker compose up -d
   ```

2. **Run the application** (for join testing):

   ```bash
   go run ./cmd/processor -config config/processor_local.json
   ```

   Use `./scripts/produce_join_events.sh` to push sample events to `join-input`, then consume `join-output` to verify. Use `config/processor_partial.json` for partial-egress and corrections testing. See section 3 for all config options.

3. **While coding** (fast feedback):

   ```bash
   go test -short ./...
   ```

4. **Before pushing or merging**:

   ```bash
   go test ./...
   ```

5. **When touching store/Scylla behavior**:

   ```bash
   go test -v ./tests/integration/...
   ```

For more detail on which tests exist and what’s still pending (scenario tests, benchmarks), see the testing section in `docs/DEVELOPMENT.md`.

