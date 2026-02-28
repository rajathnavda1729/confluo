# Join verification guide

This guide helps you verify each supported join type locally. Use it after starting the stack (Redpanda, ScyllaDB, Redis) and the processor.

**Prerequisites:**

- Docker Compose up: `docker compose up -d` (or run processor on host with Scylla/Redis/Redpanda reachable).
- Topics created: `join-input`, `join-output`, and for partial egress `join-corrections`.
- Processor config points to the join config you want to test (e.g. `config_path` in processor JSON).

**Create topics (once):**

```bash
docker compose exec redpanda rpk topic create join-input --brokers localhost:19092
docker compose exec redpanda rpk topic create join-output --brokers localhost:19092
docker compose exec redpanda rpk topic create join-corrections --brokers localhost:19092
```

**Consume output (in a separate terminal):**

```bash
docker compose exec -T redpanda rpk topic consume join-output --brokers localhost:19092 -n 10
```

---

## 1. Inner join (2-way, single key)

**Config:** `config/join_inner.json`  
**Processor:** `config/processor_local.json` (or set `config_path` to `config/join_inner.json`).

**Streams:** `orders`, `shipments`. **Key:** `order_id`. **Egress:** inner (emit only when both streams have an event for the key).

**Produce events:** Same key for both streams; message key = `order_id`.

```bash
# From repo root
BROKERS="${BROKERS:-localhost:19092}"
RPK="docker compose exec -T redpanda rpk topic produce join-input --brokers $BROKERS"

echo '{"order_id":"ord-1","price":99.99}' | $RPK -H "x-stream-id:orders" -k "ord-1"
echo '{"order_id":"ord-1","status":"shipped"}' | $RPK -H "x-stream-id:shipments" -k "ord-1"
```

**Expected on join-output:** One message with body like `{"order_id":"ord-1","price":99.99,"status":"shipped"}`.

**Quick test:** `./scripts/produce_join_events.sh` then consume `join-output`; you should see 2 joined records (ord-1, ord-2).

---

## 2. Inner join (3-way)

**Config:** `config/join_three_way.json`  
**Processor:** Set `config_path` to `config/join_three_way.json`.

**Streams:** `orders`, `shipments`, `invoices`. **Key:** `order_id`. **Egress:** inner.

**Produce events:** Send one event per stream for the same `order_id`; only when all three have arrived does the join emit.

```bash
RPK="docker compose exec -T redpanda rpk topic produce join-input --brokers ${BROKERS:-localhost:19092}"

echo '{"order_id":"ord-3","price":50}' | $RPK -H "x-stream-id:orders" -k "ord-3"
echo '{"order_id":"ord-3","status":"pending"}' | $RPK -H "x-stream-id:shipments" -k "ord-3"
echo '{"order_id":"ord-3","invoice_id":"inv-3"}' | $RPK -H "x-stream-id:invoices" -k "ord-3"
```

**Expected on join-output:** One message with `order_id`, `price`, `status`, `invoice_id` after the third event.

---

## 3. Partial egress (timeout-based)

**Config:** `config/join_partial.json`  
**Processor:** `config/processor_partial.json` (has `config_path` and `corrections_topic: join-corrections`). **Requires Redis.**

**Streams:** `orders`, `shipments`. **Key:** `order_id`. **Egress:** partial; **TTL:** 60s.

**Test A – Partial only:** Produce only `orders` for a key; wait ~60s; consume `join-output` for a partial record.

**Test B – Late arrival:** After partial, produce `shipments` for the same key; consume `join-corrections` for a correction event.

```bash
RPK="docker compose exec -T redpanda rpk topic produce join-input --brokers ${BROKERS:-localhost:19092}"
echo '{"order_id":"ord-partial","price":10}' | $RPK -H "x-stream-id:orders" -k "ord-partial"
# Wait 60s, check join-output; then:
echo '{"order_id":"ord-partial","status":"late"}' | $RPK -H "x-stream-id:shipments" -k "ord-partial"
docker compose exec -T redpanda rpk topic consume join-corrections --brokers localhost:19092 -n 5
```

---

## 4. Post-join delay

**Config:** `config/join_with_delay.json`  
**Processor:** Set `config_path` to `config/join_with_delay.json`. **Requires Redis for durable delay.**

**Streams:** `orders`, `shipments`. **Key:** `order_id`. **Delay:** 2s after join before publish.

**Produce events:** Same as inner 2-way. **Expected:** Joined record on `join-output` about **2 seconds** after the second event.

```bash
./scripts/produce_join_events.sh
# Wait 2+ seconds, then consume join-output
```

---

## 5. Composite key join

**Config:** `config/join_composite_key.json`  
**Processor:** Set `config_path` to `config/join_composite_key.json`.

**Streams:** `stream_a`, `stream_b`. **Key:** `tenant_id` + `order_id`. **Egress:** inner.

**Produce events:** Each message must contain both `tenant_id` and `order_id`; same composite key for both streams.

```bash
RPK="docker compose exec -T redpanda rpk topic produce join-input --brokers ${BROKERS:-localhost:19092}"
KEY="acme:o1"
echo '{"tenant_id":"acme","order_id":"o1","value":100}' | $RPK -H "x-stream-id:stream_a" -k "$KEY"
echo '{"tenant_id":"acme","order_id":"o1","extra":"done"}' | $RPK -H "x-stream-id:stream_b" -k "$KEY"
```

**Expected on join-output:** One message with projected fields from both streams.

---

## Summary table

| Join type        | Config file              | Processor config        | Notes                          |
|------------------|--------------------------|--------------------------|--------------------------------|
| Inner 2-way      | join_inner.json          | processor_local.json     | Key = order_id                 |
| Inner 3-way      | join_three_way.json      | config_path → that file  | orders + shipments + invoices |
| Partial egress   | join_partial.json        | processor_partial.json   | Redis + corrections topic     |
| Post-join delay  | join_with_delay.json     | config_path → that file  | 2s delay; Redis for durability |
| Composite key    | join_composite_key.json  | config_path → that file  | tenant_id + order_id           |

---

## Running the processor

**With Docker:** `docker compose up -d` then `docker compose up -d processor`.

**On host:** `go run ./cmd/processor -config config/processor_local.json`

To test another join type, set the processor config’s `config_path` to the desired join config and restart the processor.
