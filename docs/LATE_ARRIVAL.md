# Late Arrival and Correction Events

## Overview

When **partial egress** is enabled, some join keys may be published with only a subset of streams (e.g. Stream A arrived, Stream B never did). If Stream B’s event **arrives later** (after the partial was already sent), the processor can emit a **correction event** so downstream systems can update their view.

## Egress schema for idempotent updates

All egress payloads (main topic and corrections) should be treated as **upserts** keyed by join key:

- Include **join key** (and optionally **timestamp** or **version**) in every published document.
- Downstream stores (e.g. KV or table) should use the join key as the primary key and overwrite on new events, so that:
  - A later **full** join overwrites an earlier **partial** for the same key.
  - A **correction** (late arrival) overwrites the previous partial for that key.

Example projected document:

```json
{
  "order_id": "o1",
  "price": 99.99,
  "status": "shipped",
  "timestamp": "2024-01-15T10:00:00Z",
  "_key": "o1",
  "_ts": 1705312800
}
```

Downstream: `INSERT OR REPLACE` by `_key` (or by `order_id` if that is the join key).

## Corrections topic (optional)

When `corrections_topic` is set in the processor config:

1. **Partial egress:** When the timeout manager publishes a partial result, it records the join key in a Redis set (`omni_joiner:partial`).
2. **Late arrival:** When the processor sees an event for a key that is already in that set (state exists but still incomplete), it treats this as late arrival:
   - Builds the projected payload from current state (including the new participant).
   - Publishes a **correction event** to the corrections topic with `kind: "late"`, `join_key`, `timestamp`, and `payload`.
   - Removes the key from the partial set and deletes join state so the key does not time out again.

Downstream consumers of the corrections topic can merge/upsert by `join_key` into the same store as the main egress.

## Correction event shape

```json
{
  "kind": "late",
  "join_key": "o1",
  "timestamp": "2024-01-15T10:05:00Z",
  "payload": { "order_id": "o1", "price": 99.99, "status": "shipped", "timestamp": "..." }
}
```

Use `join_key` (and optionally `timestamp`) for idempotent upserts.
