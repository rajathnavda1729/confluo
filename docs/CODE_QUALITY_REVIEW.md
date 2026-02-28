# Code Quality Review (Post-Fix Pass)

System-architect review after Kafka/Redpanda, Scylla, and franz-go migration fixes. Assesses alignment with [docs/DEVELOPMENT.md](DEVELOPMENT.md) and [.cursor/rules/go-standards.mdc](../.cursor/rules/go-standards.mdc).

---

## 1. Standards Checklist

| Criterion | Status | Notes |
|-----------|--------|--------|
| Error handling: no silent ignores | Addressed | Intentional ignores documented or replaced with log/return; critical paths fixed. |
| Error wrapping with `%w` | Addressed | Kafka produce, config load, and key call sites wrap with context. |
| Context as first arg, honor cancellation | OK | Consumer, store, timeout, delay all take `context.Context` and respect `ctx.Done()`. |
| Cleanup via defer / stop goroutines on cancel | OK | `defer st.Close()`, `defer kclient.Close()`, delay/timeout loops exit on `ctx.Done()`. |
| Interfaces for coupling reduction | OK | `Producer`, `BloomChecker`, `TimeoutScheduler`, `DelayPublisher`, `LateArrivalTracker` used correctly. |
| Table-driven tests, integration skips | OK | Per DEVELOPMENT.md; integration tests skip when services unavailable. |
| No hardcoded secrets / production-ready | OK | Ports and keys come from config. |

---

## 2. Findings and Fixes

### 2.1 Critical: Commit and Handle errors in consumer loop

**Issue:** In `consumer.Run`, when `handler.Handle` failed we continued without committing (correct) but did not log, so operators had no visibility. When `Handle` succeeded, `CommitRecords` error was ignored, risking double processing or commit loss.

**Fix:** Log handle failures with topic/partition/offset/key; on commit failure log and return so the caller can back off or exit (at-least-once semantics preserved).

### 2.2 Critical: Delay queue — delete only after successful produce

**Issue:** In `delay/queue.go`, `flushDue` called `ProduceSync` then `DeleteState` and ignored both errors. If produce failed, state was still deleted, so the joined result could be lost.

**Fix:** Only call `DeleteState` after a successful `ProduceSync`; log produce failure and leave the item for a future flush (or retry in place).

### 2.3 Critical: Timeout manager — delete only after successful partial egress

**Issue:** In `timeout/manager.go`, after publishing partial egress we always called `DeleteState` and `ZRem`; publish errors were ignored. Same class as delay queue: state removed even when egress failed.

**Fix:** Only delete state and remove from the ZSET after successful publish; on publish failure log and leave the key in the set for the next poll (or add retry policy later).

### 2.4 Intentional error ignores (documented per go-standards)

These remain ignored by design; comments added so they are explicit:

- **consumer.Handle:** Timeout schedule, late-tracker Contains/Marshal/publish/remove/delete on correction path — best-effort to avoid blocking the main path; failures logged where feasible.
- **main.go:** `srv.ListenAndServe()` — metrics server best-effort; shutdown via `defer srv.Shutdown`.
- **kafka.Client.Close / store.Close:** Best-effort cleanup; no return value used by caller.

### 2.5 Error wrapping and small improvements

- **kafka.Client.ProduceSync:** Wrap `FirstErr()` with `fmt.Errorf("produce %s: %w", topic, err)`.
- **config.LoadJoinConfig:** Wrap file/unmarshal errors with path: `fmt.Errorf("load join config from %s: %w", path, err)`.
- **store.GetState:** `uuid.FromBytes` error: document that zero UUID is used on error, or return error (chose to document and keep zero UUID for backward compatibility).

---

## 3. Possible Future Optimizations

- **Structured logging:** Introduce a small logger interface (e.g. `Logger` with `Warn/Error`) and use it in consumer, delay, timeout instead of `log.Printf`, to allow JSON logs and levels in production.
- **Metrics for failures:** Add Prometheus counters for handle errors, commit errors, produce failures in delay/timeout, so SLOs and alerting can be built.
- **Retry policy for produce:** In delay queue and timeout manager, consider a bounded retry (e.g. 3x with backoff) before giving up and leaving state for next cycle.
- **Consumer commit strategy:** Document at-least-once semantics and that commit-after-handle implies possible reprocessing on crash; consider offset management notes in ORDERING.md.

---

## 4. Summary

Post-fix changes preserve behavior while aligning with project rules: **no silent critical-path failures**, **delete state only after successful egress**, and **explicit documentation** where errors are intentionally ignored. The codebase remains suitable for production use with the current scope; the items in §3 can be scheduled as follow-ups.
