# Kafka / Redpanda compatibility

## Summary

- **Redpanda** recommends the **franz-go** (twmb/franz-go) Go client. It is the validated client and auto-negotiates Kafka API versions with the broker.
- **Sarama** (IBM/sarama) is **not** in Redpanda’s validated client list. Recent Redpanda versions (v23.3 and v24.x) reject the Metadata API versions that Sarama uses (v9 and v10), which leads to “Unsupported version X for metadata API” and connection failures.
- This project uses **franz-go** so it works with **current Redpanda (v24.x)** in Docker without version pinning.

## Docker

- **Redpanda image:** `redpandadata/redpanda:v24.3.5` (or any current v24).
- **Single listener:** Kafka is exposed on port `19092`; advertised address is `redpanda:19092` so the processor in Docker can connect. For host runs, add `127.0.0.1 redpanda` to `/etc/hosts` if you use `127.0.0.1:19092`.
- No need to downgrade Redpanda or clear data when using franz-go.

## Version history

- We previously used Sarama and Redpanda v23.3 to avoid Metadata API errors; v23.3 also rejected Metadata v9 in practice.
- Switching to franz-go allows using the latest Redpanda and removes Kafka API version as a source of failures.
