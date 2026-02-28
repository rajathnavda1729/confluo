# Ordering and Partitioning

## Strict per-key ordering

For **per-key FIFO** semantics (events for the same join key processed in order):

1. **Produce input events with the join key as the Kafka message key.**  
   Use the same key format the processor uses (e.g. `join_key_hash` or the raw key string).  
   Example: when producing to the join-input topic, set `key = join_key_hash` (or the serialized composite key).

2. **Partition count.**  
   Use enough partitions for parallelism; keys hash to partitions via Kafka’s default partitioner (or use `keys.HashToPartition(joinKeyHash, numPartitions)` for custom logic).

3. **Single consumer per partition.**  
   The processor uses a consumer group. Each partition is assigned to one consumer; within a partition, messages are processed in offset order, so per-key order is preserved when keys are consistent.

No code changes are required in the processor; ordering is guaranteed by producing with the correct key.
