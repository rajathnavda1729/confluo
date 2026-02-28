-- Join state table for Omni-Joiner.
-- Run this in cqlsh after creating the keyspace, or use store.CreateKeyspaceAndTable.

CREATE KEYSPACE IF NOT EXISTS omni_joiner
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS omni_joiner.join_state (
    join_key_hash blob,
    join_key_raw text,
    config_id uuid,
    participant_data map<text, blob>,
    arrival_timestamps map<text, timestamp>,
    is_completed boolean,
    PRIMARY KEY (join_key_hash)
) WITH default_time_to_live = 600
  AND compaction = {'class': 'LeveledCompactionStrategy'};
