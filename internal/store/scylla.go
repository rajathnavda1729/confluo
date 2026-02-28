package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
)

const (
	keyspace = "omni_joiner"
	table    = "join_state"
)

// JoinState represents the current state of a join group (after read).
type JoinState struct {
	JoinKeyHash       []byte
	JoinKeyRaw        string
	ConfigID          uuid.UUID
	ParticipantData   map[string][]byte
	ArrivalTimestamps map[string]time.Time
	IsCompleted       bool
}

// Config configures the ScyllaDB store.
type Config struct {
	Hosts    []string
	Keyspace string
	Timeout  time.Duration
}

// Store provides ScyllaDB-backed join state with atomic map updates.
type Store struct {
	session *gocql.Session
	cfg     Config
}

// New creates a new Store and establishes a session.
func New(cfg Config) (*Store, error) {
	if cfg.Keyspace == "" {
		cfg.Keyspace = keyspace
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	cluster := gocql.NewCluster(cfg.Hosts...)
	cluster.Keyspace = cfg.Keyspace
	cluster.Consistency = gocql.LocalQuorum
	cluster.Timeout = cfg.Timeout
	// Pin CQL protocol and avoid host discovery so host-only clients (e.g. 127.0.0.1) work with Scylla in Docker.
	cluster.ProtoVersion = 4
	cluster.DisableInitialHostLookup = true
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("create session: %w", err)
	}
	return &Store{session: session, cfg: cfg}, nil
}

// EnsureKeyspaceAndTable creates the keyspace and join_state table if they do not exist.
// Call this before New() so the keyspace exists when the session is created.
func EnsureKeyspaceAndTable(ctx context.Context, cfg Config) error {
	if cfg.Keyspace == "" {
		cfg.Keyspace = keyspace
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	// Create keyspace via system session
	cluster := gocql.NewCluster(cfg.Hosts...)
	cluster.Timeout = cfg.Timeout
	cluster.Keyspace = "system"
	cluster.ProtoVersion = 4
	cluster.DisableInitialHostLookup = true
	sysSession, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("create system session: %w", err)
	}
	defer sysSession.Close()

	q := sysSession.Query(
		`CREATE KEYSPACE IF NOT EXISTS ` + cfg.Keyspace + ` WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`,
	).WithContext(ctx)
	if err := q.Exec(); err != nil {
		return fmt.Errorf("create keyspace: %w", err)
	}

	// Create table via a temporary session to the new keyspace
	cluster.Keyspace = cfg.Keyspace
	appSession, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("create app session: %w", err)
	}
	defer appSession.Close()

	createTable := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.join_state (
		    join_key_hash blob,
		    join_key_raw text,
		    config_id uuid,
		    participant_data map<text, blob>,
		    arrival_timestamps map<text, timestamp>,
		    is_completed boolean,
		    PRIMARY KEY (join_key_hash)
		) WITH default_time_to_live = 600
		  AND compaction = {'class': 'LeveledCompactionStrategy'}
	`, cfg.Keyspace)
	if err := appSession.Query(createTable).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	return nil
}

// Close closes the session. Best-effort cleanup.
func (s *Store) Close() {
	s.session.Close()
}

// CreateKeyspaceAndTable creates the keyspace and join_state table if they do not exist.
// Deprecated: use EnsureKeyspaceAndTable before New() instead, so the keyspace exists when the session is created.
func (s *Store) CreateKeyspaceAndTable(ctx context.Context) error {
	return EnsureKeyspaceAndTable(ctx, s.cfg)
}

// UpsertParticipant atomically adds or updates one stream's data for the join key.
// It returns the full state after the update so the caller can check if join is complete (len(ParticipantData) == N).
func (s *Store) UpsertParticipant(ctx context.Context, joinKeyHash []byte, joinKeyRaw string, configID uuid.UUID, streamID string, payload []byte) (*JoinState, error) {
	now := time.Now()
	gocqlUUID := gocql.UUID(configID)
	// CQL map index: participant_data[streamID] = payload, arrival_timestamps[streamID] = now
	query := fmt.Sprintf(
		`UPDATE %s.join_state SET
			join_key_raw = ?,
			config_id = ?,
			participant_data[?] = ?,
			arrival_timestamps[?] = ?
		WHERE join_key_hash = ?`,
		s.cfg.Keyspace,
	)
	q := s.session.Query(query, joinKeyRaw, gocqlUUID, streamID, payload, streamID, now, joinKeyHash).WithContext(ctx)
	if err := q.Exec(); err != nil {
		return nil, fmt.Errorf("upsert participant: %w", err)
	}

	// Read back the row to get updated participant_data and arrival_timestamps (ScyllaDB doesn't support RETURNING in all versions).
	return s.GetState(ctx, joinKeyHash)
}

// UpsertParticipantOnly performs the map update only (no read back). Use when the key is known to be new (e.g. first arrival from Bloom filter).
func (s *Store) UpsertParticipantOnly(ctx context.Context, joinKeyHash []byte, joinKeyRaw string, configID uuid.UUID, streamID string, payload []byte) error {
	now := time.Now()
	gocqlUUID := gocql.UUID(configID)
	query := fmt.Sprintf(
		`UPDATE %s.join_state SET
			join_key_raw = ?,
			config_id = ?,
			participant_data[?] = ?,
			arrival_timestamps[?] = ?
		WHERE join_key_hash = ?`,
		s.cfg.Keyspace,
	)
	return s.session.Query(query, joinKeyRaw, gocqlUUID, streamID, payload, streamID, now, joinKeyHash).WithContext(ctx).Exec()
}

// GetState returns the current join state for the key, or nil if not found.
func (s *Store) GetState(ctx context.Context, joinKeyHash []byte) (*JoinState, error) {
	query := fmt.Sprintf(
		`SELECT join_key_hash, join_key_raw, config_id, participant_data, arrival_timestamps, is_completed
		 FROM %s.join_state WHERE join_key_hash = ?`,
		s.cfg.Keyspace,
	)
	var (
		hash      []byte
		raw       string
		gocqlUUID gocql.UUID
		partData  map[string][]byte
		arrivalTS map[string]time.Time
		completed bool
	)
	q := s.session.Query(query, joinKeyHash).WithContext(ctx)
	if err := q.Scan(&hash, &raw, &gocqlUUID, &partData, &arrivalTS, &completed); err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get state: %w", err)
	}
	// gocql.UUID is 16 bytes; uuid.FromBytes can fail only on wrong length; use zero UUID on error
	//nolint:errcheck // length is fixed; zero UUID used on impossible error
	configID, _ := uuid.FromBytes(gocqlUUID[:])
	if partData == nil {
		partData = make(map[string][]byte)
	}
	if arrivalTS == nil {
		arrivalTS = make(map[string]time.Time)
	}
	return &JoinState{
		JoinKeyHash:       hash,
		JoinKeyRaw:        raw,
		ConfigID:          configID,
		ParticipantData:   partData,
		ArrivalTimestamps: arrivalTS,
		IsCompleted:       completed,
	}, nil
}

// DeleteState removes the join state row (after successful egress).
func (s *Store) DeleteState(ctx context.Context, joinKeyHash []byte) error {
	query := fmt.Sprintf(`DELETE FROM %s.join_state WHERE join_key_hash = ?`, s.cfg.Keyspace)
	return s.session.Query(query, joinKeyHash).WithContext(ctx).Exec()
}
