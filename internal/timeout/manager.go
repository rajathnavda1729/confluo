package timeout

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/confluo/omni-joiner/internal/config"
	"github.com/confluo/omni-joiner/internal/consumer"
	"github.com/confluo/omni-joiner/internal/projection"
	"github.com/confluo/omni-joiner/internal/store"
)

const (
	defaultPollInterval = 5 * time.Second
	defaultBatchSize    = 100
)

// Manager schedules join-key timeouts and processes partial egress when state expires incomplete.
type Manager struct {
	store          *store.Store
	joinCfg        *config.JoinConfig
	producer       consumer.Producer
	egressTopic    string
	redis          redis.UniversalClient
	setKey         string
	pollInterval   time.Duration
	batchSize      int
	partialTracker PartialTracker
}

// Config for the timeout manager.
type Config struct {
	Store          *store.Store
	JoinConfig     *config.JoinConfig
	Producer       consumer.Producer
	EgressTopic    string
	Redis          redis.UniversalClient
	SetKey         string
	PollInterval   time.Duration
	BatchSize      int
	PartialTracker PartialTracker
}

// PartialTracker records keys that had partial egress (for late-arrival corrections).
type PartialTracker interface {
	Add(ctx context.Context, joinKeyHash []byte) error
}

// New creates a timeout manager.
func New(cfg Config) *Manager {
	if cfg.SetKey == "" {
		cfg.SetKey = "omni_joiner:timeouts"
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = defaultBatchSize
	}
	return &Manager{
		store:          cfg.Store,
		joinCfg:        cfg.JoinConfig,
		producer:       cfg.Producer,
		egressTopic:    cfg.EgressTopic,
		redis:          cfg.Redis,
		setKey:         cfg.SetKey,
		pollInterval:   cfg.PollInterval,
		batchSize:      cfg.BatchSize,
		partialTracker: cfg.PartialTracker,
	}
}

// ScheduleTimeout adds joinKeyHash to the timeout set with the given deadline (Unix seconds).
func (m *Manager) ScheduleTimeout(ctx context.Context, joinKeyHash []byte, deadline time.Time) error {
	member := hex.EncodeToString(joinKeyHash)
	score := float64(deadline.Unix())
	_, err := m.redis.ZAdd(ctx, m.setKey, redis.Z{Score: score, Member: member}).Result()
	if err != nil {
		return fmt.Errorf("schedule timeout: %w", err)
	}
	return nil
}

// Run runs the timeout loop until ctx is cancelled. Call in a goroutine.
func (m *Manager) Run(ctx context.Context) {
	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.processDue(ctx); err != nil {
				continue
			}
		}
	}
}

func (m *Manager) processDue(ctx context.Context) error {
	now := time.Now().Unix()
	members, err := m.redis.ZRangeByScore(ctx, m.setKey, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   fmt.Sprintf("%d", now),
		Count: int64(m.batchSize),
	}).Result()
	if err != nil {
		return err
	}
	for _, member := range members {
		keyHash, err := hex.DecodeString(member)
		if err != nil {
			continue
		}
		state, err := m.store.GetState(ctx, keyHash)
		if err != nil || state == nil {
			m.redis.ZRem(ctx, m.setKey, member)
			continue
		}
		if len(state.ParticipantData) >= m.joinCfg.N() {
			m.redis.ZRem(ctx, m.setKey, member)
			continue
		}
		if m.joinCfg.Egress == config.EgressPartial {
			partial, err := projection.Apply(state.ParticipantData, m.joinCfg.Projection)
			if err != nil {
				continue
			}
			if m.producer != nil {
				if err := m.publish(ctx, partial, keyHash); err != nil {
					log.Printf("timeout partial egress produce failed (key=%x): %v", keyHash, err)
					continue
				}
			}
			if m.partialTracker != nil {
				//nolint:errcheck // best-effort; partial egress already published
				_ = m.partialTracker.Add(ctx, keyHash)
			}
		}
		//nolint:errcheck // best-effort; state is removed from timeout set below
		_ = m.store.DeleteState(ctx, keyHash)
		m.redis.ZRem(ctx, m.setKey, member)
	}
	return nil
}

func (m *Manager) publish(ctx context.Context, value []byte, key []byte) error {
	return m.producer.ProduceSync(ctx, m.egressTopic, key, value)
}
