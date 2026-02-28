// Package main runs the Omni-Joiner stream processor.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/confluo/omni-joiner/internal/bloom"
	"github.com/confluo/omni-joiner/internal/config"
	"github.com/confluo/omni-joiner/internal/consumer"
	"github.com/confluo/omni-joiner/internal/delay"
	"github.com/confluo/omni-joiner/internal/egress"
	"github.com/confluo/omni-joiner/internal/health"
	"github.com/confluo/omni-joiner/internal/kafka"
	"github.com/confluo/omni-joiner/internal/logger"
	"github.com/confluo/omni-joiner/internal/store"
	"github.com/confluo/omni-joiner/internal/timeout"
)

func main() {
	cfgPath := flag.String("config", "config/processor.json", "Path to processor config JSON")
	flag.Parse()

	cfg, err := loadProcessorConfig(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, cfg); err != nil && ctx.Err() == nil {
		log.Fatalf("run: %v", err)
	}
}

func loadProcessorConfig(path string) (*config.ProcessorConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg config.ProcessorConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	// Single-config: config_path or inline join_config
	if cfg.JoinConfig == nil && cfg.ConfigPath != "" {
		jc, err := config.LoadJoinConfig(cfg.ConfigPath)
		if err != nil {
			return nil, err
		}
		cfg.JoinConfig = jc
	}
	// Multi-config: config_paths or inline join_configs
	if len(cfg.JoinConfigs) == 0 && len(cfg.ConfigPaths) > 0 {
		cfg.JoinConfigs = make([]*config.JoinConfig, 0, len(cfg.ConfigPaths))
		for _, p := range cfg.ConfigPaths {
			jc, err := config.LoadJoinConfig(p)
			if err != nil {
				return nil, fmt.Errorf("load %s: %w", p, err)
			}
			cfg.JoinConfigs = append(cfg.JoinConfigs, jc)
		}
	}
	return &cfg, nil
}

// redisPingerAdapter adapts redis.Client to health.RedisPinger.
type redisPingerAdapter struct {
	c *redis.Client
}

func (r *redisPingerAdapter) Ping(ctx context.Context) error {
	return r.c.Ping(ctx).Err()
}

// retry runs fn until it succeeds or ctx is cancelled or maxDur elapses; backoff between attempts.
func retry(ctx context.Context, maxDur time.Duration, name string, log logger.Logger, fn func() error) error {
	if log == nil {
		log = logger.Global
	}
	deadline := time.Now().Add(maxDur)
	backoff := 2 * time.Second
	for {
		err := fn()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			return err
		}
		log.Warn("dependency not ready, retrying", "name", name, "error", err, "backoff", backoff)
		select {
		case <-time.After(backoff):
			if backoff < 10*time.Second {
				backoff += 2 * time.Second
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func run(ctx context.Context, cfg *config.ProcessorConfig) error {
	if cfg.MultiConfig() {
		if len(cfg.JoinConfigs) == 0 {
			log.Fatal("join_configs or config_paths required for multi-config")
		}
		for _, jc := range cfg.JoinConfigs {
			if err := config.ValidateJoinConfig(jc); err != nil {
				log.Fatalf("invalid join config %q: %v", jc.Name, err)
			}
		}
	} else {
		if cfg.JoinConfig == nil {
			log.Fatal("join_config or config_path required")
		}
		if err := config.ValidateJoinConfig(cfg.JoinConfig); err != nil {
			log.Fatalf("invalid join config: %v", err)
		}
	}

	log := logger.NewFromEnv()

	storeCfg := store.Config{
		Hosts:    cfg.ScyllaHosts,
		Keyspace: cfg.ScyllaKeyspace,
		Timeout:  10 * time.Second,
	}
	var st *store.Store
	if err := retry(ctx, 30*time.Second, "Scylla", log, func() error {
		if e := store.EnsureKeyspaceAndTable(ctx, storeCfg); e != nil {
			return e
		}
		s, e := store.New(storeCfg)
		if e != nil {
			return e
		}
		st = s
		return nil
	}); err != nil {
		return err
	}
	if st == nil {
		return fmt.Errorf("Scylla session not created")
	}
	defer st.Close()

	var kclient *kafka.Client
	if err := retry(ctx, 30*time.Second, "Kafka", log, func() error {
		var err error
		kclient, err = kafka.NewClient(cfg.KafkaBrokers, "omni-joiner-processor", cfg.InputTopic)
		return err
	}); err != nil {
		return fmt.Errorf("connect to Kafka at %v: %w (is Redpanda running? try: docker compose up -d)", cfg.KafkaBrokers, err)
	}
	defer kclient.Close()

	var redisPing health.RedisPinger
	var rdb *redis.Client
	if cfg.RedisAddr != "" {
		rdb = redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
		defer rdb.Close()
		redisPing = &redisPingerAdapter{c: rdb}
	}

	healthChecker := health.NewChecker(st, kclient, redisPing)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/health", healthChecker)
	mux.Handle("/ready", healthChecker)
	mux.Handle("/live", healthChecker)
	srv := &http.Server{Addr: ":9090", Handler: mux}
	go func() {
		// Best-effort metrics server; shutdown via defer srv.Shutdown below
		//nolint:errcheck // intentional: we shut down via defer
		_ = srv.ListenAndServe()
	}()
	defer func() {
		//nolint:errcheck // best-effort shutdown; use ctx so shutdown respects cancellation
		_ = srv.Shutdown(ctx)
	}()

	var bloomFilter consumer.BloomChecker
	var timeoutSched consumer.TimeoutScheduler
	var lateTracker consumer.LateArrivalTracker
	correctionsTopic := cfg.CorrectionsTopic
	if !cfg.MultiConfig() && rdb != nil {
		bloomFilter = bloom.New(rdb, cfg.RedisBloomKey)
		if cfg.CorrectionsTopic != "" {
			lateTracker = egress.NewRedisPartialTracker(rdb, "omni_joiner:partial")
		}
		tm := timeout.New(timeout.Config{
			Store:          st,
			JoinConfig:     cfg.JoinConfig,
			Producer:       kclient,
			EgressTopic:    cfg.EgressTopic,
			Redis:          rdb,
			SetKey:         "omni_joiner:timeouts",
			PartialTracker: lateTracker,
			Log:            log,
		})
		timeoutSched = tm
		go tm.Run(ctx)
	}

	var delayQueue consumer.DelayPublisher
	if !cfg.MultiConfig() && cfg.JoinConfig != nil && cfg.JoinConfig.PostJoinDelay.ToDuration() > 0 {
		var redisKey string
		if rdb != nil {
			redisKey = "omni_joiner:delay:" + cfg.JoinConfig.Name
		}
		dq := delay.NewQueue(st, kclient, cfg.EgressTopic, cfg.JoinConfig.Name, log, rdb, redisKey)
		delayQueue = dq
		go dq.Run(ctx)
	}

	groupID := "omni-joiner-processor"
	log.Info("processor starting", "input", cfg.InputTopic, "egress", cfg.EgressTopic, "group", groupID)
	if cfg.MultiConfig() {
		return runMultiConfig(ctx, cfg, kclient, st, rdb, log, correctionsTopic)
	}
	return runSingleConfig(ctx, cfg, kclient, st, rdb, log, bloomFilter, timeoutSched, delayQueue, lateTracker, correctionsTopic)
}

func runSingleConfig(ctx context.Context, cfg *config.ProcessorConfig, kclient *kafka.Client, st *store.Store, rdb *redis.Client, log logger.Logger, bloomFilter consumer.BloomChecker, timeoutSched consumer.TimeoutScheduler, delayQueue consumer.DelayPublisher, lateTracker consumer.LateArrivalTracker, correctionsTopic string) error {
	return consumer.RunSingle(ctx, kclient.Client, cfg.JoinConfig, st, kclient, cfg.EgressTopic, bloomFilter, timeoutSched, delayQueue, lateTracker, correctionsTopic, log)
}

func runMultiConfig(ctx context.Context, cfg *config.ProcessorConfig, kclient *kafka.Client, st *store.Store, rdb *redis.Client, log logger.Logger, correctionsTopic string) error {
	handlersByConfigID := make(map[uuid.UUID]*consumer.Handler)
	for _, jc := range cfg.JoinConfigs {
		stateStore := store.NewScopedStore(st, jc.ConfigID)
		var bloomFilter consumer.BloomChecker
		if rdb != nil {
			bloomFilter = bloom.New(rdb, cfg.RedisBloomKey+":"+jc.ConfigID.String())
		}
		var timeoutSched consumer.TimeoutScheduler
		var tracker consumer.LateArrivalTracker
		if rdb != nil {
			if cfg.CorrectionsTopic != "" {
				tracker = egress.NewRedisPartialTracker(rdb, "omni_joiner:partial:"+jc.ConfigID.String())
			}
			tm := timeout.New(timeout.Config{
				Store:          stateStore,
				JoinConfig:     jc,
				Producer:       kclient,
				EgressTopic:    cfg.EgressTopic,
				Redis:          rdb,
				SetKey:         "omni_joiner:timeouts:" + jc.ConfigID.String(),
				PartialTracker: tracker,
				Log:            log,
			})
			timeoutSched = tm
			go tm.Run(ctx)
		}
		var delayQueue consumer.DelayPublisher
		if jc.PostJoinDelay.ToDuration() > 0 {
			var redisKey string
			if rdb != nil {
				redisKey = "omni_joiner:delay:" + jc.Name
			}
			dq := delay.NewQueue(stateStore, kclient, cfg.EgressTopic, jc.Name, log, rdb, redisKey)
			delayQueue = dq
			go dq.Run(ctx)
		}
		handler := consumer.NewHandler(stateStore, jc, kclient, cfg.EgressTopic, bloomFilter, timeoutSched, delayQueue, tracker, correctionsTopic, log)
		handlersByConfigID[jc.ConfigID] = handler
	}
	return consumer.Run(ctx, kclient.Client, consumer.HandlerResolverFromMap(handlersByConfigID), log)
}
