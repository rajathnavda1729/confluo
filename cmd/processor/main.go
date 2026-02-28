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

	"github.com/confluo/omni-joiner/internal/bloom"
	"github.com/confluo/omni-joiner/internal/config"
	"github.com/confluo/omni-joiner/internal/consumer"
	"github.com/confluo/omni-joiner/internal/delay"
	"github.com/confluo/omni-joiner/internal/egress"
	"github.com/confluo/omni-joiner/internal/kafka"
	"github.com/confluo/omni-joiner/internal/store"
	"github.com/confluo/omni-joiner/internal/timeout"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
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
	if cfg.JoinConfig == nil && cfg.ConfigPath != "" {
		jc, err := config.LoadJoinConfig(cfg.ConfigPath)
		if err != nil {
			return nil, err
		}
		cfg.JoinConfig = jc
	}
	return &cfg, nil
}

// retry runs fn until it succeeds or ctx is cancelled or maxDur elapses; backoff between attempts.
func retry(ctx context.Context, maxDur time.Duration, name string, fn func() error) error {
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
		log.Printf("%s not ready: %v; retrying in %v", name, err, backoff)
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
	if cfg.JoinConfig == nil {
		log.Fatal("join_config or config_path required")
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: ":9090", Handler: mux}
	go func() {
		_ = srv.ListenAndServe()
	}()
	defer srv.Shutdown(context.Background())

	storeCfg := store.Config{
		Hosts:    cfg.ScyllaHosts,
		Keyspace: cfg.ScyllaKeyspace,
		Timeout:  10 * time.Second,
	}
	var st *store.Store
	if err := retry(ctx, 30*time.Second, "Scylla", func() error {
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
	if err := retry(ctx, 30*time.Second, "Kafka", func() error {
		var err error
		kclient, err = kafka.NewClient(cfg.KafkaBrokers, "omni-joiner-processor", cfg.InputTopic)
		return err
	}); err != nil {
		return fmt.Errorf("connect to Kafka at %v: %w (is Redpanda running? try: docker compose up -d)", cfg.KafkaBrokers, err)
	}
	defer kclient.Close()

	var bloomFilter consumer.BloomChecker
	var timeoutSched consumer.TimeoutScheduler
	var lateTracker consumer.LateArrivalTracker
	correctionsTopic := cfg.CorrectionsTopic
	if cfg.RedisAddr != "" {
		rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
		defer rdb.Close()
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
		})
		timeoutSched = tm
		go tm.Run(ctx)
	}

	var delayQueue consumer.DelayPublisher
	if cfg.JoinConfig.PostJoinDelay.ToDuration() > 0 {
		dq := delay.NewQueue(st, kclient, cfg.EgressTopic)
		delayQueue = dq
		go dq.Run(ctx)
	}

	groupID := "omni-joiner-processor"
	log.Printf("processor starting (input=%s, egress=%s, group=%s)", cfg.InputTopic, cfg.EgressTopic, groupID)
	return consumer.Run(ctx, kclient.Client, cfg.JoinConfig, st, kclient, cfg.EgressTopic, bloomFilter, timeoutSched, delayQueue, lateTracker, correctionsTopic)
}
