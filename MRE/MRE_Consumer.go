package MRE

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// ==============================
// Config & Metrics
// ==============================

type RedisTLSConfig struct {
	Enabled bool
	// Add fields as needed (CA, cert, key, InsecureSkipVerify, etc.)
}

type MREConsumerConfig struct {
	// Redis connection
	Addr         string // "host:port"
	Password     string
	DB           int
	ClientName   string
	PoolSize     int
	MinIdleConns int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolTimeout  time.Duration
	MaxRetries   int
	MinBackoff   time.Duration
	MaxBackoff   time.Duration
	TLS          RedisTLSConfig

	// Streams
	Stream        string // e.g., "user:lookup"
	ConsumerGroup string // e.g., "user-lookup-group"
	// if group doesn't exist, create it with "0" (from beginning)
	AutoCreateGroup bool

	// Workers
	NumWorkers     int           // number of consumers in the group
	BatchSize      int64         // e.g., 50_000 (COUNT)
	BlockTimeout   time.Duration // XREADGROUP BLOCK
	MinIdleToClaim time.Duration // XAUTOCLAIM MinIdle
	ClaimBatchSize int64         // XAUTOCLAIM COUNT
	DeleteAfterAck bool          // XDEL after XACK

	// Processing
	WorkerTimeout   time.Duration // handler timeout per batch
	ShutdownTimeout time.Duration // graceful stop

	// Logging
	LogEveryN int // log progress every N batches per worker
}

type Metrics struct {
	MessagesRead      int64
	MessagesProcessed int64
	MessagesAcked     int64
	MessagesClaimed   int64
	ProcessingErrors  int64
	AckErrors         int64
	ClaimErrors       int64
	ReadErrors        int64
	LastBatchDuration int64 // ns
	StartTime         time.Time
}

// ==============================
// Handler contract
// ==============================

// BatchHandler processes a batch of XMessages atomically (your business logic).
// Return an error to leave the batch unacked (so it can be retried/claimed later).
type BatchHandler func(ctx context.Context, batch []redis.XMessage) error

// ==============================
// Consumer
// ==============================

type MREConsumer struct {
	cfg     *MREConsumerConfig
	cli     *redis.Client
	metrics *Metrics

	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup
}

func DefaultMREConsumerConfig() *MREConsumerConfig {
	return &MREConsumerConfig{
		Addr:         "127.0.0.1:6379",
		DB:           0,
		ClientName:   "mre-consumer",
		PoolSize:     64,
		MinIdleConns: 8,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  0, // let commands set their own Block timeouts
		WriteTimeout: 0,
		PoolTimeout:  4 * time.Second,
		MaxRetries:   3,
		MinBackoff:   10 * time.Millisecond,
		MaxBackoff:   500 * time.Millisecond,

		Stream:          "user:lookup",
		ConsumerGroup:   "user-lookup-group",
		AutoCreateGroup: true,

		NumWorkers:     3,
		BatchSize:      50_000,
		BlockTimeout:   1 * time.Second,
		MinIdleToClaim: 7 * time.Second,
		ClaimBatchSize: 50_000,
		DeleteAfterAck: true,

		WorkerTimeout:   30 * time.Second,
		ShutdownTimeout: 30 * time.Second,

		LogEveryN: 10,
	}
}

// NewMREConsumer builds a consumer that reads from a Redis Stream using consumer-group workers.
func NewMREConsumer(cfg *MREConsumerConfig) (*MREConsumer, error) {
	if cfg == nil {
		cfg = DefaultMREConsumerConfig()
	}

	opts := &redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,

		MaxRetries:      cfg.MaxRetries,
		MinRetryBackoff: cfg.MinBackoff,
		MaxRetryBackoff: cfg.MaxBackoff,
		DialTimeout:     cfg.DialTimeout,
		ReadTimeout:     cfg.ReadTimeout,
		WriteTimeout:    cfg.WriteTimeout,
		PoolTimeout:     cfg.PoolTimeout,
		ClientName:      cfg.ClientName,
	}
	// (If TLS is needed, add TLSConfig here.)

	cli := redis.NewClient(opts)

	ctx, cancel := context.WithCancel(context.Background())

	// Ping
	if err := cli.Ping(ctx).Err(); err != nil {
		cancel()
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	cons := &MREConsumer{
		cfg: cfg,
		cli: cli,
		metrics: &Metrics{
			StartTime: time.Now(),
		},
		ctx:    ctx,
		cancel: cancel,
	}
	return cons, nil
}

// ensureGroup creates the group if not exists (if AutoCreateGroup is true).
func (c *MREConsumer) ensureGroup() error {
	ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()

	// Check stream first; if it doesn't exist and we want to mkstream: XGROUP CREATE <s> <g> 0 MKSTREAM
	if c.cfg.AutoCreateGroup {
		err := c.cli.XGroupCreateMkStream(ctx, c.cfg.Stream, c.cfg.ConsumerGroup, "0").Err()
		if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
			return fmt.Errorf("XGroupCreateMkStream: %w", err)
		}
		return nil
	}

	// Validate the group exists
	_, err := c.cli.XInfoGroups(ctx, c.cfg.Stream).Result()
	if err != nil {
		return fmt.Errorf("group check failed (AutoCreateGroup=false): %w", err)
	}
	return nil
}

// Start launches N worker-consumers that coordinate via the Redis consumer group.
func (c *MREConsumer) Start(handler BatchHandler) error {
	if err := c.ensureGroup(); err != nil {
		return err
	}

	log.Printf("[MRE] starting %d workers on stream=%q group=%q (batch=%d)",
		c.cfg.NumWorkers, c.cfg.Stream, c.cfg.ConsumerGroup, c.cfg.BatchSize)

	// Start workers with unique consumer names
	for i := 0; i < c.cfg.NumWorkers; i++ {
		cName := fmt.Sprintf("%s-%d", safeName(c.cfg.ClientName, "mre"), i+1)
		c.wg.Add(1)
		go c.workerLoop(cName, handler)
	}

	// Optional: metrics ticker
	c.wg.Add(1)
	go c.metricPrinter()

	return nil
}

// Stop requests a graceful shutdown and waits up to ShutdownTimeout.
func (c *MREConsumer) Stop() {
	log.Printf("[MRE] stopping...")
	c.cancel()

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(c.cfg.ShutdownTimeout):
		log.Printf("[MRE] stop timeout — forcing exit")
	}

	_ = c.cli.Close()
	log.Printf("[MRE] stopped")
}

// ==============================
// Worker Logic (XAUTOCLAIM + XREADGROUP)
// ==============================

func (c *MREConsumer) workerLoop(consumerName string, handler BatchHandler) {
	defer c.wg.Done()

	var batchCount int64
	logPrefix := fmt.Sprintf("[wrk:%s]", consumerName)

	backoff := 100 * time.Millisecond
	maxBackoff := 5 * time.Second

	// First, recovery pass: claim old PEL messages (MinIdle)
	c.claimLoop(logPrefix, consumerName, handler)

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		start := time.Now()
		ctx, cancel := context.WithTimeout(c.ctx, c.cfg.BlockTimeout+2*time.Second)

		streams, err := c.cli.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    c.cfg.ConsumerGroup,
			Consumer: consumerName,
			Streams:  []string{c.cfg.Stream, ">"}, // new messages only
			Count:    c.cfg.BatchSize,
			Block:    c.cfg.BlockTimeout,
		}).Result()
		cancel()

		if err != nil {
			if err == redis.Nil || strings.Contains(err.Error(), "deadline exceeded") {
				// normal no-data case — just loop
				continue
			}
			atomic.AddInt64(&c.metrics.ReadErrors, 1)
			log.Printf("%s XREADGROUP error: %v — backing off %v", logPrefix, err, backoff)
			time.Sleep(backoff)
			backoff = minDuration(maxBackoff, time.Duration(float64(backoff)*1.5))
			continue
		}
		backoff = 100 * time.Millisecond

		for _, st := range streams {
			if len(st.Messages) == 0 {
				continue
			}

			atomic.AddInt64(&c.metrics.MessagesRead, int64(len(st.Messages)))
			batchCount++

			// Process batch
			if err := c.processAndAckBatch(logPrefix, consumerName, st.Messages, handler); err != nil {
				// Leave them pending for retry/claim later
				log.Printf("%s batch error (left unacked): %v", logPrefix, err)
				atomic.AddInt64(&c.metrics.ProcessingErrors, 1)
			}

			// recovery cadence: after some batches, try a quick claim pass to help herd stuck messages
			if batchCount%25 == 0 {
				c.claimLoopShort(logPrefix, consumerName, handler, 500*time.Millisecond)
			}

			// metrics
			atomic.StoreInt64(&c.metrics.LastBatchDuration, time.Since(start).Nanoseconds())
			if c.cfg.LogEveryN > 0 && int(batchCount)%c.cfg.LogEveryN == 0 {
				log.Printf("%s processed %d batches, total read=%d processed=%d acked=%d",
					logPrefix, batchCount,
					atomic.LoadInt64(&c.metrics.MessagesRead),
					atomic.LoadInt64(&c.metrics.MessagesProcessed),
					atomic.LoadInt64(&c.metrics.MessagesAcked),
				)
			}
		}
	}
}

func (c *MREConsumer) processAndAckBatch(
	logPrefix, consumerName string,
	msgs []redis.XMessage,
	handler BatchHandler,
) error {
	// Run handler with timeout
	ctx, cancel := context.WithTimeout(c.ctx, c.cfg.WorkerTimeout)
	defer cancel()

	if err := handler(ctx, msgs); err != nil {
		return err
	}
	atomic.AddInt64(&c.metrics.MessagesProcessed, int64(len(msgs)))

	// Ack in pipeline
	ackCtx, ackCancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer ackCancel()

	pipe := c.cli.Pipeline()
	for _, m := range msgs {
		pipe.XAck(ackCtx, c.cfg.Stream, c.cfg.ConsumerGroup, m.ID)
	}
	if _, err := pipe.Exec(ackCtx); err != nil {
		atomic.AddInt64(&c.metrics.AckErrors, 1)
		return fmt.Errorf("ack pipeline failed: %w", err)
	}
	atomic.AddInt64(&c.metrics.MessagesAcked, int64(len(msgs)))

	// Optional delete after ack
	if c.cfg.DeleteAfterAck {
		delCtx, delCancel := context.WithTimeout(c.ctx, 5*time.Second)
		defer delCancel()
		dpipe := c.cli.Pipeline()
		for _, m := range msgs {
			dpipe.XDel(delCtx, c.cfg.Stream, m.ID)
		}
		_, _ = dpipe.Exec(delCtx)
	}

	return nil
}

// claimLoop sweeps the PEL with XAUTOCLAIM (MinIdle) until cursor completes.
func (c *MREConsumer) claimLoop(logPrefix, consumerName string, handler BatchHandler) {
	start := "0-0"
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
		claimed, next, err := c.cli.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   c.cfg.Stream,
			Group:    c.cfg.ConsumerGroup,
			Consumer: consumerName,
			MinIdle:  c.cfg.MinIdleToClaim,
			Start:    start,
			Count:    c.cfg.ClaimBatchSize,
		}).Result()
		cancel()

		if err != nil {
			atomic.AddInt64(&c.metrics.ClaimErrors, 1)
			log.Printf("%s XAUTOCLAIM error: %v", logPrefix, err)
			return
		}
		if len(claimed) == 0 && next == "0-0" {
			// nothing to claim
			return
		}

		if len(claimed) > 0 {
			atomic.AddInt64(&c.metrics.MessagesClaimed, int64(len(claimed)))
			// process & ack claimed
			if err := c.processAndAckBatch(logPrefix, consumerName, claimed, handler); err != nil {
				// leave those claimed messages pending; they will be retried
				log.Printf("%s claimed-batch error (left unacked): %v", logPrefix, err)
			}
		}

		// advance cursor
		if next == "0-0" {
			return
		}
		start = next
	}
}

// claimLoopShort runs a quick single pass to help move stuck PEL entries periodically.
func (c *MREConsumer) claimLoopShort(logPrefix, consumerName string, handler BatchHandler, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(c.ctx, timeout)
	defer cancel()

	claimed, next, err := c.cli.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   c.cfg.Stream,
		Group:    c.cfg.ConsumerGroup,
		Consumer: consumerName,
		MinIdle:  c.cfg.MinIdleToClaim,
		Start:    "0-0",
		Count:    c.cfg.ClaimBatchSize,
	}).Result()

	if err != nil {
		atomic.AddInt64(&c.metrics.ClaimErrors, 1)
		log.Printf("%s quick XAUTOCLAIM error: %v", logPrefix, err)
		return
	}
	_ = next

	if len(claimed) > 0 {
		atomic.AddInt64(&c.metrics.MessagesClaimed, int64(len(claimed)))
		if err := c.processAndAckBatch(logPrefix, consumerName, claimed, handler); err != nil {
			log.Printf("%s quick-claim batch error (left unacked): %v", logPrefix, err)
		}
	}
}

// ==============================
// Metrics & Helpers
// ==============================

func (c *MREConsumer) metricPrinter() {
	defer c.wg.Done()
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-t.C:
			read := atomic.LoadInt64(&c.metrics.MessagesRead)
			proc := atomic.LoadInt64(&c.metrics.MessagesProcessed)
			acked := atomic.LoadInt64(&c.metrics.MessagesAcked)
			claimed := atomic.LoadInt64(&c.metrics.MessagesClaimed)
			perr := atomic.LoadInt64(&c.metrics.ProcessingErrors)
			aerr := atomic.LoadInt64(&c.metrics.AckErrors)
			rerr := atomic.LoadInt64(&c.metrics.ReadErrors)
			cerr := atomic.LoadInt64(&c.metrics.ClaimErrors)

			uptime := time.Since(c.metrics.StartTime).Truncate(time.Second)
			rate := float64(proc) / (float64(uptime) + 1e-9)

			lastBatch := time.Duration(atomic.LoadInt64(&c.metrics.LastBatchDuration))

			log.Printf("[MRE][metrics] up=%v read=%d processed=%d acked=%d claimed=%d rate=%.1f/s lastBatch=%v errs{proc=%d ack=%d read=%d claim=%d}",
				uptime, read, proc, acked, claimed, rate, lastBatch, perr, aerr, rerr, cerr)
		}
	}
}

func safeName(parts ...string) string {
	s := strings.Join(parts, "-")
	s = strings.ReplaceAll(s, " ", "-")
	if len(s) > 64 {
		return s[:64]
	}
	return s
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
