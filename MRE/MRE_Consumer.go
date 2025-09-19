package MRE

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "RedisStreams/api/proto"
)

// MRELookupConsumer handles consuming lookup requests from Redis Streams
type MRELookupConsumer struct {
	// Connection management
	client     pb.RedisStreamsClient
	conn       *grpc.ClientConn
	serverAddr string

	// Configuration
	config *MRELookupConsumerConfig

	// Pipeline channels
	messageChan chan *pb.Message
	ackChan     chan string

	// Metrics
	metrics *MRELookupConsumerMetrics

	// Lifecycle management
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	isRunning int64

	// Thread safety
	// mu sync.RWMutex // Removed unused field
}

// MRELookupConsumerConfig holds configuration for the consumer
type MRELookupConsumerConfig struct {
	// Server configuration
	ServerAddr string

	// Topic and stream configuration
	TopicName     string
	StreamName    string
	ConsumerGroup string
	ConsumerName  string

	// Connection settings
	ConnectionTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration

	// Consumer settings
	BatchSize       int64
	BlockTimeout    time.Duration
	ConsumerTimeout time.Duration
	AutoAck         bool
	MaxRetries      int

	// Performance settings
	ChannelBuffer     int
	ProcessingWorkers int
	EnableMetrics     bool
	StatsInterval     time.Duration

	// Graceful shutdown
	ShutdownTimeout time.Duration
}

// MRELookupConsumerMetrics tracks consumer performance
type MRELookupConsumerMetrics struct {
	// Core metrics
	MessagesReceived  int64
	MessagesProcessed int64
	MessagesFailed    int64
	MessagesAcked     int64
	BatchesProcessed  int64

	// Performance metrics
	ProcessingRate float64
	AverageLatency int64
	MaxLatency     int64
	MinLatency     int64

	// Error metrics
	ConnectionErrors int64
	ParseErrors      int64
	AckErrors        int64
	ProcessingErrors int64

	// Timestamps
	LastProcessedTime time.Time
	LastErrorTime     time.Time
	StartTime         time.Time

	// Thread-safe access
	mu sync.RWMutex
}

// DefaultMRELookupConsumerConfig returns default configuration
func DefaultMRELookupConsumerConfig(serverAddr string) *MRELookupConsumerConfig {
	return &MRELookupConsumerConfig{
		// Server configuration
		ServerAddr: serverAddr,

		// Topic and stream configuration
		TopicName:     "user.lookup",
		StreamName:    "user:lookup",
		ConsumerGroup: "user-lookup-group",
		ConsumerName:  "mre-lookup-consumer",

		// Connection settings
		ConnectionTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      10 * time.Second,

		// Consumer settings
		BatchSize:       50000, // Up to 50k messages per batch
		BlockTimeout:    1 * time.Second,
		ConsumerTimeout: 10 * time.Second,
		AutoAck:         false, // Manual ACK for better control
		MaxRetries:      3,

		// Performance settings
		ChannelBuffer:     100000, // Increased buffer for 50k batch processing
		ProcessingWorkers: 16,     // More workers for high throughput
		EnableMetrics:     true,
		StatsInterval:     5 * time.Second,

		// Graceful shutdown
		ShutdownTimeout: 30 * time.Second,
	}
}

// NewMRELookupConsumer creates a new consumer instance
func NewMRELookupConsumer(config *MRELookupConsumerConfig) (*MRELookupConsumer, error) {
	if config == nil {
		config = DefaultMRELookupConsumerConfig("localhost:16001")
	}

	// Create gRPC connection
	log.Printf("Connecting to gRPC server at %s...", config.ServerAddr)
	conn, err := grpc.Dial(config.ServerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC server at %s: %w", config.ServerAddr, err)
	}
	log.Printf("Successfully connected to gRPC server at %s", config.ServerAddr)

	client := pb.NewRedisStreamsClient(conn)

	ctx, cancel := context.WithCancel(context.Background())

	consumer := &MRELookupConsumer{
		client:      client,
		conn:        conn,
		serverAddr:  config.ServerAddr,
		config:      config,
		messageChan: make(chan *pb.Message, config.ChannelBuffer),
		ackChan:     make(chan string, config.ChannelBuffer),
		metrics: &MRELookupConsumerMetrics{
			StartTime: time.Now(),
		},
		ctx:    ctx,
		cancel: cancel,
	}

	return consumer, nil
}

// Start begins consuming messages
func (c *MRELookupConsumer) Start() error {
	if !atomic.CompareAndSwapInt64(&c.isRunning, 0, 1) {
		return fmt.Errorf("consumer is already running")
	}

	log.Printf("Starting MRE Lookup Consumer:")
	log.Printf("  Server: %s", c.serverAddr)
	log.Printf("  Topic: %s", c.config.TopicName)
	log.Printf("  Stream: %s", c.config.StreamName)
	log.Printf("  Consumer Group: %s", c.config.ConsumerGroup)
	log.Printf("  Consumer Name: %s", c.config.ConsumerName)
	log.Printf("  Batch Size: %d", c.config.BatchSize)
	log.Printf("  Processing Workers: %d", c.config.ProcessingWorkers)

	// Start processing workers
	for i := 0; i < c.config.ProcessingWorkers; i++ {
		c.wg.Add(1)
		go c.processingWorker(i)
	}

	// Start ACK worker
	c.wg.Add(1)
	go c.ackWorker()

	// Start metrics reporter
	if c.config.EnableMetrics {
		c.wg.Add(1)
		go c.metricsReporter()
	}

	// Start message receiver
	c.wg.Add(1)
	go c.messageReceiver()

	// Handle graceful shutdown
	go c.handleShutdown()

	return nil
}

// messageReceiver receives messages from the stream
func (c *MRELookupConsumer) messageReceiver() {
	defer c.wg.Done()
	defer close(c.messageChan)

	backoffDelay := 100 * time.Millisecond
	maxBackoffDelay := 10 * time.Second

	for {
		select {
		case <-c.ctx.Done():
			log.Printf("Message receiver stopping due to context cancellation")
			return
		default:
		}

		// Create subscription request
		req := &pb.SubscribeRequest{
			Topic:             c.config.TopicName,
			ConsumerName:      c.config.ConsumerName,
			ConsumerGroup:     c.config.ConsumerGroup,
			StartId:           ">", // Read new messages only (from consumer group's current position)
			BatchSize:         c.config.BatchSize,
			BlockTimeoutMs:    int64(c.config.BlockTimeout.Milliseconds()),
			ConsumerTimeoutMs: int64(c.config.ConsumerTimeout.Milliseconds()),
			AutoAck:           c.config.AutoAck,
		}

		// Create context with timeout for the subscription
		ctx, cancel := context.WithTimeout(c.ctx, c.config.ReadTimeout)
		stream, err := c.client.Subscribe(ctx, req)
		// Don't cancel immediately - let the context live for the subscription duration

		if err != nil {
			log.Printf("Failed to subscribe to %s: %v, retrying in %v", c.config.TopicName, err, backoffDelay)
			log.Printf("Subscription details: Topic=%s, ConsumerGroup=%s, ConsumerName=%s",
				c.config.TopicName, c.config.ConsumerGroup, c.config.ConsumerName)
			atomic.AddInt64(&c.metrics.ConnectionErrors, 1)
			cancel() // Cancel the subscription context on error

			select {
			case <-c.ctx.Done():
				return
			case <-time.After(backoffDelay):
			}

			// Exponential backoff
			backoffDelay = time.Duration(float64(backoffDelay) * 1.5)
			if backoffDelay > maxBackoffDelay {
				backoffDelay = maxBackoffDelay
			}
			continue
		}

		// Reset backoff on success
		backoffDelay = 100 * time.Millisecond

		// Receive messages in batches
		batchCount := 0
		totalMessages := 0
		for {
			select {
			case <-c.ctx.Done():
				log.Printf("Message receiver stopping due to context cancellation")
				cancel() // Cancel the subscription context
				return
			default:
			}

			msg, err := stream.Recv()
			if err != nil {
				if err.Error() == "EOF" {
					if totalMessages == 0 {
						log.Printf("Stream %s ended (EOF) - no messages available", c.config.TopicName)
						// Wait before reconnecting
						select {
						case <-c.ctx.Done():
							cancel() // Cancel the subscription context
							return
						case <-time.After(backoffDelay):
						}
					} else {
						log.Printf("Stream %s ended (EOF) - processed %d messages in %d batches", c.config.TopicName, totalMessages, batchCount)
						if totalMessages >= 10000 {
							log.Printf("Large batch session completed: %d messages in %d batches", totalMessages, batchCount)
						}
					}
				} else {
					log.Printf("Stream receive error for %s: %v", c.config.TopicName, err)
					atomic.AddInt64(&c.metrics.ConnectionErrors, 1)
				}
				cancel() // Cancel the subscription context before breaking
				break    // Break inner loop to reconnect
			}

			totalMessages++
			atomic.AddInt64(&c.metrics.MessagesReceived, 1)

			// Log batch progress every 10k messages
			if totalMessages%10000 == 0 {
				log.Printf("Received %d messages in current session (batch %d)", totalMessages, batchCount+1)
			}

			// Send message to processing channel
			select {
			case c.messageChan <- msg:
			case <-c.ctx.Done():
				cancel() // Cancel the subscription context
				return
			}
		}
	}
}

// processingWorker processes messages from the channel
func (c *MRELookupConsumer) processingWorker(workerID int) {
	defer c.wg.Done()

	log.Printf("Processing worker %d started", workerID)

	for {
		select {
		case <-c.ctx.Done():
			log.Printf("Processing worker %d stopping due to context cancellation", workerID)
			return
		case msg, ok := <-c.messageChan:
			if !ok {
				log.Printf("Processing worker %d stopping - message channel closed", workerID)
				return
			}

			startTime := time.Now()
			err := c.processLookupRequest(msg)
			processingTime := time.Since(startTime)

			// Update metrics
			c.updateProcessingMetrics(processingTime, err)

			if err != nil {
				log.Printf("Worker %d: Failed to process message %s: %v", workerID, msg.Id, err)
				atomic.AddInt64(&c.metrics.ProcessingErrors, 1)
			} else {
				atomic.AddInt64(&c.metrics.MessagesProcessed, 1)
				c.metrics.mu.Lock()
				c.metrics.LastProcessedTime = time.Now()
				c.metrics.mu.Unlock()

				// Send for ACK if not auto-ack
				if !c.config.AutoAck {
					select {
					case c.ackChan <- msg.Id:
					case <-c.ctx.Done():
						return
					}
				}
			}
		}
	}
}

// processLookupRequest processes a single lookup request
func (c *MRELookupConsumer) processLookupRequest(msg *pb.Message) error {
	// Extract fields from the message
	fields := msg.Fields.AsMap()
	if fields == nil {
		return fmt.Errorf("message has no fields")
	}

	// Parse the lookup request
	lookupReq, err := c.parseLookupRequest(fields)
	if err != nil {
		return fmt.Errorf("failed to parse lookup request: %w", err)
	}

	// Process the lookup request
	return c.handleLookupRequest(lookupReq, msg.Id)
}

// parseLookupRequest parses a lookup request from message fields
func (c *MRELookupConsumer) parseLookupRequest(fields map[string]interface{}) (*LookupRequest, error) {
	req := &LookupRequest{}

	// Parse basic fields
	if queryID, ok := fields["query_id"].(string); ok {
		req.QueryID = queryID
	} else {
		return nil, fmt.Errorf("missing or invalid query_id")
	}

	if userID, ok := fields["user_id"].(string); ok {
		req.UserID = userID
	} else {
		return nil, fmt.Errorf("missing or invalid user_id")
	}

	if lookupType, ok := fields["lookup_type"].(string); ok {
		req.LookupType = lookupType
	} else {
		return nil, fmt.Errorf("missing or invalid lookup_type")
	}

	if timestamp, ok := fields["timestamp"].(string); ok {
		req.Timestamp = timestamp
	} else {
		req.Timestamp = time.Now().Format(time.RFC3339)
	}

	// Parse fields map
	if fieldsData, ok := fields["fields"].(map[string]interface{}); ok {
		req.Fields = make(map[string]string)
		for k, v := range fieldsData {
			if str, ok := v.(string); ok {
				req.Fields[k] = str
			}
		}
	}

	// Parse result if present
	if resultData, ok := fields["result"].(map[string]interface{}); ok {
		result, err := c.parseResult(resultData)
		if err != nil {
			return nil, fmt.Errorf("failed to parse result: %w", err)
		}
		req.Result = result
	}

	return req, nil
}

// parseResult parses the result data from message fields
func (c *MRELookupConsumer) parseResult(resultData map[string]interface{}) (*Result, error) {
	result := &Result{}

	// Parse epoch
	if epoch, ok := resultData["epoch"].(float64); ok {
		result.Epoch = uint64(epoch)
	}

	// Parse primary
	if primary, ok := resultData["primary"].(string); ok {
		if p, err := strconv.ParseInt(primary, 10, 16); err == nil {
			result.Primary = int16(p)
		}
	}

	// Parse replicas
	if replicas, ok := resultData["replicas"].(string); ok {
		if replicas != "" {
			replicaStrs := strings.Split(replicas, ",")
			result.Replicas = make([]int16, 0, len(replicaStrs))
			for _, r := range replicaStrs {
				if r = strings.TrimSpace(r); r != "" {
					if val, err := strconv.ParseInt(r, 10, 16); err == nil {
						result.Replicas = append(result.Replicas, int16(val))
					}
				}
			}
		}
	}

	// Parse all
	if all, ok := resultData["all"].(string); ok {
		if all != "" {
			allStrs := strings.Split(all, ",")
			result.All = make([]int16, 0, len(allStrs))
			for _, a := range allStrs {
				if a = strings.TrimSpace(a); a != "" {
					if val, err := strconv.ParseInt(a, 10, 16); err == nil {
						result.All = append(result.All, int16(val))
					}
				}
			}
		}
	}

	return result, nil
}

// handleLookupRequest handles the actual lookup request processing
func (c *MRELookupConsumer) handleLookupRequest(req *LookupRequest, messageID string) error {
	// Log the lookup request
	log.Printf("Processing lookup request:")
	log.Printf("  Message ID: %s", messageID)
	log.Printf("  Query ID: %s", req.QueryID)
	log.Printf("  User ID: %s", req.UserID)
	log.Printf("  Lookup Type: %s", req.LookupType)
	log.Printf("  Timestamp: %s", req.Timestamp)
	log.Printf("  Fields: %+v", req.Fields)

	if req.Result != nil {
		log.Printf("  Result: Epoch=%d, Primary=%d, Replicas=%v, All=%v",
			req.Result.Epoch, req.Result.Primary, req.Result.Replicas, req.Result.All)
	}

	// Here you would implement your actual lookup logic
	// For now, we'll just simulate processing
	time.Sleep(10 * time.Millisecond) // Simulate processing time

	// Log completion
	log.Printf("Successfully processed lookup request %s", req.QueryID)

	return nil
}

// ackWorker handles message acknowledgments
func (c *MRELookupConsumer) ackWorker() {
	defer c.wg.Done()

	log.Printf("ACK worker started")

	for {
		select {
		case <-c.ctx.Done():
			log.Printf("ACK worker stopping due to context cancellation")
			return
		case msgID, ok := <-c.ackChan:
			if !ok {
				log.Printf("ACK worker stopping - ACK channel closed")
				return
			}

			// Send ACK
			ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
			_, err := c.client.Ack(ctx, &pb.AckRequest{
				Topic:         c.config.TopicName,
				ConsumerGroup: c.config.ConsumerGroup,
				Id:            msgID,
			})
			cancel()

			if err != nil {
				log.Printf("Failed to ACK message %s: %v", msgID, err)
				atomic.AddInt64(&c.metrics.AckErrors, 1)
			} else {
				atomic.AddInt64(&c.metrics.MessagesAcked, 1)
			}
		}
	}
}

// updateProcessingMetrics updates processing metrics
func (c *MRELookupConsumer) updateProcessingMetrics(processingTime time.Duration, err error) {
	c.metrics.mu.Lock()
	defer c.metrics.mu.Unlock()

	// Update latency metrics
	latencyNs := processingTime.Nanoseconds()
	if c.metrics.AverageLatency == 0 {
		c.metrics.AverageLatency = latencyNs
		c.metrics.MaxLatency = latencyNs
		c.metrics.MinLatency = latencyNs
	} else {
		// Simple moving average
		c.metrics.AverageLatency = (c.metrics.AverageLatency + latencyNs) / 2
		if latencyNs > c.metrics.MaxLatency {
			c.metrics.MaxLatency = latencyNs
		}
		if latencyNs < c.metrics.MinLatency {
			c.metrics.MinLatency = latencyNs
		}
	}

	// Update error timestamp
	if err != nil {
		c.metrics.LastErrorTime = time.Now()
	}
}

// metricsReporter reports metrics periodically
func (c *MRELookupConsumer) metricsReporter() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.StatsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.reportMetrics()
		}
	}
}

// reportMetrics reports current metrics
func (c *MRELookupConsumer) reportMetrics() {
	c.metrics.mu.RLock()
	defer c.metrics.mu.RUnlock()

	uptime := time.Since(c.metrics.StartTime)
	processingRate := float64(c.metrics.MessagesProcessed) / uptime.Seconds()

	log.Printf("=== MRE Lookup Consumer Metrics ===")
	log.Printf("Uptime: %v", uptime.Truncate(time.Second))
	log.Printf("Messages Received: %d", atomic.LoadInt64(&c.metrics.MessagesReceived))
	log.Printf("Messages Processed: %d", atomic.LoadInt64(&c.metrics.MessagesProcessed))
	log.Printf("Messages Failed: %d", atomic.LoadInt64(&c.metrics.MessagesFailed))
	log.Printf("Messages ACKed: %d", atomic.LoadInt64(&c.metrics.MessagesAcked))
	log.Printf("Processing Rate: %.2f msg/sec", processingRate)
	log.Printf("Average Latency: %v", time.Duration(c.metrics.AverageLatency))
	log.Printf("Max Latency: %v", time.Duration(c.metrics.MaxLatency))
	log.Printf("Min Latency: %v", time.Duration(c.metrics.MinLatency))
	log.Printf("Connection Errors: %d", atomic.LoadInt64(&c.metrics.ConnectionErrors))
	log.Printf("Parse Errors: %d", atomic.LoadInt64(&c.metrics.ParseErrors))
	log.Printf("ACK Errors: %d", atomic.LoadInt64(&c.metrics.AckErrors))
	log.Printf("Processing Errors: %d", atomic.LoadInt64(&c.metrics.ProcessingErrors))
	log.Printf("Last Processed: %v", c.metrics.LastProcessedTime.Format(time.RFC3339))
	if !c.metrics.LastErrorTime.IsZero() {
		log.Printf("Last Error: %v", c.metrics.LastErrorTime.Format(time.RFC3339))
	}
	log.Printf("=====================================")
}

// handleShutdown handles graceful shutdown
func (c *MRELookupConsumer) handleShutdown() {
	// Wait for shutdown signal (you can implement signal handling here)
	// For now, this is a placeholder
	<-c.ctx.Done()
}

// Stop stops the consumer
func (c *MRELookupConsumer) Stop() error {
	if !atomic.CompareAndSwapInt64(&c.isRunning, 1, 0) {
		return fmt.Errorf("consumer is not running")
	}

	log.Printf("Stopping MRE Lookup Consumer...")

	// Cancel context to signal shutdown
	c.cancel()

	// Wait for workers to finish
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Printf("All workers stopped gracefully")
	case <-time.After(c.config.ShutdownTimeout):
		log.Printf("Timeout waiting for workers to stop")
	}

	// Close gRPC connection
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			log.Printf("Error closing gRPC connection: %v", err)
		}
	}

	log.Printf("MRE Lookup Consumer stopped")
	return nil
}

// GetMetrics returns current metrics
func (c *MRELookupConsumer) GetMetrics() *MRELookupConsumerMetrics {
	c.metrics.mu.RLock()
	defer c.metrics.mu.RUnlock()

	// Create a copy to avoid race conditions
	metrics := MRELookupConsumerMetrics{
		MessagesReceived:  atomic.LoadInt64(&c.metrics.MessagesReceived),
		MessagesProcessed: atomic.LoadInt64(&c.metrics.MessagesProcessed),
		MessagesFailed:    atomic.LoadInt64(&c.metrics.MessagesFailed),
		MessagesAcked:     atomic.LoadInt64(&c.metrics.MessagesAcked),
		BatchesProcessed:  c.metrics.BatchesProcessed,
		ProcessingRate:    c.metrics.ProcessingRate,
		AverageLatency:    c.metrics.AverageLatency,
		MaxLatency:        c.metrics.MaxLatency,
		MinLatency:        c.metrics.MinLatency,
		ConnectionErrors:  atomic.LoadInt64(&c.metrics.ConnectionErrors),
		ParseErrors:       atomic.LoadInt64(&c.metrics.ParseErrors),
		AckErrors:         atomic.LoadInt64(&c.metrics.AckErrors),
		ProcessingErrors:  atomic.LoadInt64(&c.metrics.ProcessingErrors),
		LastProcessedTime: c.metrics.LastProcessedTime,
		LastErrorTime:     c.metrics.LastErrorTime,
		StartTime:         c.metrics.StartTime,
	}

	return &metrics
}

// Close closes the consumer
func (c *MRELookupConsumer) Close() error {
	return c.Stop()
}
