package QueueModule

import (
	RSconfig "RedisStreams/Config"
	"RedisStreams/Logging"
	metrics "RedisStreams/Metrics"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// MessageHandler defines the function signature for message processing
type MessageHandler func(ctx context.Context, topic string, msgID string, fields map[string]interface{}) error

// RedisStreamMQ is the main client for Redis Streams messaging
type RedisStreamMQ struct {
	config          *RSconfig.Config
	client          *redis.Client
	Topics          map[string]*RSconfig.TopicConfig
	activeConsumers map[string]*Consumer
	mu              sync.RWMutex
	ctx             context.Context
	cancel          context.CancelFunc
	logger          RSconfig.Logger
	wg              sync.WaitGroup
	metrics         *metrics.Metrics
}

// Consumer represents an active consumer
type Consumer struct {
	Config    ConsumerConfig
	Handler   MessageHandler
	StopChan  chan struct{}
	IsRunning bool
}

// ConsumerConfig holds configuration for a consumer
type ConsumerConfig struct {
	TopicName       string        `yaml:"topic_name"`
	ConsumerName    string        `yaml:"consumer_name"`
	StartID         string        `yaml:"start_id"`
	BatchSize       int64         `yaml:"batch_size"`
	BlockTimeout    time.Duration `yaml:"block_timeout"`
	ConsumerTimeout time.Duration `yaml:"consumer_timeout"`
	AutoAck         bool          `yaml:"auto_ack"`
	MaxRetries      int           `yaml:"max_retries"`
	Description     string        `yaml:"description"`
}

// BatchMessage represents a message in a batch
type BatchMessage struct {
	Topic  string                 `json:"topic"`
	ID     string                 `json:"id"`
	Fields map[string]interface{} `json:"fields"`
}

// MessageMetadata holds message metadata
type MessageMetadata struct {
	MessageID  string            `json:"message_id"`
	Topic      string            `json:"topic"`
	Timestamp  time.Time         `json:"timestamp"`
	Headers    map[string]string `json:"headers"`
	RetryCount int               `json:"retry_count"`
	Source     string            `json:"source"`
}

// New creates a new Redis Streams MQ client
func New(config *RSconfig.Config, logger RSconfig.Logger) (*RedisStreamMQ, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	if logger == nil {
		logger = &Logging.DefaultLogger{}
	}

	ctx, cancel := context.WithCancel(context.Background())

	mq := &RedisStreamMQ{
		config:          config,
		Topics:          make(map[string]*RSconfig.TopicConfig),
		activeConsumers: make(map[string]*Consumer),
		ctx:             ctx,
		cancel:          cancel,
		logger:          logger,
		metrics:         metrics.NewMetrics(),
	}

	// Load topic configurations
	for i := range config.Topics {
		mq.Topics[config.Topics[i].Name] = &config.Topics[i]
	}

	if err := mq.connect(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	// Auto-create streams if enabled
	if config.Streams.AutoCreateStreams {
		if err := mq.createStreams(); err != nil {
			logger.Warn("Failed to auto-create some streams", "error", err)
		}
	}

	// Start monitoring if enabled
	if config.Monitoring.Enabled {
		go mq.startMonitoring()
	}

	logger.Info("Redis Streams MQ client initialized successfully",
		"topics", len(config.Topics),
		"redis_host", config.Redis.Host,
		"redis_port", config.Redis.Port)

	return mq, nil
}

// connect establishes connection to Redis
func (mq *RedisStreamMQ) connect() error {
	addr := fmt.Sprintf("%s:%d", mq.config.Redis.Host, mq.config.Redis.Port)

	// Optimize pool size based on performance config
	poolSize := mq.config.Redis.PoolSize
	if mq.config.Performance.WorkerPoolSize > poolSize {
		poolSize = mq.config.Performance.WorkerPoolSize * 2 // 2x workers for Redis connections
	}

	opts := &redis.Options{
		Addr:            addr,
		Password:        mq.config.Redis.Password,
		DB:              mq.config.Redis.Database,
		PoolSize:        poolSize,
		MinIdleConns:    mq.config.Redis.MinIdleConns,
		MaxRetries:      mq.config.Redis.MaxRetries,
		MinRetryBackoff: mq.config.Redis.MinRetryBackoff,
		MaxRetryBackoff: mq.config.Redis.MaxRetryBackoff,
		DialTimeout:     mq.config.Redis.DialTimeout,
		ReadTimeout:     mq.config.Redis.ReadTimeout,
		WriteTimeout:    mq.config.Redis.WriteTimeout,
		PoolTimeout:     mq.config.Redis.PoolTimeout,
		ClientName:      mq.config.Redis.ClientName,
	}

	// Configure TLS if enabled
	if mq.config.Redis.TLS.Enabled {
		opts.TLSConfig = Logging.CreateTLSConfig(mq.config.Redis.TLS)
	}

	mq.client = redis.NewClient(opts)

	// Test connection
	ctx, cancel := context.WithTimeout(mq.ctx, 5*time.Second)
	defer cancel()

	if _, err := mq.client.Ping(ctx).Result(); err != nil {
		return fmt.Errorf("failed to ping Redis: %w", err)
	}

	mq.logger.Info("Connected to Redis", "addr", addr)
	return nil
}

// createStreams creates all configured streams
func (mq *RedisStreamMQ) createStreams() error {
	for _, topic := range mq.Topics {
		if err := mq.CreateTopic(topic.Name); err != nil {
			mq.logger.Error("Failed to create stream for topic",
				"topic", topic.Name, "error", err)
			return err
		}
	}
	return nil
}

// CreateTopic creates or configures a stream for a topic
func (mq *RedisStreamMQ) CreateTopic(topicName string) error {
	topic, exists := mq.Topics[topicName]
	if !exists {
		return fmt.Errorf("topic '%s' not configured", topicName)
	}

	ctx, cancel := context.WithTimeout(mq.ctx, 5*time.Second)
	defer cancel()

	streamName := topic.StreamName
	if streamName == "" {
		streamName = fmt.Sprintf("stream:%s", topicName)
	}

	// Add a dummy message to create the stream
	msgID, err := mq.client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName,
		Values: map[string]interface{}{
			"__init__": "stream_initialization",
			"topic":    topicName,
		},
	}).Result()

	if err != nil {
		return fmt.Errorf("failed to create stream %s: %w", streamName, err)
	}

	// Remove the dummy message
	mq.client.XDel(ctx, streamName, msgID)

	mq.logger.Info("Stream created/configured",
		"topic", topicName, "stream", streamName)
	return nil
}

// Publish publishes a message to a topic
func (mq *RedisStreamMQ) Publish(topicName string, data interface{}, headers ...map[string]string) (string, error) {
	topic, exists := mq.Topics[topicName]
	if !exists {
		return "", fmt.Errorf("topic '%s' not configured", topicName)
	}

	streamName := topic.StreamName
	if streamName == "" {
		streamName = fmt.Sprintf("stream:%s", topicName)
	}

	// Prepare message fields
	fields := make(map[string]interface{})

	// Add message metadata
	metadata := MessageMetadata{
		Topic:     topicName,
		Timestamp: time.Now(),
		Source:    mq.config.Redis.ClientName,
		Headers:   make(map[string]string),
	}

	// Add headers if provided
	if len(headers) > 0 {
		for k, v := range headers[0] {
			metadata.Headers[k] = v
		}
	}

	// Marshal data based on type
	switch v := data.(type) {
	case string:
		fields["data"] = v
		fields["content_type"] = "text/plain"
	case []byte:
		fields["data"] = string(v)
		fields["content_type"] = "application/octet-stream"
	default:
		jsonData, err := json.Marshal(v)
		if err != nil {
			return "", fmt.Errorf("failed to marshal data: %w", err)
		}
		fields["data"] = string(jsonData)
		fields["content_type"] = "application/json"
	}

	// Add metadata
	metadataJSON, _ := json.Marshal(metadata)
	fields["metadata"] = string(metadataJSON)

	ctx, cancel := context.WithTimeout(mq.ctx, mq.config.Redis.WriteTimeout)
	defer cancel()

	args := &redis.XAddArgs{
		Stream: streamName,
		ID:     "*",
		Values: fields,
	}

	// Apply stream limits
	if topic.MaxLen > 0 {
		args.MaxLen = topic.MaxLen
		if topic.TrimStrategy == "MAXLEN" || topic.TrimStrategy == "" {
			args.Approx = true
		}
	}

	msgID, err := mq.client.XAdd(ctx, args).Result()
	if err != nil {
		mq.metrics.IncPublishErrors(topicName)
		return "", fmt.Errorf("failed to publish to topic %s: %w", topicName, err)
	}

	mq.metrics.IncMessagesPublished(topicName)
	mq.logger.Debug("Message published",
		"topic", topicName, "message_id", msgID)

	return msgID, nil
}

// PublishBatch publishes multiple messages in a batch
func (mq *RedisStreamMQ) PublishBatch(messages []BatchMessage) error {
	if len(messages) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(mq.ctx, mq.config.Redis.WriteTimeout*5)
	defer cancel()

	pipe := mq.client.Pipeline()
	topicCounts := make(map[string]int)

	for _, msg := range messages {
		topic, exists := mq.Topics[msg.Topic]
		if !exists {
			mq.logger.Warn("Topic not configured, skipping message", "topic", msg.Topic)
			continue
		}

		streamName := topic.StreamName
		if streamName == "" {
			streamName = fmt.Sprintf("stream:%s", msg.Topic)
		}

		args := &redis.XAddArgs{
			Stream: streamName,
			ID:     msg.ID,
			Values: msg.Fields,
		}

		if topic.MaxLen > 0 {
			args.MaxLen = topic.MaxLen
			args.Approx = true
		}

		pipe.XAdd(ctx, args)
		topicCounts[msg.Topic]++
	}

	if _, err := pipe.Exec(ctx); err != nil {
		for topic := range topicCounts {
			mq.metrics.IncPublishErrors(topic)
		}
		return fmt.Errorf("failed to publish batch messages: %w", err)
	}

	for topic, count := range topicCounts {
		mq.metrics.AddMessagesPublished(topic, count)
	}

	mq.logger.Debug("Batch messages published", "count", len(messages))
	return nil
}

// Subscribe creates a consumer and starts consuming messages
func (mq *RedisStreamMQ) Subscribe(consumerConfig ConsumerConfig, handler MessageHandler) error {
	topic, exists := mq.Topics[consumerConfig.TopicName]
	if !exists {
		return fmt.Errorf("topic '%s' not configured", consumerConfig.TopicName)
	}

	streamName := topic.StreamName
	if streamName == "" {
		streamName = fmt.Sprintf("stream:%s", consumerConfig.TopicName)
	}

	groupName := topic.ConsumerGroup
	if groupName == "" {
		groupName = fmt.Sprintf("group:%s", consumerConfig.TopicName)
	}

	// Apply defaults
	if consumerConfig.BatchSize == 0 {
		consumerConfig.BatchSize = mq.config.Consumers.DefaultBatchSize
	}
	if consumerConfig.BlockTimeout == 0 {
		consumerConfig.BlockTimeout = mq.config.Consumers.DefaultBlockTimeout
	}
	if consumerConfig.ConsumerTimeout == 0 {
		consumerConfig.ConsumerTimeout = mq.config.Consumers.DefaultConsumerTimeout
	}
	if consumerConfig.StartID == "" {
		consumerConfig.StartID = "$"
	}

	// Create consumer group
	ctx, cancel := context.WithTimeout(mq.ctx, 5*time.Second)
	defer cancel()

	err := mq.client.XGroupCreateMkStream(ctx, streamName, groupName, consumerConfig.StartID).Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("failed to create consumer group %s: %w", groupName, err)
	}

	// Create consumer
	consumerKey := fmt.Sprintf("%s:%s", groupName, consumerConfig.ConsumerName)
	consumer := &Consumer{
		Config:    consumerConfig,
		Handler:   handler,
		StopChan:  make(chan struct{}),
		IsRunning: true,
	}

	mq.mu.Lock()
	mq.activeConsumers[consumerKey] = consumer
	mq.mu.Unlock()

	// Start consuming
	mq.wg.Add(1)
	go mq.consumeMessages(streamName, groupName, consumer)

	mq.logger.Info("Consumer started",
		"topic", consumerConfig.TopicName,
		"consumer", consumerConfig.ConsumerName,
		"group", groupName)

	return nil
}

// consumeMessages handles message consumption
func (mq *RedisStreamMQ) consumeMessages(streamName, groupName string, consumer *Consumer) {
	defer mq.wg.Done()
	defer func() {
		mq.mu.Lock()
		consumer.IsRunning = false
		mq.mu.Unlock()
	}()

	config := consumer.Config

	for {
		select {
		case <-mq.ctx.Done():
			return
		case <-consumer.StopChan:
			return
		default:
			ctx, cancel := context.WithTimeout(mq.ctx, config.BlockTimeout+time.Second)

			streams, err := mq.client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    groupName,
				Consumer: config.ConsumerName,
				Streams:  []string{streamName, ">"},
				Count:    config.BatchSize,
				Block:    config.BlockTimeout,
			}).Result()

			cancel()

			if err != nil {
				if err == redis.Nil {
					continue
				}
				mq.logger.Error("Error reading from stream",
					"stream", streamName, "error", err)
				time.Sleep(time.Second)
				continue
			}

			// Process messages
			for _, stream := range streams {
				for _, msg := range stream.Messages {
					mq.processMessage(stream.Stream, groupName, config, msg, consumer.Handler)
				}
			}
		}
	}
}

// processMessage processes a single message
func (mq *RedisStreamMQ) processMessage(streamName, groupName string, config ConsumerConfig,
	msg redis.XMessage, handler MessageHandler) {

	defer func() {
		if r := recover(); r != nil {
			mq.logger.Error("Panic in message handler",
				"panic", r, "message_id", msg.ID)
			mq.metrics.IncProcessingErrors(config.TopicName)
		}
	}()

	handlerCtx, handlerCancel := context.WithTimeout(mq.ctx, config.ConsumerTimeout)
	defer handlerCancel()

	startTime := time.Now()
	err := handler(handlerCtx, config.TopicName, msg.ID, msg.Values)
	processingTime := time.Since(startTime)

	mq.metrics.RecordProcessingTime(config.TopicName, processingTime)

	if err != nil {
		mq.logger.Error("Error processing message",
			"topic", config.TopicName,
			"message_id", msg.ID,
			"error", err)
		mq.metrics.IncProcessingErrors(config.TopicName)

		// Handle retry logic or dead letter queue
		mq.handleMessageFailure(streamName, groupName, config, msg, err)
	} else {
		mq.metrics.IncMessagesProcessed(config.TopicName)

		if config.AutoAck {
			ackCtx, ackCancel := context.WithTimeout(mq.ctx, time.Second)
			mq.client.XAck(ackCtx, streamName, groupName, msg.ID)
			ackCancel()

			if mq.config.Consumers.DeleteAfterAck {
				delCtx, delCancel := context.WithTimeout(mq.ctx, time.Second)
				mq.client.XDel(delCtx, streamName, msg.ID)
				delCancel()
			}
		}
	}
}

// handleMessageFailure handles message processing failures
func (mq *RedisStreamMQ) handleMessageFailure(streamName, groupName string,
	config ConsumerConfig, msg redis.XMessage, err error) {

	// Extract retry count from message metadata
	retryCount := 0
	if metadataStr, ok := msg.Values["metadata"].(string); ok {
		var metadata MessageMetadata
		if json.Unmarshal([]byte(metadataStr), &metadata) == nil {
			retryCount = metadata.RetryCount
		}
	}

	topic := mq.Topics[config.TopicName]
	maxRetries := config.MaxRetries
	if maxRetries == 0 && topic != nil {
		maxRetries = topic.RetryAttempts
	}

	if retryCount >= maxRetries {
		// Send to dead letter queue if configured
		if topic != nil && topic.DeadLetterTopic != "" {
			mq.sendToDeadLetter(topic.DeadLetterTopic, msg, err)
		}

		// Acknowledge to prevent reprocessing
		ctx, cancel := context.WithTimeout(mq.ctx, time.Second)
		mq.client.XAck(ctx, streamName, groupName, msg.ID)
		cancel()

		if mq.config.Consumers.DeleteAfterAck {
			delCtx, delCancel := context.WithTimeout(mq.ctx, time.Second)
			mq.client.XDel(delCtx, streamName, msg.ID)
			delCancel()
		}
	}
	// If not max retries, message will remain unacknowledged for retry
}

// sendToDeadLetter sends failed message to dead letter topic
func (mq *RedisStreamMQ) sendToDeadLetter(deadLetterTopic string,
	originalMsg redis.XMessage, err error) {

	deadLetterData := map[string]interface{}{
		"original_message": originalMsg.Values,
		"original_id":      originalMsg.ID,
		"error":            err.Error(),
		"failed_at":        time.Now(),
	}

	_, publishErr := mq.Publish(deadLetterTopic, deadLetterData)
	if publishErr != nil {
		mq.logger.Error("Failed to send message to dead letter topic",
			"topic", deadLetterTopic, "error", publishErr)
	}
}

// AckMessage manually acknowledges a message
func (mq *RedisStreamMQ) AckMessage(topicName, consumerGroup, msgID string) error {
	topic, exists := mq.Topics[topicName]
	if !exists {
		return fmt.Errorf("topic '%s' not configured", topicName)
	}

	streamName := topic.StreamName
	if streamName == "" {
		streamName = fmt.Sprintf("stream:%s", topicName)
	}

	groupName := consumerGroup
	if groupName == "" {
		groupName = fmt.Sprintf("group:%s", topicName)
	}

	ctx, cancel := context.WithTimeout(mq.ctx, time.Second)
	defer cancel()

	if err := mq.client.XAck(ctx, streamName, groupName, msgID).Err(); err != nil {
		return err
	}

	if mq.config.Consumers.DeleteAfterAck {
		delCtx, delCancel := context.WithTimeout(mq.ctx, time.Second)
		defer delCancel()
		if err := mq.client.XDel(delCtx, streamName, msgID).Err(); err != nil {
			return err
		}
	}

	return nil
}

// GetTopicInfo returns information about a topic's stream
func (mq *RedisStreamMQ) GetTopicInfo(topicName string) (*redis.XInfoStream, error) {
	topic, exists := mq.Topics[topicName]
	if !exists {
		return nil, fmt.Errorf("topic '%s' not configured", topicName)
	}

	streamName := topic.StreamName
	if streamName == "" {
		streamName = fmt.Sprintf("stream:%s", topicName)
	}

	ctx, cancel := context.WithTimeout(mq.ctx, 5*time.Second)
	defer cancel()

	return mq.client.XInfoStream(ctx, streamName).Result()
}

// ListTopics returns all configured topics
func (mq *RedisStreamMQ) ListTopics() []string {
	mq.mu.RLock()
	defer mq.mu.RUnlock()

	topics := make([]string, 0, len(mq.Topics))
	for name := range mq.Topics {
		topics = append(topics, name)
	}
	return topics
}

// Health checks the health of the connection and streams
func (mq *RedisStreamMQ) Health() error {
	ctx, cancel := context.WithTimeout(mq.ctx, 3*time.Second)
	defer cancel()

	if _, err := mq.client.Ping(ctx).Result(); err != nil {
		return fmt.Errorf("Redis connection unhealthy: %w", err)
	}

	return nil
}

// GetMetrics returns current metrics
func (mq *RedisStreamMQ) GetMetrics() map[string]interface{} {
	return mq.metrics.GetAll()
}

// startMonitoring starts the monitoring goroutine
func (mq *RedisStreamMQ) startMonitoring() {
	ticker := time.NewTicker(mq.config.Monitoring.MetricsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-mq.ctx.Done():
			return
		case <-ticker.C:
			mq.collectMetrics()
		}
	}
}

// collectMetrics collects various metrics
func (mq *RedisStreamMQ) collectMetrics() {
	for topicName, topic := range mq.Topics {
		streamName := topic.StreamName
		if streamName == "" {
			streamName = fmt.Sprintf("stream:%s", topicName)
		}

		// Collect stream length
		ctx, cancel := context.WithTimeout(mq.ctx, 5*time.Second)
		if info, err := mq.client.XInfoStream(ctx, streamName).Result(); err == nil {
			mq.metrics.SetStreamLength(topicName, info.Length)
		}
		cancel()
	}
}

// Close gracefully closes the Redis Streams MQ client
func (mq *RedisStreamMQ) Close() error {
	mq.logger.Info("Shutting down Redis Streams MQ client...")

	// Signal all consumers to stop
	mq.mu.Lock()
	for _, consumer := range mq.activeConsumers {
		if consumer.IsRunning {
			close(consumer.StopChan)
			consumer.IsRunning = false
		}
	}
	mq.mu.Unlock()

	// Cancel context
	mq.cancel()

	// Wait for consumers to stop
	done := make(chan struct{})
	go func() {
		mq.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		mq.logger.Info("All consumers stopped gracefully")
	case <-time.After(30 * time.Second):
		mq.logger.Warn("Timeout waiting for consumers to stop")
	}

	// Close Redis client
	if mq.client != nil {
		if err := mq.client.Close(); err != nil {
			mq.logger.Error("Error closing Redis client", "error", err)
		}
	}

	mq.logger.Info("Redis Streams MQ client shutdown completed")
	return nil
}

// StopConsumer stops a specific consumer
func (mq *RedisStreamMQ) StopConsumer(topicName, consumerName string) error {
	topic, exists := mq.Topics[topicName]
	if !exists {
		return fmt.Errorf("topic '%s' not configured", topicName)
	}

	groupName := topic.ConsumerGroup
	if groupName == "" {
		groupName = fmt.Sprintf("group:%s", topicName)
	}

	consumerKey := fmt.Sprintf("%s:%s", groupName, consumerName)

	mq.mu.Lock()
	consumer, exists := mq.activeConsumers[consumerKey]
	if !exists {
		mq.mu.Unlock()
		return fmt.Errorf("consumer not found: %s", consumerKey)
	}

	if consumer.IsRunning {
		close(consumer.StopChan)
		consumer.IsRunning = false
	}

	delete(mq.activeConsumers, consumerKey)
	mq.mu.Unlock()

	mq.logger.Info("Consumer stopped",
		"topic", topicName, "consumer", consumerName)
	return nil
}

// GetRedisClient returns the underlying Redis client for direct operations
func (mq *RedisStreamMQ) GetRedisClient() *redis.Client {
	return mq.client
}
