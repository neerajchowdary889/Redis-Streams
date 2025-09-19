package QueueModule

import (
	RSconfig "RedisStreams/Config"
	"RedisStreams/Logging"
	metrics "RedisStreams/Metrics"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// MessageHandler defines the function signature for message processing
type MessageHandler func(ctx context.Context, topic string, msgID string, fields map[string]interface{}) error

// BatchMessageHandler defines the function signature for batch message processing
type BatchMessageHandler func(ctx context.Context, topic string, messages []redis.XMessage) error

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
	ConsumerGroup   string        `yaml:"consumer_group"`
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
		// get from config.yml
		Temp, err := RSconfig.TopicConfigLoader("Config/config.yml")
		if err != nil {
			return err
		}
		streamName = Temp.GetStreamName(topicName)
		if streamName == "" {
			return fmt.Errorf("stream name not configured for topic '%s'", topicName)
		}
		topic.StreamName = streamName
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
		// get from config.yml
		Temp, err := RSconfig.TopicConfigLoader("Config/config.yml")
		if err != nil {
			return "", err
		}
		streamName = Temp.GetStreamName(topicName)
		if streamName == "" {
			return "", fmt.Errorf("stream name not configured for topic '%s'", topicName)
		}
		topic.StreamName = streamName
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
			// get from config.yml
			Temp, err := RSconfig.TopicConfigLoader("Config/config.yml")
			if err != nil {
				return err
			}
			streamName = Temp.GetStreamName(msg.Topic)
			if streamName == "" {
				return fmt.Errorf("stream name not configured for topic '%s'", msg.Topic)
			}
			topic.StreamName = streamName
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
	// In this case two instances calling the same obj
	var topic_config *RSconfig.LoadTopicConfig
	var err error
	var streamName string
	var groupName string

	topic, exists := mq.Topics[consumerConfig.TopicName]
	if !exists {
		return fmt.Errorf("topic '%s' not configured", consumerConfig.TopicName)
	}

	if topic.StreamName == "" || consumerConfig.ConsumerGroup == "" {
		topic_config, err = RSconfig.TopicConfigLoader("Config/config.yml")
		if topic_config == nil || err != nil {
			return err
		}
	}

	streamName = topic.StreamName
	if streamName == "" {
		// get from config.yml
		streamName = topic_config.GetStreamName(consumerConfig.TopicName)
		if streamName == "" {
			return fmt.Errorf("stream name not configured for topic '%s'", consumerConfig.TopicName)
		}
	}

	// Fix consumer group name logic
	groupName = consumerConfig.ConsumerGroup
	if groupName == "" {
		// get from config.yml
		pairs := topic_config.GetStreamConsumerPairs()
		groupName = pairs[consumerConfig.TopicName]["consumer_group"]
		if groupName == "" {
			return fmt.Errorf("group name not configured for topic '%s'", consumerConfig.TopicName)
		}
		consumerConfig.ConsumerGroup = groupName
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
		consumerConfig.StartID = "0" // Start from beginning to process all messages including pending
	}

	// Check if consumer group exists first
	ctx, cancel := context.WithTimeout(mq.ctx, 5*time.Second)
	defer cancel()

	// Check if the stream exists
	_, err = mq.client.XInfoStream(ctx, streamName).Result()
	if err != nil {
		return fmt.Errorf("stream '%s' does not exist: %w", streamName, err)
	}

	// Check if consumer group exists
	groups, err := mq.client.XInfoGroups(ctx, streamName).Result()
	if err != nil {
		return fmt.Errorf("failed to get consumer groups for stream '%s': %w", streamName, err)
	}

	groupExists := false
	for _, group := range groups {
		if group.Name == groupName {
			groupExists = true
			break
		}
	}

	if !groupExists {
		return fmt.Errorf("consumer group '%s' does not exist for stream '%s'. Available groups: %v",
			groupName, streamName, getGroupNames(groups))
	}

	// Group exists, use the provided StartID for reading
	if consumerConfig.StartID == "" {
		// For existing groups, start from "0" to process pending messages
		consumerConfig.StartID = "0"
	}

	// Create consumer
	consumerKey := consumerConfig.ConsumerName
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

// SubscribeBatch creates a consumer and starts consuming messages in batches
func (mq *RedisStreamMQ) SubscribeBatch(consumerConfig ConsumerConfig, handler BatchMessageHandler) error {
	// In this case two instances calling the same obj
	var topic_config *RSconfig.LoadTopicConfig
	var err error
	var streamName string
	var groupName string

	topic, exists := mq.Topics[consumerConfig.TopicName]
	if !exists {
		return fmt.Errorf("topic '%s' not configured", consumerConfig.TopicName)
	}

	if topic.StreamName == "" || consumerConfig.ConsumerGroup == "" {
		topic_config, err = RSconfig.TopicConfigLoader("Config/config.yml")
		if topic_config == nil || err != nil {
			return err
		}
	}

	streamName = topic.StreamName
	if streamName == "" {
		// get from config.yml
		streamName = topic_config.GetStreamName(consumerConfig.TopicName)
		if streamName == "" {
			return fmt.Errorf("stream name not configured for topic '%s'", consumerConfig.TopicName)
		}
	}

	// Fix consumer group name logic
	groupName = consumerConfig.ConsumerGroup
	if groupName == "" {
		// get from config.yml
		pairs := topic_config.GetStreamConsumerPairs()
		groupName = pairs[consumerConfig.TopicName]["consumer_group"]
		if groupName == "" {
			return fmt.Errorf("group name not configured for topic '%s'", consumerConfig.TopicName)
		}
		consumerConfig.ConsumerGroup = groupName
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
		consumerConfig.StartID = "0" // Start from beginning to process all messages including pending
	}

	// Check if consumer group exists first
	ctx, cancel := context.WithTimeout(mq.ctx, 5*time.Second)
	defer cancel()

	// Check if the stream exists
	_, err = mq.client.XInfoStream(ctx, streamName).Result()
	if err != nil {
		return fmt.Errorf("stream '%s' does not exist: %w", streamName, err)
	}

	// Check if consumer group exists
	groups, err := mq.client.XInfoGroups(ctx, streamName).Result()
	if err != nil {
		return fmt.Errorf("failed to get consumer groups for stream '%s': %w", streamName, err)
	}

	groupExists := false
	for _, group := range groups {
		if group.Name == groupName {
			groupExists = true
			break
		}
	}

	if !groupExists {
		return fmt.Errorf("consumer group '%s' does not exist for stream '%s'. Available groups: %v",
			groupName, streamName, getGroupNames(groups))
	}

	// Group exists, use the provided StartID for reading
	if consumerConfig.StartID == "" {
		// For existing groups, start from "0" to process pending messages
		consumerConfig.StartID = "0"
	}

	// Create consumer
	consumerKey := consumerConfig.ConsumerName
	consumer := &Consumer{
		Config:    consumerConfig,
		Handler:   nil, // Not used for batch processing
		StopChan:  make(chan struct{}),
		IsRunning: true,
	}

	mq.mu.Lock()
	mq.activeConsumers[consumerKey] = consumer
	mq.mu.Unlock()

	// Start consuming
	mq.wg.Add(1)
	go mq.consumeMessagesBatch(streamName, groupName, consumer, handler)

	mq.logger.Info("Batch consumer started",
		"topic", consumerConfig.TopicName,
		"consumer", consumerConfig.ConsumerName,
		"group", groupName,
		"batch_size", consumerConfig.BatchSize)

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
	var lastProcessedID string = "0"

	// First, try to claim pending messages
	mq.logger.Info("Starting consumer, checking for pending messages...")
	mq.claimPendingMessages(streamName, groupName, config, consumer.Handler)

	for {
		select {
		case <-mq.ctx.Done():
			return
		case <-consumer.StopChan:
			return
		default:
			ctx, cancel := context.WithTimeout(mq.ctx, config.BlockTimeout+time.Second)

			// Determine start position to avoid duplicates
			startID := lastProcessedID
			if startID != "0" && startID != ">" {
				startID = mq.incrementMessageID(startID)
			}

			streams, err := mq.client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    groupName,
				Consumer: config.ConsumerName,
				Streams:  []string{streamName, startID}, // Read from last processed position
				Count:    config.BatchSize,
				Block:    config.BlockTimeout,
			}).Result()

			cancel()

			if err != nil {
				if err == redis.Nil {
					// No new messages, continue polling
					continue
				}

				// Check if it's a context timeout (expected behavior)
				if strings.Contains(err.Error(), "context deadline exceeded") ||
					strings.Contains(err.Error(), "context canceled") {
					// This is expected when no messages arrive within the timeout
					// Don't log as error, just continue
					continue
				}

				// Log other errors
				mq.logger.Error("Error reading from stream",
					"stream", streamName, "error", err)
				time.Sleep(time.Second)
				continue
			}

			// Process messages in batches
			for _, stream := range streams {
				if len(stream.Messages) > 0 {
					// Update last processed ID to the last message in the batch
					lastProcessedID = stream.Messages[len(stream.Messages)-1].ID

					// Track batch consumption
					mq.metrics.AddConsumedMessages(config.TopicName, int64(len(stream.Messages)))

					// Log batch size
					if len(stream.Messages) >= 1000 {
						mq.logger.Info("Processing large batch",
							"stream", stream.Stream,
							"batch_size", len(stream.Messages),
							"last_id", lastProcessedID)
					}

					// Process the entire batch at once
					mq.processBatch(stream.Stream, groupName, config, stream.Messages, consumer.Handler)
				}
			}
		}
	}
}

// consumeMessagesBatch handles batch message consumption
func (mq *RedisStreamMQ) consumeMessagesBatch(streamName, groupName string, consumer *Consumer, handler BatchMessageHandler) {
	defer mq.wg.Done()
	defer func() {
		mq.mu.Lock()
		consumer.IsRunning = false
		mq.mu.Unlock()
	}()

	config := consumer.Config
	var lastProcessedID string = config.StartID
	if lastProcessedID == "" {
		lastProcessedID = ">" // Start from new messages by default
	}

	// First, try to claim pending messages
	mq.logger.Info("Starting batch consumer, checking for pending messages...")
	mq.claimPendingMessagesBatch(streamName, groupName, config, handler)

	for {
		select {
		case <-mq.ctx.Done():
			return
		case <-consumer.StopChan:
			return
		default:
			ctx, cancel := context.WithTimeout(mq.ctx, config.BlockTimeout+time.Second)

			// Use ">" to read from consumer group's current position
			startID := ">"

			mq.logger.Info("Batch XReadGroup call",
				"stream", streamName,
				"startID", startID,
				"batchSize", config.BatchSize,
				"blockTimeout", config.BlockTimeout)

			streams, err := mq.client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    groupName,
				Consumer: config.ConsumerName,
				Streams:  []string{streamName, startID}, // Read from last processed position
				Count:    config.BatchSize,
				Block:    config.BlockTimeout,
			}).Result()

			cancel()

			if err != nil {
				if err == redis.Nil {
					// No new messages, continue polling
					continue
				}

				// Check if it's a context timeout (expected behavior)
				if strings.Contains(err.Error(), "context deadline exceeded") ||
					strings.Contains(err.Error(), "context canceled") {
					// This is expected when no messages arrive within the timeout
					// Don't log as error, just continue
					continue
				}

				// Log other errors
				mq.logger.Error("Error reading from stream",
					"stream", streamName, "error", err)
				time.Sleep(time.Second)
				continue
			}

			// Process messages in batches
			for _, stream := range streams {
				if len(stream.Messages) > 0 {
					// Update last processed ID to the last message in the batch BEFORE processing
					lastProcessedID = stream.Messages[len(stream.Messages)-1].ID

					// Track batch consumption
					mq.metrics.AddConsumedMessages(config.TopicName, int64(len(stream.Messages)))

					// Log batch size
					mq.logger.Info("Processing batch",
						"stream", stream.Stream,
						"batch_size", len(stream.Messages),
						"last_id", lastProcessedID)

					// Process the entire batch at once using the batch handler
					mq.processBatchWithHandler(stream.Stream, groupName, config, stream.Messages, handler)
				}
			}
		}
	}
}

// claimPendingMessages claims and processes pending messages for the consumer group
func (mq *RedisStreamMQ) claimPendingMessages(streamName, groupName string, config ConsumerConfig, handler MessageHandler) {
	mq.logger.Info("claimPendingMessages called", "streamName", streamName, "groupName", groupName)

	ctx, cancel := context.WithTimeout(mq.ctx, 10*time.Second)
	defer cancel()

	// Get pending messages for this consumer group
	pending, err := mq.client.XPending(ctx, streamName, groupName).Result()
	if err != nil {
		mq.logger.Error("Failed to get pending messages", "error", err)
		return
	}

	mq.logger.Info("Pending messages info", "count", pending.Count)

	if pending.Count == 0 {
		mq.logger.Info("No pending messages to claim")
		return
	}

	mq.logger.Info("Claiming pending messages", "count", pending.Count)

	// Claim all pending messages that are older than 1 second
	claimed, err := mq.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   streamName,
		Group:    groupName,
		Consumer: config.ConsumerName,
		MinIdle:  time.Second,
	}).Result()

	if err != nil {
		mq.logger.Error("Failed to claim pending messages", "error", err)
		return
	}

	if len(claimed) == 0 {
		mq.logger.Info("No messages to claim (all are too recent)")
		return
	}

	mq.logger.Info("Claimed pending messages", "count", len(claimed))

	// Process claimed messages
	for _, msg := range claimed {
		mq.metrics.IncConsumedMessages(config.TopicName)
		mq.processMessage(streamName, groupName, config, msg, handler)
	}
}

// processBatch processes a batch of messages
func (mq *RedisStreamMQ) processBatch(streamName, groupName string, config ConsumerConfig,
	messages []redis.XMessage, handler MessageHandler) {

	defer func() {
		if r := recover(); r != nil {
			mq.logger.Error("Panic in batch handler",
				"panic", r, "batch_size", len(messages))
			mq.metrics.AddProcessingErrors(config.TopicName, int64(len(messages)))
		}
	}()

	startTime := time.Now()
	batchSize := len(messages)

	mq.logger.Info("Processing batch",
		"stream", streamName,
		"batch_size", batchSize)

	// Process all messages in the batch
	processedCount := 0
	errorCount := 0

	for _, msg := range messages {
		handlerCtx, handlerCancel := context.WithTimeout(mq.ctx, config.ConsumerTimeout)

		err := handler(handlerCtx, config.TopicName, msg.ID, msg.Values)
		handlerCancel()

		if err != nil {
			// Check if it's a context cancellation error (expected during client disconnect)
			if err == context.Canceled || err == context.DeadlineExceeded {
				mq.logger.Debug("Message processing canceled in batch",
					"topic", config.TopicName,
					"message_id", msg.ID,
					"reason", err.Error())
			} else {
				mq.logger.Error("Error processing message in batch",
					"topic", config.TopicName,
					"message_id", msg.ID,
					"error", err)
				errorCount++
			}
		} else {
			processedCount++
		}
	}

	processingTime := time.Since(startTime)
	mq.metrics.RecordProcessingTime(config.TopicName, processingTime)
	mq.metrics.AddMessagesProcessed(config.TopicName, int64(processedCount))
	mq.metrics.AddProcessingErrors(config.TopicName, int64(errorCount))

	// Auto-ack all messages in the batch if enabled
	if config.AutoAck {
		// Use pipeline for faster ACK operations
		ackCtx, ackCancel := context.WithTimeout(mq.ctx, 5*time.Second)
		defer ackCancel()

		pipe := mq.client.Pipeline()

		// Add all ACK commands to pipeline
		for _, msg := range messages {
			pipe.XAck(ackCtx, streamName, groupName, msg.ID)
		}

		// Execute all ACKs at once
		_, err := pipe.Exec(ackCtx)
		if err == nil {
			// Track successful ACKs
			mq.metrics.AddMessagesAcknowledged(config.TopicName, int64(len(messages)))
		} else {
			mq.logger.Error("Batch ACK failed", "error", err, "batch_size", len(messages))
		}

		// Delete messages if configured
		if mq.config.Consumers.DeleteAfterAck {
			delCtx, delCancel := context.WithTimeout(mq.ctx, 5*time.Second)
			defer delCancel()

			delPipe := mq.client.Pipeline()
			for _, msg := range messages {
				delPipe.XDel(delCtx, streamName, msg.ID)
			}
			delPipe.Exec(delCtx)
		}
	}

	mq.logger.Info("Batch processing completed",
		"stream", streamName,
		"batch_size", batchSize,
		"processed", processedCount,
		"errors", errorCount,
		"duration", processingTime)
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
		// Check if it's a context cancellation error (expected during client disconnect)
		if err == context.Canceled || err == context.DeadlineExceeded {
			// Don't log context cancellation as an error - it's expected behavior
			mq.logger.Debug("Message processing canceled",
				"topic", config.TopicName,
				"message_id", msg.ID,
				"reason", err.Error())
			// Don't increment error metrics for context cancellation
		} else {
			mq.logger.Error("Error processing message",
				"topic", config.TopicName,
				"message_id", msg.ID,
				"error", err)
			mq.metrics.IncProcessingErrors(config.TopicName)

			// Handle retry logic or dead letter queue
			mq.handleMessageFailure(streamName, groupName, config, msg, err)
		}
	} else {
		mq.metrics.IncMessagesProcessed(config.TopicName)

		if config.AutoAck {
			ackCtx, ackCancel := context.WithTimeout(mq.ctx, time.Second)
			err := mq.client.XAck(ackCtx, streamName, groupName, msg.ID).Err()
			ackCancel()

			if err == nil {
				// Track successful ACK
				mq.metrics.IncMessagesAcknowledged(config.TopicName)
			}

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
		err := mq.client.XAck(ctx, streamName, groupName, msg.ID).Err()
		cancel()

		if err == nil {
			// Track successful ACK for failed messages sent to DLQ
			mq.metrics.IncMessagesAcknowledged(config.TopicName)
		}

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
	var topic_config *RSconfig.LoadTopicConfig
	var err error
	var streamName string

	topic, exists := mq.Topics[topicName]
	if !exists {
		return fmt.Errorf("topic '%s' not configured", topicName)
	}

	if topic.StreamName == "" || consumerGroup == "" {
		topic_config, err = RSconfig.TopicConfigLoader("Config/config.yml") // Load once
		if topic_config == nil || err != nil {
			return err
		}
	}

	streamName = topic.StreamName
	if streamName == "" {
		// get from config.yml
		streamName = topic_config.GetStreamName(topicName)
		if streamName == "" {
			return fmt.Errorf("stream name not configured for topic '%s'", topicName)
		}
	}

	if consumerGroup == "" {
		// get from config.yml
		consumerGroup = topic_config.GetStreamConsumerPairs()[topicName]["consumer_group"]
		if consumerGroup == "" {
			return fmt.Errorf("group name not configured for topic '%s'", topicName)
		}
	}

	ctx, cancel := context.WithTimeout(mq.ctx, time.Second)
	defer cancel()

	if err := mq.client.XAck(ctx, streamName, consumerGroup, msgID).Err(); err != nil {
		return err
	}

	// Track successful manual ACK
	mq.metrics.IncMessagesAcknowledged(topicName)

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
	var streamName string
	topic, exists := mq.Topics[topicName]
	if !exists {
		return nil, fmt.Errorf("topic '%s' not configured", topicName)
	}

	streamName = topic.StreamName
	if streamName == "" {
		// get from config.yml
		topic_config, err := RSconfig.TopicConfigLoader("Config/config.yml")
		if err != nil {
			return nil, err
		}
		streamName = topic_config.GetStreamName(topicName)
		if streamName == "" {
			return nil, fmt.Errorf("stream name not configured for topic '%s'", topicName)
		}
		topic.StreamName = streamName
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
		return fmt.Errorf("redis connection unhealthy: %w", err)
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
			// get from config.yml
			Temp, err := RSconfig.TopicConfigLoader("Config/config.yml")
			if err != nil {
				return
			}
			streamName = Temp.GetStreamName(topicName)
			if streamName == "" {
				return
			}
			topic.StreamName = streamName
		}

		// Collect stream length
		ctx, cancel := context.WithTimeout(mq.ctx, 5*time.Second)
		if info, err := mq.client.XInfoStream(ctx, topic.StreamName).Result(); err == nil {
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
	var topic_config *RSconfig.LoadTopicConfig
	var err error

	topic, exists := mq.Topics[topicName]
	if !exists {
		return fmt.Errorf("topic '%s' not configured", topicName)
	}

	var groupName string
	groupName = topic.ConsumerGroup
	if groupName == "" {
		// get from config.yml
		topic_config, err = RSconfig.TopicConfigLoader("Config/config.yml")
		if topic_config == nil || err != nil {
			return err
		}

		groupName = topic_config.GetStreamConsumerPairs()[topicName]["consumer_group"]
		if groupName == "" {
			return fmt.Errorf("group name not configured for topic '%s'", topicName)
		}
	}

	consumerKey := consumerName
	if consumerKey == "" {
		consumerKey = groupName
	}

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

// getGroupNames extracts group names from XInfoGroups result
func getGroupNames(groups []redis.XInfoGroup) []string {
	names := make([]string, len(groups))
	for i, group := range groups {
		names[i] = group.Name
	}
	return names
}

// incrementMessageID increments a Redis message ID to avoid duplicates
func (mq *RedisStreamMQ) incrementMessageID(msgID string) string {
	// Redis message IDs are in format: timestamp-sequence
	// We need to increment the sequence part to get the next message
	parts := strings.Split(msgID, "-")
	if len(parts) != 2 {
		// If we can't parse the ID, return ">" to read new messages
		return ">"
	}

	// Increment the sequence part
	sequence, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		// If we can't parse the sequence, return ">" to read new messages
		return ">"
	}

	return fmt.Sprintf("%s-%d", parts[0], sequence+1)
}

// claimPendingMessagesBatch claims and processes pending messages in batches
func (mq *RedisStreamMQ) claimPendingMessagesBatch(streamName, groupName string, config ConsumerConfig, handler BatchMessageHandler) {
	mq.logger.Info("claimPendingMessagesBatch called", "streamName", streamName, "groupName", groupName)

	ctx, cancel := context.WithTimeout(mq.ctx, 10*time.Second)
	defer cancel()

	// Get pending messages for this consumer group
	pending, err := mq.client.XPending(ctx, streamName, groupName).Result()
	if err != nil {
		mq.logger.Error("Failed to get pending messages", "error", err)
		return
	}

	mq.logger.Info("Pending messages info", "count", pending.Count)

	if pending.Count == 0 {
		mq.logger.Info("No pending messages to claim")
		return
	}

	mq.logger.Info("Claiming pending messages", "count", pending.Count)

	// Claim all pending messages that are older than 1 second
	claimed, err := mq.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   streamName,
		Group:    groupName,
		Consumer: config.ConsumerName,
		MinIdle:  time.Second,
	}).Result()

	if err != nil {
		mq.logger.Error("Failed to claim pending messages", "error", err)
		return
	}

	if len(claimed) == 0 {
		mq.logger.Info("No messages to claim (all are too recent)")
		return
	}

	mq.logger.Info("Claimed pending messages", "count", len(claimed))

	// Process claimed messages as a batch
	if len(claimed) > 0 {
		mq.metrics.AddConsumedMessages(config.TopicName, int64(len(claimed)))
		mq.processBatchWithHandler(streamName, groupName, config, claimed, handler)
	}
}

// processBatchWithHandler processes a batch of messages using the batch handler
func (mq *RedisStreamMQ) processBatchWithHandler(streamName, groupName string, config ConsumerConfig,
	messages []redis.XMessage, handler BatchMessageHandler) {

	defer func() {
		if r := recover(); r != nil {
			mq.logger.Error("Panic in batch handler",
				"panic", r, "batch_size", len(messages))
			mq.metrics.AddProcessingErrors(config.TopicName, int64(len(messages)))
		}
	}()

	startTime := time.Now()
	batchSize := len(messages)

	mq.logger.Info("Processing batch with handler",
		"stream", streamName,
		"batch_size", batchSize)

	// Process the entire batch at once using the batch handler
	handlerCtx, handlerCancel := context.WithTimeout(mq.ctx, config.ConsumerTimeout)
	defer handlerCancel()

	err := handler(handlerCtx, config.TopicName, messages)
	processingTime := time.Since(startTime)

	mq.metrics.RecordProcessingTime(config.TopicName, processingTime)

	if err != nil {
		// Check if it's a context cancellation error (expected during client disconnect)
		if err == context.Canceled || err == context.DeadlineExceeded {
			mq.logger.Debug("Batch processing canceled",
				"topic", config.TopicName,
				"batch_size", batchSize,
				"reason", err.Error())
		} else {
			mq.logger.Error("Error processing batch",
				"topic", config.TopicName,
				"batch_size", batchSize,
				"error", err)
			mq.metrics.AddProcessingErrors(config.TopicName, int64(batchSize))
		}
	} else {
		mq.metrics.AddMessagesProcessed(config.TopicName, int64(batchSize))

		// Auto-ack all messages in the batch if enabled
		if config.AutoAck {
			// Use pipeline for faster ACK operations
			ackCtx, ackCancel := context.WithTimeout(mq.ctx, 5*time.Second)
			defer ackCancel()

			pipe := mq.client.Pipeline()

			// Add all ACK commands to pipeline
			for _, msg := range messages {
				pipe.XAck(ackCtx, streamName, groupName, msg.ID)
			}

			// Execute all ACKs at once
			_, err := pipe.Exec(ackCtx)
			if err == nil {
				// Track successful ACKs
				mq.metrics.AddMessagesAcknowledged(config.TopicName, int64(len(messages)))
			} else {
				mq.logger.Error("Batch ACK failed", "error", err, "batch_size", len(messages))
			}

			// Delete messages if configured
			if mq.config.Consumers.DeleteAfterAck {
				delCtx, delCancel := context.WithTimeout(mq.ctx, 5*time.Second)
				defer delCancel()

				delPipe := mq.client.Pipeline()
				for _, msg := range messages {
					delPipe.XDel(delCtx, streamName, msg.ID)
				}
				delPipe.Exec(delCtx)
			}
		}
	}

	mq.logger.Info("Batch processing completed",
		"stream", streamName,
		"batch_size", batchSize,
		"duration", processingTime)
}
