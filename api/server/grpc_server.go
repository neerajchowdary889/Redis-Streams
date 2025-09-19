package server

import (
	RSconfig "RedisStreams/Config"
	mq "RedisStreams/QueueModule"
	pb "RedisStreams/api/proto"
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	structpb "google.golang.org/protobuf/types/known/structpb"
)

type RedisStreamsServer struct {
	pb.UnimplementedRedisStreamsServer
	mq            *mq.RedisStreamMQ
	config        *RSconfig.Config
	client        *redis.Client
	workerPool    chan struct{}
	messagePool   sync.Pool
	ackPool       sync.Pool
	mu            sync.RWMutex
	activeStreams map[string]context.CancelFunc
}

func NewRedisStreamsServer(mqClient *mq.RedisStreamMQ, cfg *RSconfig.Config) *RedisStreamsServer {
	// Get Redis client from MQ for direct operations
	client := mqClient.GetRedisClient()

	// Calculate optimal worker pool size (CPU cores * 2)
	workerCount := runtime.NumCPU() * 2
	if workerCount < 4 {
		workerCount = 4
	}
	if workerCount > 16 {
		workerCount = 16
	}

	server := &RedisStreamsServer{
		mq:            mqClient,
		config:        cfg,
		client:        client,
		workerPool:    make(chan struct{}, workerCount),
		activeStreams: make(map[string]context.CancelFunc),
	}

	// Initialize object pools for memory efficiency
	server.messagePool = sync.Pool{
		New: func() interface{} {
			return &pb.Message{
				Fields: &structpb.Struct{Fields: make(map[string]*structpb.Value)},
			}
		},
	}

	server.ackPool = sync.Pool{
		New: func() interface{} {
			return &pb.AckRequest{}
		},
	}

	return server
}

func (s *RedisStreamsServer) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	var payload interface{}
	switch {
	case req.Text != "":
		payload = req.Text
	case len(req.BytesData) > 0:
		payload = req.BytesData
	case req.Json != nil:
		payload = req.Json.AsMap()
	default:
		return nil, errors.New("empty payload")
	}
	id, err := s.mq.Publish(req.Topic, payload, req.Headers)
	if err != nil {
		return nil, err
	}
	return &pb.PublishResponse{Id: id}, nil
}

func (s *RedisStreamsServer) PublishBatch(ctx context.Context, req *pb.PublishBatchRequest) (*pb.PublishBatchResponse, error) {
	// Use parallel processing for large batches
	if len(req.Messages) > 100 {
		return s.publishBatchParallel(ctx, req)
	}

	msgs := make([]mq.BatchMessage, 0, len(req.Messages))
	for _, m := range req.Messages {
		fields := m.Fields.AsMap()
		msgs = append(msgs, mq.BatchMessage{Topic: m.Topic, ID: m.Id, Fields: fields})
	}
	if err := s.mq.PublishBatch(msgs); err != nil {
		return nil, err
	}
	return &pb.PublishBatchResponse{}, nil
}

// PublishStream handles streaming publish requests for high throughput
func (s *RedisStreamsServer) PublishStream(stream pb.RedisStreams_PublishStreamServer) error {
	const batchSize = 1000
	const flushInterval = 10 * time.Millisecond

	var batch []mq.BatchMessage
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	ctx := stream.Context()

	// Channel to receive messages from stream
	msgChan := make(chan *pb.PublishRequest, batchSize)
	errChan := make(chan error, 1)

	// Goroutine to receive messages
	go func() {
		defer close(msgChan)
		for {
			req, err := stream.Recv()
			if err != nil {
				errChan <- err
				return
			}

			select {
			case msgChan <- req:
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			}
		}
	}()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		// Create a copy for async processing
		batchCopy := make([]mq.BatchMessage, len(batch))
		copy(batchCopy, batch)

		select {
		case s.workerPool <- struct{}{}:
			go func() {
				defer func() { <-s.workerPool }()
				if err := s.mq.PublishBatch(batchCopy); err != nil {
					// Log error - can't return it in async context
					fmt.Printf("PublishBatch error: %v\n", err)
				}
			}()
		default:
			// Fallback to synchronous processing
			if err := s.mq.PublishBatch(batchCopy); err != nil {
				return err
			}
		}

		batch = batch[:0] // Reset slice but keep capacity
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return flush() // Final flush before shutdown

		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}

		case req, ok := <-msgChan:
			if !ok {
				// Channel closed, check for error
				if err := <-errChan; err != nil {
					// Final flush before returning error
					flush()
					return err
				}
				// Normal completion
				return flush()
			}

			// Process message
			fields := req.Json.AsMap()
			batch = append(batch, mq.BatchMessage{
				Topic:  req.Topic,
				ID:     "*",
				Fields: fields,
			})

			// Flush if batch is full
			if len(batch) >= batchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

// publishBatchParallel processes large batches in parallel
func (s *RedisStreamsServer) publishBatchParallel(ctx context.Context, req *pb.PublishBatchRequest) (*pb.PublishBatchResponse, error) {
	const chunkSize = 100
	chunks := make([][]mq.BatchMessage, 0, (len(req.Messages)+chunkSize-1)/chunkSize)

	// Split into chunks
	for i := 0; i < len(req.Messages); i += chunkSize {
		end := i + chunkSize
		if end > len(req.Messages) {
			end = len(req.Messages)
		}

		chunk := make([]mq.BatchMessage, 0, end-i)
		for j := i; j < end; j++ {
			m := req.Messages[j]
			fields := m.Fields.AsMap()
			chunk = append(chunk, mq.BatchMessage{Topic: m.Topic, ID: m.Id, Fields: fields})
		}
		chunks = append(chunks, chunk)
	}

	// Process chunks in parallel
	var wg sync.WaitGroup
	errChan := make(chan error, len(chunks))

	for _, chunk := range chunks {
		wg.Add(1)
		go func(msgs []mq.BatchMessage) {
			defer wg.Done()
			select {
			case s.workerPool <- struct{}{}:
				defer func() { <-s.workerPool }()
				if err := s.mq.PublishBatch(msgs); err != nil {
					errChan <- err
				}
			default:
				// Fallback to synchronous processing
				if err := s.mq.PublishBatch(msgs); err != nil {
					errChan <- err
				}
			}
		}(chunk)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		if err != nil {
			return nil, err
		}
	}

	return &pb.PublishBatchResponse{}, nil
}

func (s *RedisStreamsServer) Subscribe(req *pb.SubscribeRequest, stream pb.RedisStreams_SubscribeServer) error {
	fmt.Printf("gRPC Server: Subscribe method called with request: %+v\n", req)

	// Get the stream context to detect client disconnection
	ctx := stream.Context()
	var topic_config *RSconfig.LoadTopicConfig
	var err error

	if req.ConsumerGroup == "" || req.ConsumerName == "" {
		// Read from the config.yml file
		topic_config, err = RSconfig.TopicConfigLoader("Config/config.yml")
		if err != nil {
			return err
		}
	}

	if req.ConsumerGroup == "" {
		pairs := topic_config.GetStreamConsumerPairs()
		req.ConsumerGroup = pairs[req.Topic]["consumer_group"]
	}

	if req.ConsumerName == "" {

		pairs := topic_config.GetStreamConsumerPairs()
		req.ConsumerName = pairs[req.Topic]["consumer_name"]
	}

	conf := mq.ConsumerConfig{
		TopicName:       req.Topic,
		ConsumerName:    req.ConsumerName,
		StartID:         ">", // Read new messages only for production
		ConsumerGroup:   req.ConsumerGroup,
		BatchSize:       req.BatchSize,
		BlockTimeout:    time.Duration(req.BlockTimeoutMs) * time.Millisecond,
		ConsumerTimeout: time.Duration(req.ConsumerTimeoutMs) * time.Millisecond,
		AutoAck:         req.AutoAck,
	}

	// Batch processing variables
	var lastProcessedID string = "0"

	// Pass the stream context to the subscription
	fmt.Printf("gRPC Server: Calling mq.SubscribeBatch with config: %+v\n", conf)

	// Create batch handler that processes entire batches
	batchHandler := func(ctx context.Context, topic string, messages []redis.XMessage) error {
		// Check if client is still connected
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fmt.Printf("gRPC Server: Received TRUE batch of %d messages\n", len(messages))

		// Convert entire batch to protobuf messages at once
		pbMessages := make([]*pb.Message, 0, len(messages))
		for _, msg := range messages {
			// Convert Redis message to protobuf message
			pbMsg := &pb.Message{
				Topic:  topic,
				Id:     msg.ID,
				Fields: toStruct(msg.Values),
			}
			pbMessages = append(pbMessages, pbMsg)
			lastProcessedID = msg.ID
		}

		// Send the entire batch at once
		fmt.Printf("gRPC Server: Sending TRUE batch of %d messages (last ID: %s)\n", len(pbMessages), lastProcessedID)
		if err := s.sendBatch(stream, pbMessages, lastProcessedID); err != nil {
			return err
		}

		return nil
	}

	// Start the subscription in a goroutine
	subscriptionDone := make(chan error, 1)
	go func() {
		err := s.mq.SubscribeBatch(conf, batchHandler)
		subscriptionDone <- err
	}()

	// Wait for either the subscription to complete or the client to disconnect
	select {
	case err := <-subscriptionDone:
		if err != nil {
			fmt.Printf("gRPC Server: mq.SubscribeBatch failed: %v\n", err)
			return err
		}
		fmt.Printf("gRPC Server: mq.SubscribeBatch succeeded\n")
	case <-ctx.Done():
		fmt.Printf("gRPC Server: Client disconnected, ending subscription\n")
		return nil
	}

	// Wait for the stream context to be done (client disconnects)
	<-ctx.Done()

	fmt.Printf("gRPC Server: Client disconnected, ending subscription\n")
	return nil
}

// sendBatch sends a batch of messages to the client
func (s *RedisStreamsServer) sendBatch(stream pb.RedisStreams_SubscribeServer, messages []*pb.Message, lastID string) error {
	if len(messages) == 0 {
		return nil
	}

	fmt.Printf("gRPC Server: Sending batch of %d messages (last ID: %s)\n", len(messages), lastID)

	// Send all messages in the batch
	for _, msg := range messages {
		if err := stream.Send(msg); err != nil {
			return fmt.Errorf("failed to send message in batch: %w", err)
		}
	}

	return nil
}

func (s *RedisStreamsServer) Ack(ctx context.Context, req *pb.AckRequest) (*pb.AckResponse, error) {
	if err := s.mq.AckMessage(req.Topic, req.ConsumerGroup, req.Id); err != nil {
		return nil, err
	}
	// ACK tracking is handled in mq.AckMessage()
	return &pb.AckResponse{}, nil
}

// AckBatch handles streaming ACK requests for high throughput
func (s *RedisStreamsServer) AckBatch(stream pb.RedisStreams_AckBatchServer) error {
	const batchSize = 500
	const flushInterval = 5 * time.Millisecond

	var acks []*pb.AckRequest
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	ctx := stream.Context()

	// Channel to receive ACK requests from stream
	ackChan := make(chan *pb.AckRequest, batchSize)
	errChan := make(chan error, 1)

	// Goroutine to receive ACK requests
	go func() {
		defer close(ackChan)
		for {
			req, err := stream.Recv()
			if err != nil {
				errChan <- err
				return
			}

			select {
			case ackChan <- req:
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			}
		}
	}()

	flush := func() error {
		if len(acks) == 0 {
			return nil
		}

		// Create a copy of ACK data for processing
		ackData := make([]struct {
			topic, consumerGroup, id string
		}, len(acks))

		for i, ack := range acks {
			ackData[i] = struct {
				topic, consumerGroup, id string
			}{
				topic:         ack.Topic,
				consumerGroup: ack.ConsumerGroup,
				id:            ack.Id,
			}
			// Return to pool immediately after copying data
			s.ackPool.Put(ack)
		}

		// Process ACKs
		select {
		case s.workerPool <- struct{}{}:
			go func() {
				defer func() { <-s.workerPool }()
				for _, data := range ackData {
					if err := s.mq.AckMessage(data.topic, data.consumerGroup, data.id); err != nil {
						// Log error - can't return in async context
						fmt.Printf("AckMessage error: %v\n", err)
					}
				}
			}()
		default:
			// Fallback to synchronous processing
			for _, data := range ackData {
				if err := s.mq.AckMessage(data.topic, data.consumerGroup, data.id); err != nil {
					return err
				}
			}
		}

		acks = acks[:0] // Reset slice but keep capacity
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return flush() // Final flush before shutdown

		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}

		case req, ok := <-ackChan:
			if !ok {
				// Channel closed, check for error
				if err := <-errChan; err != nil {
					// Final flush before returning error
					flush()
					return err
				}
				// Normal completion
				return flush()
			}

			acks = append(acks, req)

			// Flush if batch is full
			if len(acks) >= batchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

func (s *RedisStreamsServer) ListTopics(ctx context.Context, _ *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	names := s.mq.ListTopics()
	return &pb.ListTopicsResponse{Names: names}, nil
}

// ReadStream reads messages from a stream with optional limit and blocking
func (s *RedisStreamsServer) ReadStream(ctx context.Context, req *pb.ReadStreamRequest) (*pb.ReadStreamResponse, error) {
	topic, exists := s.mq.Topics[req.Topic]
	if !exists {
		return nil, fmt.Errorf("topic '%s' not configured", req.Topic)
	}
	var streamName string
	streamName = topic.StreamName
	if streamName == "" {
		// get from config.yml
		Temp, err := RSconfig.TopicConfigLoader("Config/config.yml")
		if err != nil {
			return nil, err
		}
		streamName = Temp.GetStreamName(req.Topic)
		if streamName == "" {
			return nil, fmt.Errorf("stream name not configured for topic '%s'", req.Topic)
		}
	}

	// Set defaults
	startID := req.StartId
	if startID == "" {
		startID = "0"
	}
	count := req.Count
	if count == 0 {
		count = 10000 // default limit
	}

	var messages []redis.XMessage
	var err error

	if req.BlockTimeoutMs > 0 {
		// Blocking read
		streams, err := s.client.XRead(ctx, &redis.XReadArgs{
			Streams: []string{streamName, startID},
			Count:   count,
			Block:   time.Duration(req.BlockTimeoutMs) * time.Millisecond,
		}).Result()
		if err != nil && err != redis.Nil {
			return nil, err
		}
		if len(streams) > 0 {
			messages = streams[0].Messages
		}
	} else {
		// Non-blocking read
		messages, err = s.client.XRange(ctx, streamName, startID, "+").Result()
		if err != nil {
			return nil, err
		}
		// Apply count limit
		if int64(len(messages)) > count {
			messages = messages[:count]
		}
	}

	// Convert to protobuf messages
	pbMessages := make([]*pb.Message, 0, len(messages))
	var lastID string

	for _, msg := range messages {
		pbMsg := &pb.Message{
			Topic:  req.Topic,
			Id:     msg.ID,
			Fields: toStruct(msg.Values),
		}
		pbMessages = append(pbMessages, pbMsg)
		lastID = msg.ID
	}

	hasMore := int64(len(messages)) == count

	return &pb.ReadStreamResponse{
		Messages: pbMessages,
		HasMore:  hasMore,
		LastId:   lastID,
	}, nil
}

// ReadRange reads messages from a stream within a specific range
func (s *RedisStreamsServer) ReadRange(ctx context.Context, req *pb.ReadRangeRequest) (*pb.ReadRangeResponse, error) {
	topic, exists := s.mq.Topics[req.Topic]
	if !exists {
		return nil, fmt.Errorf("topic '%s' not configured", req.Topic)
	}
	var streamName string
	streamName = topic.StreamName
	if streamName == "" {
		// get from config.yml
		topic_config, err := RSconfig.TopicConfigLoader("Config/config.yml")
		if err != nil {
			return nil, err
		}
		streamName = topic_config.GetStreamName(req.Topic)
		if streamName == "" {
			return nil, fmt.Errorf("stream name not configured for topic '%s'", req.Topic)
		}
	}

	startID := req.StartId
	if startID == "" {
		startID = "0"
	}
	endID := req.EndId
	if endID == "" {
		endID = "+"
	}

	messages, err := s.client.XRange(ctx, streamName, startID, endID).Result()
	if err != nil {
		return nil, err
	}

	// Apply count limit if specified
	if req.Count > 0 && int64(len(messages)) > req.Count {
		messages = messages[:req.Count]
	}

	// Convert to protobuf messages
	pbMessages := make([]*pb.Message, 0, len(messages))
	for _, msg := range messages {
		pbMsg := &pb.Message{
			Topic:  req.Topic,
			Id:     msg.ID,
			Fields: toStruct(msg.Values),
		}
		pbMessages = append(pbMessages, pbMsg)
	}

	return &pb.ReadRangeResponse{
		Messages: pbMessages,
	}, nil
}

// StreamInfo returns information about a stream
func (s *RedisStreamsServer) StreamInfo(ctx context.Context, req *pb.StreamInfoRequest) (*pb.StreamInfoResponse, error) {
	topic, exists := s.mq.Topics[req.Topic]
	if !exists {
		return nil, fmt.Errorf("topic '%s' not configured", req.Topic)
	}

	var streamName string
	streamName = topic.StreamName
	if streamName == "" {
		// get from config.yml
		topic_config, err := RSconfig.TopicConfigLoader("Config/config.yml")
		if err != nil {
			return nil, err
		}
		streamName = topic_config.GetStreamName(req.Topic)
		if streamName == "" {
			return nil, fmt.Errorf("stream name not configured for topic '%s'", req.Topic)
		}
	}

	info, err := s.client.XInfoStream(ctx, streamName).Result()
	if err != nil {
		return nil, err
	}

	return &pb.StreamInfoResponse{
		Length:       info.Length,
		FirstEntryId: info.FirstEntry.ID,
		LastEntryId:  info.LastEntry.ID,
		EntriesAdded: info.EntriesAdded,
		Groups:       info.Groups,
		Consumers:    s.getTotalConsumers(topic.StreamName),
	}, nil
}

// ConsumerGroupInfo returns information about consumer groups for a topic
func (s *RedisStreamsServer) ConsumerGroupInfo(ctx context.Context, req *pb.ConsumerGroupInfoRequest) (*pb.ConsumerGroupInfoResponse, error) {
	topic, exists := s.mq.Topics[req.Topic]
	if !exists {
		return nil, fmt.Errorf("topic '%s' not configured", req.Topic)
	}

	var streamName string
	streamName = topic.StreamName
	if streamName == "" {
		// get from config.yml
		topic_config, err := RSconfig.TopicConfigLoader("Config/config.yml")
		if err != nil {
			return nil, err
		}
		streamName = topic_config.GetStreamName(req.Topic)
		if streamName == "" {
			return nil, fmt.Errorf("stream name not configured for topic '%s'", req.Topic)
		}
	}

	// Get detailed group information
	groupInfos, err := s.client.XInfoGroups(ctx, streamName).Result()
	if err != nil {
		return nil, err
	}

	groups := make([]*pb.ConsumerGroupInfo, 0, len(groupInfos))
	for _, group := range groupInfos {
		groups = append(groups, &pb.ConsumerGroupInfo{
			Name:            group.Name,
			Consumers:       group.Consumers,
			Pending:         group.Pending,
			LastDeliveredId: group.LastDeliveredID,
		})
	}

	return &pb.ConsumerGroupInfoResponse{
		Groups: groups,
	}, nil
}

// CreateConsumerGroup creates a new consumer group
func (s *RedisStreamsServer) CreateConsumerGroup(ctx context.Context, req *pb.CreateConsumerGroupRequest) (*pb.CreateConsumerGroupResponse, error) {
	topic, exists := s.mq.Topics[req.Topic]
	if !exists {
		return nil, fmt.Errorf("topic '%s' not configured", req.Topic)
	}

	var streamName string
	streamName = topic.StreamName
	if streamName == "" {
		// get from config.yml
		topic_config, err := RSconfig.TopicConfigLoader("Config/config.yml")
		if err != nil {
			return nil, err
		}
		streamName = topic_config.GetStreamName(req.Topic)
		if streamName == "" {
			return nil, fmt.Errorf("stream name not configured for topic '%s'", req.Topic)
		}
	}

	startID := req.StartId
	if startID == "" {
		startID = "$"
	}

	err := s.client.XGroupCreateMkStream(ctx, streamName, req.GroupName, startID).Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return nil, err
	}

	return &pb.CreateConsumerGroupResponse{}, nil
}

// DeleteConsumerGroup deletes a consumer group
func (s *RedisStreamsServer) DeleteConsumerGroup(ctx context.Context, req *pb.DeleteConsumerGroupRequest) (*pb.DeleteConsumerGroupResponse, error) {
	topic, exists := s.mq.Topics[req.Topic]
	if !exists {
		return nil, fmt.Errorf("topic '%s' not configured", req.Topic)
	}

	var streamName string
	streamName = topic.StreamName
	if streamName == "" {
		// get from config.yml
		topic_config, err := RSconfig.TopicConfigLoader("Config/config.yml")
		if err != nil {
			return nil, err
		}
		streamName = topic_config.GetStreamName(req.Topic)
		if streamName == "" {
			return nil, fmt.Errorf("stream name not configured for topic '%s'", req.Topic)
		}
	}

	err := s.client.XGroupDestroy(ctx, streamName, req.GroupName).Err()
	if err != nil {
		return nil, err
	}

	return &pb.DeleteConsumerGroupResponse{}, nil
}

// Helper function to get total consumers across all groups
func (s *RedisStreamsServer) getTotalConsumers(streamName string) int64 {
	ctx := context.Background()
	groupInfos, err := s.client.XInfoGroups(ctx, streamName).Result()
	if err != nil {
		return 0
	}

	total := int64(0)
	for _, group := range groupInfos {
		total += group.Consumers
	}
	return total
}

// toStruct converts a map to protobuf Struct
func toStruct(m map[string]interface{}) *structpb.Struct {
	if m == nil {
		return &structpb.Struct{Fields: make(map[string]*structpb.Value)}
	}

	s, err := structpb.NewStruct(m)
	if err != nil {
		fmt.Printf("Error converting map to struct: %v\n", err)
		return &structpb.Struct{Fields: make(map[string]*structpb.Value)}
	}
	return s
}
