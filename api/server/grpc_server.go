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
				Fields: &structpb.Struct{},
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

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		// Process batch in parallel
		select {
		case s.workerPool <- struct{}{}:
			go func(msgs []mq.BatchMessage) {
				defer func() { <-s.workerPool }()
				s.mq.PublishBatch(msgs)
			}(batch)
		default:
			// Fallback to synchronous processing
			if err := s.mq.PublishBatch(batch); err != nil {
				return err
			}
		}

		batch = batch[:0] // Reset slice but keep capacity
		return nil
	}

	for {
		select {
		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}
		default:
			req, err := stream.Recv()
			if err != nil {
				// Flush remaining messages before returning
				if err := flush(); err != nil {
					return err
				}
				return err
			}

			// Convert to batch message
			fields := req.Json.AsMap()
			batch = append(batch, mq.BatchMessage{
				Topic:  req.Topic,
				ID:     "*", // Auto-generate ID
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
	conf := mq.ConsumerConfig{
		TopicName:       req.Topic,
		ConsumerName:    req.ConsumerName,
		BatchSize:       req.BatchSize,
		BlockTimeout:    time.Duration(req.BlockTimeoutMs) * time.Millisecond,
		ConsumerTimeout: time.Duration(req.ConsumerTimeoutMs) * time.Millisecond,
		AutoAck:         req.AutoAck,
	}

	// Use message pool for better memory efficiency
	handler := func(ctx context.Context, topic, id string, fields map[string]interface{}) error {
		msg := s.messagePool.Get().(*pb.Message)
		msg.Topic = topic
		msg.Id = id
		msg.Fields = toStruct(fields)

		err := stream.Send(msg)

		// Return to pool
		s.messagePool.Put(msg)
		return err
	}

	return s.mq.Subscribe(conf, handler)
}

func (s *RedisStreamsServer) Ack(ctx context.Context, req *pb.AckRequest) (*pb.AckResponse, error) {
	if err := s.mq.AckMessage(req.Topic, req.ConsumerGroup, req.Id); err != nil {
		return nil, err
	}
	return &pb.AckResponse{}, nil
}

// AckBatch handles streaming ACK requests for high throughput
func (s *RedisStreamsServer) AckBatch(stream pb.RedisStreams_AckBatchServer) error {
	const batchSize = 500
	const flushInterval = 5 * time.Millisecond

	var acks []*pb.AckRequest
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	flush := func() error {
		if len(acks) == 0 {
			return nil
		}

		// Process ACKs in parallel
		select {
		case s.workerPool <- struct{}{}:
			go func(ackList []*pb.AckRequest) {
				defer func() { <-s.workerPool }()
				for _, ack := range ackList {
					s.mq.AckMessage(ack.Topic, ack.ConsumerGroup, ack.Id)
					// Return to pool
					s.ackPool.Put(ack)
				}
			}(acks)
		default:
			// Fallback to synchronous processing
			for _, ack := range acks {
				if err := s.mq.AckMessage(ack.Topic, ack.ConsumerGroup, ack.Id); err != nil {
					// Return to pool
					s.ackPool.Put(ack)
					return err
				}
				s.ackPool.Put(ack)
			}
		}

		acks = acks[:0] // Reset slice but keep capacity
		return nil
	}

	for {
		select {
		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}
		default:
			req, err := stream.Recv()
			if err != nil {
				// Flush remaining ACKs before returning
				if err := flush(); err != nil {
					return err
				}
				return err
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

	streamName := topic.StreamName
	if streamName == "" {
		streamName = fmt.Sprintf("stream:%s", req.Topic)
	}

	// Set defaults
	startID := req.StartId
	if startID == "" {
		startID = "0"
	}
	count := req.Count
	if count == 0 {
		count = 100 // default limit
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

	streamName := topic.StreamName
	if streamName == "" {
		streamName = fmt.Sprintf("stream:%s", req.Topic)
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

	streamName := topic.StreamName
	if streamName == "" {
		streamName = fmt.Sprintf("stream:%s", req.Topic)
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
		Consumers:    s.getTotalConsumers(streamName),
	}, nil
}

// ConsumerGroupInfo returns information about consumer groups for a topic
func (s *RedisStreamsServer) ConsumerGroupInfo(ctx context.Context, req *pb.ConsumerGroupInfoRequest) (*pb.ConsumerGroupInfoResponse, error) {
	topic, exists := s.mq.Topics[req.Topic]
	if !exists {
		return nil, fmt.Errorf("topic '%s' not configured", req.Topic)
	}

	streamName := topic.StreamName
	if streamName == "" {
		streamName = fmt.Sprintf("stream:%s", req.Topic)
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

	streamName := topic.StreamName
	if streamName == "" {
		streamName = fmt.Sprintf("stream:%s", req.Topic)
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

	streamName := topic.StreamName
	if streamName == "" {
		streamName = fmt.Sprintf("stream:%s", req.Topic)
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
	s, _ := structpb.NewStruct(m)
	return s
}
