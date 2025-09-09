package server

import (
	RSconfig "RedisStreams/Config"
	mq "RedisStreams/QueueModule"
	pb "RedisStreams/api/proto"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	structpb "google.golang.org/protobuf/types/known/structpb"
)

type RedisStreamsServer struct {
	pb.UnimplementedRedisStreamsServer
	mq     *mq.RedisStreamMQ
	config *RSconfig.Config
	client *redis.Client
}

func NewRedisStreamsServer(mqClient *mq.RedisStreamMQ, cfg *RSconfig.Config) *RedisStreamsServer {
	// Get Redis client from MQ for direct operations
	client := mqClient.GetRedisClient()
	return &RedisStreamsServer{
		mq:     mqClient,
		config: cfg,
		client: client,
	}
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

func (s *RedisStreamsServer) Subscribe(req *pb.SubscribeRequest, stream pb.RedisStreams_SubscribeServer) error {
	conf := mq.ConsumerConfig{
		TopicName:       req.Topic,
		ConsumerName:    req.ConsumerName,
		BatchSize:       req.BatchSize,
		BlockTimeout:    time.Duration(req.BlockTimeoutMs) * time.Millisecond,
		ConsumerTimeout: time.Duration(req.ConsumerTimeoutMs) * time.Millisecond,
		AutoAck:         req.AutoAck,
	}

	handler := func(ctx context.Context, topic, id string, fields map[string]interface{}) error {
		msg := &pb.Message{Topic: topic, Id: id}
		// Convert fields to Struct
		m := make(map[string]interface{})
		for k, v := range fields {
			m[k] = v
		}
		msg.Fields = toStruct(m)
		return stream.Send(msg)
	}

	return s.mq.Subscribe(conf, handler)
}

func (s *RedisStreamsServer) Ack(ctx context.Context, req *pb.AckRequest) (*pb.AckResponse, error) {
	if err := s.mq.AckMessage(req.Topic, req.ConsumerGroup, req.Id); err != nil {
		return nil, err
	}
	return &pb.AckResponse{}, nil
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
