package server

import (
	RSconfig "RedisStreams/Config"
	mq "RedisStreams/QueueModule"
	pb "RedisStreams/api/proto"
	"context"
	"errors"
	"time"

	structpb "google.golang.org/protobuf/types/known/structpb"
)

type RedisStreamsServer struct {
	pb.UnimplementedRedisStreamsServer
	mq     *mq.RedisStreamMQ
	config *RSconfig.Config
}

func NewRedisStreamsServer(mqClient *mq.RedisStreamMQ, cfg *RSconfig.Config) *RedisStreamsServer {
	return &RedisStreamsServer{mq: mqClient, config: cfg}
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

// toStruct converts a map to protobuf Struct
func toStruct(m map[string]interface{}) *structpb.Struct {
	s, _ := structpb.NewStruct(m)
	return s
}
