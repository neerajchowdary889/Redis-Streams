package MRE

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "MRETest/RedisStreams/api/proto"
)

type Result struct {
	Epoch    uint64  // epoch used for the placement
	Primary  int16   // shard ID of the primary node
	Replicas []int16 // list of replica shard IDs (length = Replicas-1, may be empty)
	All      []int16 // combined list of [primary] + replicas
}

type LookupRequest struct {
	QueryID    string            `json:"query_id"`
	UserID     string            `json:"user_id"`
	LookupType string            `json:"lookup_type"`
	Fields     map[string]string `json:"fields"`
	Timestamp  string            `json:"timestamp"`
	Result     *Result           `json:"result,omitempty"`
}

type LookupConsumer struct {
	client    pb.RedisStreamsClient
	conn      *grpc.ClientConn
	batchSize int
	mu        sync.Mutex
}

func NewLookupConsumer(serverAddr string) (*LookupConsumer, error) {
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC server: %v", err)
	}

	return &LookupConsumer{
		client:    pb.NewRedisStreamsClient(conn),
		conn:      conn,
		batchSize: 50, // Process 50 entries in a batch
	}, nil
}

func (c *LookupConsumer) Close() error {
	return c.conn.Close()
}

// Parse comma-separated string back to []int16
func parseInt16Slice(str string) []int16 {
	if str == "" {
		return []int16{}
	}

	parts := strings.Split(str, ",")
	result := make([]int16, 0, len(parts))

	for _, part := range parts {
		val, err := strconv.ParseInt(strings.TrimSpace(part), 10, 16)
		if err != nil {
			log.Printf("Warning: failed to parse int16 from '%s': %v", part, err)
			continue
		}
		result = append(result, int16(val))
	}

	return result
}

// Parse result data from the message
func parseResultData(resultData map[string]interface{}) *Result {
	if resultData == nil {
		return nil
	}

	// Parse primary
	primaryStr, ok := resultData["primary"].(string)
	if !ok {
		return nil
	}
	primary, err := strconv.ParseInt(primaryStr, 10, 16)
	if err != nil {
		log.Printf("Warning: failed to parse primary: %v", err)
		return nil
	}

	// Parse replicas
	replicasStr, ok := resultData["replicas"].(string)
	if !ok {
		replicasStr = ""
	}
	replicas := parseInt16Slice(replicasStr)

	// Parse all
	allStr, ok := resultData["all"].(string)
	if !ok {
		allStr = ""
	}
	all := parseInt16Slice(allStr)

	// Parse epoch
	epoch, ok := resultData["epoch"].(float64)
	if !ok {
		epoch = 0
	}

	return &Result{
		Epoch:    uint64(epoch),
		Primary:  int16(primary),
		Replicas: replicas,
		All:      all,
	}
}

func (c *LookupConsumer) processMessage(message *pb.Message) {
	log.Printf("=== Processing Message ===")
	log.Printf("ID: %s", message.Id)
	log.Printf("Topic: %s", message.Topic)

	// Parse JSON struct from protobuf
	jsonData := message.Fields.AsMap()
	log.Printf("Raw JSON: %+v", jsonData) // <-- Add this for debugging

	// Extract fields
	queryID, _ := jsonData["query_id"].(string)
	userID, _ := jsonData["user_id"].(string)
	lookupType, _ := jsonData["lookup_type"].(string)
	timestamp, _ := jsonData["timestamp"].(string)

	// Handle "fields" map
	fieldsMap := make(map[string]string)
	if fields, ok := jsonData["fields"].(map[string]interface{}); ok {
		for k, v := range fields {
			if str, ok := v.(string); ok {
				fieldsMap[k] = str
			}
		}
	}

	// Handle "result"
	var result *Result
	if resultData, ok := jsonData["result"].(map[string]interface{}); ok {
		result = parseResultData(resultData)
	}

	req := LookupRequest{
		QueryID:    queryID,
		UserID:     userID,
		LookupType: lookupType,
		Fields:     fieldsMap,
		Timestamp:  timestamp,
		Result:     result,
	}

	log.Printf("Query ID: %s", req.QueryID)
	log.Printf("User ID: %s", req.UserID)
	log.Printf("Lookup Type: %s", req.LookupType)
	log.Printf("Fields: %+v", req.Fields)
	log.Printf("Timestamp: %s", req.Timestamp)

	if req.Result != nil {
		log.Printf("Result - Epoch: %d, Primary: %d, Replicas: %v, All: %v",
			req.Result.Epoch, req.Result.Primary, req.Result.Replicas, req.Result.All)
	} else {
		log.Printf("Result: nil")
	}

	log.Printf("========================\n")
}

// Consume messages from a stream
func (c *LookupConsumer) ConsumeStream(streamName string, consumerGroup string, consumerName string, count int32) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	log.Printf("Starting to consume from stream: %s", streamName)
	log.Printf("Consumer Group: %s, Consumer: %s", consumerGroup, consumerName)

	// Read messages from the stream
	resp, err := c.client.ReadStream(ctx, &pb.ReadStreamRequest{
		Topic:   streamName,
		Count:   int64(count),
		StartId: "0", // Start from the beginning
	})
	if err != nil {
		return fmt.Errorf("failed to read stream: %v", err)
	}

	log.Printf("Found %d messages in stream", len(resp.Messages))

	// Process each message
	for _, message := range resp.Messages {
		c.processMessage(message)

		// Acknowledge the message to delete it
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := c.client.Ack(ctx, &pb.AckRequest{
			Topic: streamName,
			Id:    message.Id,
		})
		cancel()

		if err != nil {
			log.Printf("Warning: failed to ACK message %s: %v", message.Id, err)
		} else {
			log.Printf("ACKed message: %s", message.Id)
		}
	}

	return nil
}

// Subscribe to stream for real-time consumption
func (c *LookupConsumer) SubscribeToStream(streamName string, consumerGroup string, consumerName string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf("Subscribing to stream: %s", streamName)
	log.Printf("Consumer Group: %s, Consumer: %s", consumerGroup, consumerName)

	// Create subscription request
	req := &pb.SubscribeRequest{
		Topic:             streamName,
		ConsumerName:      consumerGroup,
		BatchSize:         10,
		BlockTimeoutMs:    5000, // 5 seconds
		ConsumerTimeoutMs: 5000, // 5 seconds
		AutoAck:           true,
	}

	// Start streaming
	stream, err := c.client.Subscribe(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to subscribe to stream: %v", err)
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received shutdown signal, stopping consumer...")
		cancel()
	}()

	// Process messages as they arrive
	for {
		select {
		case <-ctx.Done():
			log.Println("Context cancelled, stopping consumer")
			return nil
		default:
			// Receive message
			message, err := stream.Recv()
			if err != nil {
				if err.Error() == "EOF" {
					log.Println("Stream ended")
					return nil
				}
				log.Printf("Error receiving message: %v", err)
				continue
			}

			// Process the message
			c.processMessage(message)

			// Acknowledge the message to delete it
			ackCtx, ackCancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err = c.client.Ack(ackCtx, &pb.AckRequest{
				Topic: streamName,
				Id:    message.Id,
			})
			ackCancel()

			if err != nil {
				log.Printf("Warning: failed to ACK message %s: %v", message.Id, err)
			} else {
				log.Printf("ACKed message: %s", message.Id)
			}
		}
	}
}

// StartBatchConsumption starts batch consumption with specified parameters
func (c *LookupConsumer) StartBatchConsumption(streamName, consumerGroup, consumerName string, count int32) error {
	log.Printf("Starting batch consumption from stream: %s", streamName)
	return c.ConsumeStream(streamName, consumerGroup, consumerName, count)
}

// StartStreamConsumption starts continuous stream consumption
func (c *LookupConsumer) StartStreamConsumption(streamName, consumerGroup, consumerName string) error {
	log.Printf("Starting stream consumption from: %s", streamName)
	return c.SubscribeToStream(streamName, consumerGroup, consumerName)
}

// StartBatchStreamConsumption starts batch stream consumption
func (c *LookupConsumer) StartBatchStreamConsumption(streamName, consumerGroup, consumerName string) error {
	log.Printf("Starting batch stream consumption from: %s", streamName)
	return c.SubscribeToStreamWithBatching(streamName, consumerGroup, consumerName)
}

// SetBatchSize sets the batch size for processing
func (c *LookupConsumer) SetBatchSize(size int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.batchSize = size
}

// GetBatchSize returns the current batch size
func (c *LookupConsumer) GetBatchSize() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.batchSize
}

// processBatch processes a batch of messages
func (c *LookupConsumer) processBatch(messages []*pb.Message, streamName string) error {
	if len(messages) == 0 {
		return nil
	}

	var messageIDs []string

	// Process each message in the batch
	for _, msg := range messages {
		c.processMessage(msg)
		messageIDs = append(messageIDs, msg.Id)

		// Log progress
		if len(messageIDs)%10 == 0 {
			log.Printf("Processed %d/%d messages in batch", len(messageIDs), len(messages))
		}
	}

	// Acknowledge all processed messages
	if len(messageIDs) > 0 {
		for _, msgID := range messageIDs {
			ackCtx, ackCancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := c.client.Ack(ackCtx, &pb.AckRequest{
				Topic: streamName,
				Id:    msgID,
			})
			ackCancel()

			if err != nil {
				log.Printf("Warning: failed to ACK message %s: %v", msgID, err)
			}
		}
		log.Printf("Acknowledged %d messages", len(messageIDs))
	}

	return nil
}

// SubscribeToStreamWithBatching - Enhanced version with batch processing
func (c *LookupConsumer) SubscribeToStreamWithBatching(streamName string, consumerGroup string, consumerName string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf("Subscribing to stream: %s with batch size: %d", streamName, c.batchSize)
	log.Printf("Consumer Group: %s, Consumer: %s", consumerGroup, consumerName)

	// Create a buffered channel for messages
	messageChan := make(chan *pb.Message, c.batchSize*2)
	errChan := make(chan error, 1)

	// Start a goroutine to read messages from the stream
	go func() {
		req := &pb.SubscribeRequest{
			Topic:             streamName,
			ConsumerName:      consumerGroup,
			BatchSize:         int64(c.batchSize),
			BlockTimeoutMs:    5000,  // 5 seconds
			ConsumerTimeoutMs: 5000,  // 5 seconds
			AutoAck:           false, // We'll handle ACKs manually
		}

		stream, err := c.client.Subscribe(ctx, req)
		if err != nil {
			errChan <- fmt.Errorf("failed to subscribe to stream: %v", err)
			return
		}

		for {
			select {
			case <-ctx.Done():
				return
			default:
				msg, err := stream.Recv()
				if err != nil {
					if err.Error() == "EOF" {
						log.Println("Stream ended")
						return
					}
					log.Printf("Error receiving message: %v", err)
					continue
				}
				messageChan <- msg
			}
		}
	}()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received shutdown signal, stopping consumer...")
		cancel()
	}()

	// Process messages in batches
	batch := make([]*pb.Message, 0, c.batchSize)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Context cancelled, stopping consumer")
			return nil

		case err := <-errChan:
			return fmt.Errorf("error in stream subscription: %v", err)

		case msg := <-messageChan:
			batch = append(batch, msg)
			if len(batch) >= c.batchSize {
				// Process the current batch
				if err := c.processBatch(batch, streamName); err != nil {
					log.Printf("Error processing batch: %v", err)
				}
				// Reset the batch
				batch = make([]*pb.Message, 0, c.batchSize)
			}

		case <-ticker.C:
			// Process any pending messages every second (for low traffic streams)
			if len(batch) > 0 {
				log.Printf("Processing partial batch of %d messages", len(batch))
				if err := c.processBatch(batch, streamName); err != nil {
					log.Printf("Error processing batch: %v", err)
				}
				batch = make([]*pb.Message, 0, c.batchSize)
			}
		}
	}
}
