package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"google.golang.org/protobuf/types/known/structpb"

	pb "MRETest/RedisStreams/api/proto"
)

type LookupRequest struct {
	QueryID    string            `json:"query_id"`
	UserID     string            `json:"user_id"`
	LookupType string            `json:"lookup_type"`
	Fields     map[string]string `json:"fields"`
	Timestamp  string            `json:"timestamp"`
}

type LookupClient struct {
	client pb.RedisStreamsClient
	conn   *grpc.ClientConn
}

func NewLookupClient(serverAddr string) (*LookupClient, error) {
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC server: %v", err)
	}

	return &LookupClient{
		client: pb.NewRedisStreamsClient(conn),
		conn:   conn,
	}, nil
}

func (c *LookupClient) Close() error {
	return c.conn.Close()
}

func (c *LookupClient) PublishLookupRequest(req LookupRequest) (string, error) {
	// Convert the fields map to map[string]interface{} for protobuf
	fieldsMap := make(map[string]interface{}, len(req.Fields))
	for k, v := range req.Fields {
		fieldsMap[k] = v
	}

	// Create the protobuf struct with the converted map
	fields, err := structpb.NewStruct(map[string]interface{}{
		"query_id":    req.QueryID,
		"user_id":     req.UserID,
		"lookup_type": req.LookupType,
		"fields":      fieldsMap,
		"timestamp":   req.Timestamp,
	})
	if err != nil {
		return "", fmt.Errorf("failed to create struct: %v", err)
	}

	// Create and send the request
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.client.Publish(ctx, &pb.PublishRequest{
		Topic:   "user.lookup",
		Json:    fields,
		Headers: map[string]string{"source": "mre_lookup"},
	})
	if err != nil {
		return "", fmt.Errorf("failed to publish message: %v", err)
	}

	return resp.Id, nil
}

func generateTestRequests(count int) []LookupRequest {
	reqs := make([]LookupRequest, 0, count)
	now := time.Now().Format(time.RFC3339)

	for i := 0; i < count; i++ {
		req := LookupRequest{
			QueryID:    fmt.Sprintf("qry_%d_%d", time.Now().UnixNano(), i),
			UserID:     fmt.Sprintf("user_%d", 1000+i%10), // 10 different users
			LookupType: []string{"email", "phone", "device"}[i%3],
			Timestamp:  now,
			Fields: map[string]string{
				"email":  fmt.Sprintf("user%d@example.com", i%100),
				"phone":  fmt.Sprintf("+1%09d", 1000000000+i%1000000),
				"device": fmt.Sprintf("device_%d", i%5),
			},
		}
		reqs = append(reqs, req)
	}

	return reqs
}

func main() {
	serverAddr := flag.String("server", "localhost:16001", "gRPC server address")
	messageCount := flag.Int("count", 10, "Number of test messages to send")
	flag.Parse()

	// Initialize client
	client, err := NewLookupClient(*serverAddr)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Generate test requests
	reqs := generateTestRequests(*messageCount)

	// Publish messages concurrently
	var wg sync.WaitGroup
	errChan := make(chan error, len(reqs))

	for i, req := range reqs {
		wg.Add(1)
		go func(idx int, r LookupRequest) {
			defer wg.Done()

			// Add some delay between messages
			time.Sleep(time.Duration(idx%10) * 10 * time.Millisecond)

			// Publish the message
			id, err := client.PublishLookupRequest(r)
			if err != nil {
				errChan <- fmt.Errorf("error in request %d: %v", idx, err)
				return
			}

			log.Printf("Published message %d: %s - %s", idx, r.QueryID, id)
		}(i, req)

		// Check for termination signal
		select {
		case sig := <-sigChan:
			log.Printf("Received signal: %v. Shutting down...", sig)
			wg.Wait()
			close(errChan)
			return
		default:
			// Continue processing
		}
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(errChan)

	// Log any errors
	errCount := 0
	for err := range errChan {
		errCount++
		log.Printf("Error: %v", err)
	}

	if errCount > 0 {
		log.Printf("Completed with %d errors", errCount)
	} else {
		log.Printf("Successfully published %d messages", len(reqs))
	}
}
