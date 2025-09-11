package MRE

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"google.golang.org/protobuf/types/known/structpb"

	pb "MRETest/RedisStreams/api/proto"
)

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

	// Prepare the message data
	messageData := map[string]interface{}{
		"query_id":    req.QueryID,
		"user_id":     req.UserID,
		"lookup_type": req.LookupType,
		"fields":      fieldsMap,
		"timestamp":   req.Timestamp,
	}

	// Add result data if present
	if req.Result != nil {
		// Convert []int16 to comma-separated strings for protobuf compatibility
		replicasStr := ""
		for i, v := range req.Result.Replicas {
			if i > 0 {
				replicasStr += ","
			}
			replicasStr += fmt.Sprintf("%d", v)
		}

		allStr := ""
		for i, v := range req.Result.All {
			if i > 0 {
				allStr += ","
			}
			allStr += fmt.Sprintf("%d", v)
		}

		resultData := map[string]interface{}{
			"epoch":    req.Result.Epoch,
			"primary":  fmt.Sprintf("%d", req.Result.Primary),
			"replicas": replicasStr,
			"all":      allStr,
		}
		messageData["result"] = resultData
	}

	// Create the protobuf struct with the converted map
	fields, err := structpb.NewStruct(messageData)
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
		// Generate a mock result for some requests (every 3rd request)
		var result *Result
		if i%3 == 0 {
			replicas := []int16{int16(1 + i%3), int16(2 + i%3)}
			all := append([]int16{int16(i % 5)}, replicas...)

			result = &Result{
				Epoch:    uint64(time.Now().Unix()),
				Primary:  int16(i % 5),
				Replicas: replicas,
				All:      all,
			}
		}

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
			Result: result,
		}
		reqs = append(reqs, req)
	}

	return reqs
}

// PublishBatch publishes multiple lookup requests in a single batch call
func (c *LookupClient) PublishBatch(requests []LookupRequest) (int, error) {
	if len(requests) == 0 {
		return 0, nil
	}

	// Prepare batch messages
	messages := make([]*pb.BatchMessage, 0, len(requests))

	for _, req := range requests {
		// Convert the fields map to map[string]interface{} for protobuf
		fieldsMap := make(map[string]interface{}, len(req.Fields))
		for k, v := range req.Fields {
			fieldsMap[k] = v
		}

		// Prepare the message data
		messageData := map[string]interface{}{
			"query_id":    req.QueryID,
			"user_id":     req.UserID,
			"lookup_type": req.LookupType,
			"fields":      fieldsMap,
			"timestamp":   req.Timestamp,
		}

		// Add result data if present
		if req.Result != nil {
			// Convert []int16 to comma-separated strings for protobuf compatibility
			replicasStr := ""
			for i, v := range req.Result.Replicas {
				if i > 0 {
					replicasStr += ","
				}
				replicasStr += fmt.Sprintf("%d", v)
			}

			allStr := ""
			for i, v := range req.Result.All {
				if i > 0 {
					allStr += ","
				}
				allStr += fmt.Sprintf("%d", v)
			}

			resultData := map[string]interface{}{
				"epoch":    req.Result.Epoch,
				"primary":  fmt.Sprintf("%d", req.Result.Primary),
				"replicas": replicasStr,
				"all":      allStr,
			}
			messageData["result"] = resultData
		}

		// Create the protobuf struct with the converted map
		fields, err := structpb.NewStruct(messageData)
		if err != nil {
			return 0, fmt.Errorf("failed to create struct for request %s: %v", req.QueryID, err)
		}

		// Create batch message
		batchMsg := &pb.BatchMessage{
			Topic:  "user.lookup",
			Fields: fields,
		}

		messages = append(messages, batchMsg)
	}

	// Create and send the batch request
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := c.client.PublishBatch(ctx, &pb.PublishBatchRequest{
		Messages: messages,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to publish batch: %v", err)
	}

	return len(requests), nil
}

// PublishBatchSequential publishes multiple lookup requests one by one (fallback method)
func (c *LookupClient) PublishBatchSequential(requests []LookupRequest) ([]string, error) {
	ids := make([]string, 0, len(requests))

	for _, req := range requests {
		id, err := c.PublishLookupRequest(req)
		if err != nil {
			return ids, fmt.Errorf("failed to publish request %s: %v", req.QueryID, err)
		}
		ids = append(ids, id)
	}

	return ids, nil
}

// PublishSingle publishes a single lookup request
func (c *LookupClient) PublishSingle(queryID, userID, lookupType string, fields map[string]string, result *Result) (string, error) {
	req := LookupRequest{
		QueryID:    queryID,
		UserID:     userID,
		LookupType: lookupType,
		Fields:     fields,
		Timestamp:  time.Now().Format(time.RFC3339),
		Result:     result,
	}

	return c.PublishLookupRequest(req)
}
