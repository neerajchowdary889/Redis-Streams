package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"

	pb "RedisStreams/api/proto"
)

func main() {
	// Connect to gRPC server
	conn, err := grpc.Dial("localhost:16001", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewRedisStreamsClient(conn)

	// Test different performance scenarios
	fmt.Println("🚀 Performance Testing Started")

	// Test 1: Batch Publishing (should get 160-240k TPS)
	testBatchPublishing(client)

	// Test 2: Streaming Publishing (should get 240-400k TPS)
	testStreamingPublishing(client)

	// Test 3: Parallel Publishing (should get 200-300k TPS)
	testParallelPublishing(client)

	// Test 4: Streaming ACK (should improve consumer performance)
	testStreamingACK(client)
}

func testBatchPublishing(client pb.RedisStreamsClient) {
	fmt.Println("\n📦 Testing Batch Publishing...")

	const batchSize = 1000
	const numBatches = 100

	start := time.Now()

	for i := 0; i < numBatches; i++ {
		messages := make([]*pb.BatchMessage, batchSize)
		for j := 0; j < batchSize; j++ {
			// Create a simple struct for the message
			messageData := map[string]interface{}{
				"query_id":    fmt.Sprintf("batch_%d_%d", i, j),
				"user_id":     fmt.Sprintf("user_%d", j),
				"lookup_type": "email",
				"timestamp":   time.Now().Format(time.RFC3339),
			}

			fields, _ := structpb.NewStruct(messageData)
			messages[j] = &pb.BatchMessage{
				Topic:  "user.lookup",
				Fields: fields,
			}
		}

		_, err := client.PublishBatch(context.Background(), &pb.PublishBatchRequest{
			Messages: messages,
		})
		if err != nil {
			log.Printf("Batch %d failed: %v", i, err)
		}
	}

	elapsed := time.Since(start)
	totalMessages := int64(batchSize * numBatches)
	tps := float64(totalMessages) / elapsed.Seconds()

	fmt.Printf("✅ Batch Publishing: %d messages in %v (%.0f TPS)\n",
		totalMessages, elapsed, tps)
}

func testStreamingPublishing(client pb.RedisStreamsClient) {
	fmt.Println("\n🌊 Testing Streaming Publishing...")

	const totalMessages = 100000

	start := time.Now()

	stream, err := client.PublishStream(context.Background())
	if err != nil {
		log.Fatalf("Failed to create stream: %v", err)
	}

	// Send messages in streaming fashion
	for i := 0; i < totalMessages; i++ {
		messageData := map[string]interface{}{
			"query_id":    fmt.Sprintf("stream_%d", i),
			"user_id":     fmt.Sprintf("user_%d", i),
			"lookup_type": "email",
			"timestamp":   time.Now().Format(time.RFC3339),
		}

		fields, _ := structpb.NewStruct(messageData)
		req := &pb.PublishRequest{
			Topic: "user.lookup",
			Json:  fields,
		}

		if err := stream.Send(req); err != nil {
			log.Printf("Stream send failed: %v", err)
			break
		}
	}

	stream.CloseAndRecv()

	elapsed := time.Since(start)
	tps := float64(totalMessages) / elapsed.Seconds()

	fmt.Printf("✅ Streaming Publishing: %d messages in %v (%.0f TPS)\n",
		totalMessages, elapsed, tps)
}

func testParallelPublishing(client pb.RedisStreamsClient) {
	fmt.Println("\n⚡ Testing Parallel Publishing...")

	const numWorkers = 8
	const messagesPerWorker = 12500

	start := time.Now()

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Create dedicated connection for each worker
			conn, err := grpc.Dial("localhost:16001", grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Worker %d failed to connect: %v", workerID, err)
				return
			}
			defer conn.Close()

			workerClient := pb.NewRedisStreamsClient(conn)

			// Use batching for better performance
			const batchSize = 100
			for i := 0; i < messagesPerWorker; i += batchSize {
				end := i + batchSize
				if end > messagesPerWorker {
					end = messagesPerWorker
				}

				// Create batch
				messages := make([]*pb.BatchMessage, end-i)
				for j := i; j < end; j++ {
					messageData := map[string]interface{}{
						"query_id":    fmt.Sprintf("parallel_%d_%d", workerID, j),
						"user_id":     fmt.Sprintf("user_%d", j),
						"lookup_type": "email",
						"timestamp":   time.Now().Format(time.RFC3339),
					}

					fields, _ := structpb.NewStruct(messageData)
					messages[j-i] = &pb.BatchMessage{
						Topic:  "user.lookup",
						Fields: fields,
					}
				}

				_, err := workerClient.PublishBatch(context.Background(), &pb.PublishBatchRequest{
					Messages: messages,
				})
				if err != nil {
					log.Printf("Worker %d batch failed: %v", workerID, err)
				}
			}
		}(w)
	}

	wg.Wait()

	elapsed := time.Since(start)
	totalMessages := int64(numWorkers * messagesPerWorker)
	tps := float64(totalMessages) / elapsed.Seconds()

	fmt.Printf("✅ Parallel Publishing: %d messages in %v (%.0f TPS)\n",
		totalMessages, elapsed, tps)
}

func testStreamingACK(client pb.RedisStreamsClient) {
	fmt.Println("\n✅ Testing Streaming ACK...")

	// First, read some messages
	readResp, err := client.ReadStream(context.Background(), &pb.ReadStreamRequest{
		Topic:   "user.lookup",
		Count:   1000,
		StartId: "0",
	})
	if err != nil {
		log.Printf("Failed to read stream: %v", err)
		return
	}

	if len(readResp.Messages) == 0 {
		fmt.Println("No messages to ACK")
		return
	}

	start := time.Now()

	// Stream ACKs
	ackStream, err := client.AckBatch(context.Background())
	if err != nil {
		log.Fatalf("Failed to create ACK stream: %v", err)
	}

	for _, msg := range readResp.Messages {
		ackReq := &pb.AckRequest{
			Topic:         "user.lookup",
			ConsumerGroup: "user-lookup-group",
			Id:            msg.Id,
		}

		if err := ackStream.Send(ackReq); err != nil {
			log.Printf("ACK send failed: %v", err)
			break
		}
	}

	ackStream.CloseAndRecv()

	elapsed := time.Since(start)
	tps := float64(len(readResp.Messages)) / elapsed.Seconds()

	fmt.Printf("✅ Streaming ACK: %d messages in %v (%.0f TPS)\n",
		len(readResp.Messages), elapsed, tps)
}
