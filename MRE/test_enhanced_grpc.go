package MRE

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "RedisStreams/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TESTmain() {
	// Connect to gRPC server with message size limits
	conn, err := grpc.Dial("localhost:16001",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(10*1024*1024), // 10MB max receive
			grpc.MaxCallSendMsgSize(10*1024*1024), // 10MB max send
		),
	)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewRedisStreamsClient(conn)

	// First, publish some test messages to ensure there's data to consume
	log.Println("Publishing test messages...")
	for i := 0; i < 5; i++ {
		publishReq := &pb.PublishRequest{
			Topic: "user.lookup",
			Text:  fmt.Sprintf("Test message %d", i+1),
			Headers: map[string]string{
				"message_id": fmt.Sprintf("msg-%d", i+1),
				"timestamp":  fmt.Sprintf("%d", time.Now().Unix()),
			},
		}

		resp, err := client.Publish(context.Background(), publishReq)
		if err != nil {
			log.Printf("Failed to publish message %d: %v", i+1, err)
		} else {
			log.Printf("Published message %d with ID: %s", i+1, resp.Id)
		}
	}
	log.Println("Finished publishing test messages")

	// Create subscribe request
	req := &pb.SubscribeRequest{
		Topic:             "user.lookup",
		ConsumerGroup:     "user-lookup-group",
		ConsumerName:      "test-consumer",
		StartId:           "0",  // Read new messages for consumer groups
		BatchSize:         10000,  // Much smaller batch for testing
		BlockTimeoutMs:    1000, // 1 second
		ConsumerTimeoutMs: 3000, // 30 seconds
		AutoAck:           true,
	}

	log.Printf("Starting enhanced gRPC subscription with config: %+v", req)

	// Start subscription
	stream, err := client.Subscribe(context.Background(), req)
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	// Track metrics
	var totalMessages int64
	var batchCount int64
	startTime := time.Now()

	// Create context with timeout for the test
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second) // 60 second test
	defer cancel()

	// Process messages with proper waiting
	for {
		select {
		case <-ctx.Done():
			log.Println("Test timeout reached")
			goto done
		default:
			// Set a timeout for receiving messages
			msgCtx, msgCancel := context.WithTimeout(ctx, 5*time.Second)

			// Use a channel to receive messages asynchronously
			msgChan := make(chan *pb.Message, 1)
			errChan := make(chan error, 1)

			go func() {
				msg, err := stream.Recv()
				if err != nil {
					errChan <- err
					return
				}
				msgChan <- msg
			}()

			select {
			case <-msgCtx.Done():
				msgCancel()
				log.Println("No messages received in 5 seconds, continuing to wait...")
				time.Sleep(1 * time.Second)
				continue
			case err := <-errChan:
				msgCancel()
				log.Printf("Stream ended: %v", err)
				goto done
			case msg := <-msgChan:
				msgCancel()

				totalMessages++
				if totalMessages%1000 == 0 {
					elapsed := time.Since(startTime)
					rate := float64(totalMessages) / elapsed.Seconds()
					log.Printf("Processed %d messages (rate: %.2f msg/sec)", totalMessages, rate)
				}

				// Log first few messages for verification
				if totalMessages <= 5 {
					log.Printf("Message %d: ID=%s, Topic=%s", totalMessages, msg.Id, msg.Topic)
				}

				// Simulate batch processing
				if totalMessages%1000 == 0 {
					batchCount++
					log.Printf("Completed batch %d with 1000 messages", batchCount)
				}
			}
		}
	}

done:
	elapsed := time.Since(startTime)
	rate := float64(totalMessages) / elapsed.Seconds()

	log.Printf("=== ENHANCED GRPC STREAMING TEST COMPLETE ===")
	log.Printf("Total messages processed: %d", totalMessages)
	log.Printf("Total batches: %d", batchCount)
	log.Printf("Total time: %v", elapsed)
	log.Printf("Average rate: %.2f msg/sec", rate)

	if totalMessages > 0 {
		log.Println("✅ Testing Passed")
	} else {
		log.Println("❌ Testing Failed - No messages received")
	}
}
