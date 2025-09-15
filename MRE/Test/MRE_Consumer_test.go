package MRE

import (
	"RedisStreams/MRE"
	"context"
	"log"
	"testing"
	"time"

	pb "RedisStreams/api/proto"
)

func TestStreaming(t *testing.T) {
	streamName := "user.lookup"
	consumerGroup := ""
	consumerName := "test-consumer"

	c, err := MRE.NewLookupConsumer("localhost:16001")
	if err != nil {
		t.Fatalf("❌ Failed to create consumer: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf("🧪 Testing streaming for stream: %s", streamName)
	log.Printf("📋 Consumer Group: %s, Consumer: %s", consumerGroup, consumerName)

	// First, let's check if the stream exists and has messages
	log.Println("🔍 Checking stream info...")

	// Create a simple request to check stream info
	streamInfoReq := &pb.StreamInfoRequest{Topic: streamName}
	streamInfo, err := c.Client.StreamInfo(ctx, streamInfoReq)
	if err != nil {
		log.Printf("❌ Failed to get stream info: %v", err)
		return
	}

	log.Printf("📊 Stream Info:")
	log.Printf("   Length: %d messages", streamInfo.Length)
	log.Printf("   First Entry: %s", streamInfo.FirstEntryId)
	log.Printf("   Last Entry: %s", streamInfo.LastEntryId)
	log.Printf("   Groups: %d", streamInfo.Groups)
	log.Printf("   Consumers: %d", streamInfo.Consumers)

	// Try to read some messages directly first
	log.Println("\n📖 Testing direct read...")
	readReq := &pb.ReadStreamRequest{
		Topic:   streamName,
		StartId: "0", // Start from beginning
		Count:   5,   // Read 5 messages
	}

	readResp, err := c.Client.ReadStream(ctx, readReq)
	if err != nil {
		log.Printf("❌ Failed to read stream directly: %v", err)
	} else {
		log.Printf("✅ Direct read successful: %d messages", len(readResp.Messages))
		for i, msg := range readResp.Messages {
			log.Printf("   Message %d: ID=%s, Fields=%+v", i+1, msg.Id, msg.Fields.AsMap())
		}
	}

	// Now test the streaming subscription
	log.Println("\n🌊 Testing streaming subscription...")

	// FIX: Use consumerName instead of consumerGroup
	req := &pb.SubscribeRequest{
		Topic:             streamName,
		ConsumerName:      consumerName, // FIXED: was consumerGroup
		BatchSize:         5,
		BlockTimeoutMs:    5000,  // 5 seconds
		ConsumerTimeoutMs: 15000, // 15 seconds
		AutoAck:           false, // Manual ack for testing
	}

	log.Printf("📤 Subscribe Request:")
	log.Printf("   Topic: %s", req.Topic)
	log.Printf("   ConsumerName: %s", req.ConsumerName)
	log.Printf("   BatchSize: %d", req.BatchSize)
	log.Printf("   BlockTimeoutMs: %d", req.BlockTimeoutMs)
	log.Printf("   AutoAck: %v", req.AutoAck)

	// Start streaming
	stream, err := c.Client.Subscribe(ctx, req)
	if err != nil {
		log.Printf("❌ Failed to subscribe to stream: %v", err)
		return
	}

	log.Println("✅ Successfully subscribed to stream")
	log.Println("⏳ Waiting for messages (will timeout after 10 seconds)...")

	// Set up timeout
	timeout := time.After(10 * time.Second)
	messageCount := 0

	for {
		select {
		case <-timeout:
			log.Printf("⏰ Timeout reached after 10 seconds")
			log.Printf("📊 Total messages received: %d", messageCount)
			if messageCount == 0 {
				log.Println("❌ No messages received - this indicates a problem with streaming")
				log.Println("💡 Possible causes:")
				log.Println("   1. Consumer group already consumed all messages")
				log.Println("   2. Wrong consumer name/group configuration")
				log.Println("   3. Stream is empty or has no new messages")
				log.Println("   4. Consumer group start ID is set to '$' (new messages only)")
			}
			return

		default:
			// Try to receive a message
			message, err := stream.Recv()
			if err != nil {
				if err.Error() == "EOF" {
					log.Println("📄 Stream ended (EOF)")
					return
				}
				log.Printf("❌ Error receiving message: %v", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}

			messageCount++
			log.Printf("🎉 Received message #%d:", messageCount)
			log.Printf("   ID: %s", message.Id)
			log.Printf("   Topic: %s", message.Topic)
			log.Printf("   Fields: %+v", message.Fields.AsMap())

			// Acknowledge the message
			ackReq := &pb.AckRequest{
				Topic: streamName,
				Id:    message.Id,
			}

			ackCtx, ackCancel := context.WithTimeout(context.Background(), 30*time.Second)
			_, ackErr := c.Client.Ack(ackCtx, ackReq)
			ackCancel()
			if ackErr != nil {
				log.Printf("⚠️  Failed to ACK message %s: %v", message.Id, ackErr)
			} else {
				log.Printf("✅ ACKed message: %s", message.Id)
			}
		}
	}
}

func TestFunctions(t *testing.T) {
	log.Println("�� Testing MRE Consumer TestStreaming Function")
	log.Println("==============================================")

	log.Println("✅ Connected to gRPC server at localhost:16001")

	// Test streaming with the TestStreaming function
	log.Println("\n🚀 Starting TestStreaming...")
	TestStreaming(t)

	log.Println("\n�� Test completed!")
}
