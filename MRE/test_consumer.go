package MRE

import (
	"log"
	"time"
)

// TestMRELookupConsumer tests the consumer functionality
func TestMRELookupConsumer() {
	log.Println("=== Testing MRE Lookup Consumer ===")

	// Create consumer configuration
	config := DefaultMRELookupConsumerConfig("localhost:16001")
	config.BatchSize = 50000      // Up to 50k messages per batch
	config.ProcessingWorkers = 16 // More workers for high throughput
	config.EnableMetrics = true
	config.StatsInterval = 2 * time.Second

	// Create consumer
	consumer, err := NewMRELookupConsumer(config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Start consuming
	log.Println("Starting consumer...")
	if err := consumer.Start(); err != nil {
		log.Fatalf("Failed to start consumer: %v", err)
	}

	// Run for 10 seconds
	log.Println("Running consumer for 10 seconds...")
	time.Sleep(10 * time.Second)

	// Get metrics
	metrics := consumer.GetMetrics()
	log.Printf("Test results:")
	log.Printf("  Messages Received: %d", metrics.MessagesReceived)
	log.Printf("  Messages Processed: %d", metrics.MessagesProcessed)
	log.Printf("  Messages Failed: %d", metrics.MessagesFailed)
	log.Printf("  Messages ACKed: %d", metrics.MessagesAcked)
	log.Printf("  Processing Rate: %.2f msg/sec",
		float64(metrics.MessagesProcessed)/time.Since(metrics.StartTime).Seconds())

	// Stop consumer
	if err := consumer.Stop(); err != nil {
		log.Printf("Error stopping consumer: %v", err)
	}

	log.Println("Test completed")
}

// TestMRELookupConsumerWithCustomConfig tests with custom configuration
func TestMRELookupConsumerWithCustomConfig() {
	log.Println("=== Testing MRE Lookup Consumer with Custom Config ===")

	// Create custom configuration
	config := &MRELookupConsumerConfig{
		ServerAddr:        "localhost:16001",
		TopicName:         "user.lookup",
		StreamName:        "user:lookup",
		ConsumerGroup:     "user-lookup-group",
		ConsumerName:      "test-consumer",
		ConnectionTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      10 * time.Second,
		BatchSize:         50000, // Up to 50k messages per batch
		BlockTimeout:      1 * time.Second,
		ConsumerTimeout:   10 * time.Second,
		AutoAck:           false,
		MaxRetries:        3,
		ChannelBuffer:     100000, // Increased buffer for high throughput
		ProcessingWorkers: 16,     // More workers for high throughput
		EnableMetrics:     true,
		StatsInterval:     1 * time.Second,
		ShutdownTimeout:   30 * time.Second,
	}

	// Create consumer
	consumer, err := NewMRELookupConsumer(config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Start consuming
	log.Println("Starting custom consumer...")
	if err := consumer.Start(); err != nil {
		log.Fatalf("Failed to start consumer: %v", err)
	}

	// Run for 15 seconds
	log.Println("Running custom consumer for 15 seconds...")
	time.Sleep(15 * time.Second)

	// Get final metrics
	metrics := consumer.GetMetrics()
	log.Printf("Custom config test results:")
	log.Printf("  Messages Received: %d", metrics.MessagesReceived)
	log.Printf("  Messages Processed: %d", metrics.MessagesProcessed)
	log.Printf("  Messages Failed: %d", metrics.MessagesFailed)
	log.Printf("  Messages ACKed: %d", metrics.MessagesAcked)
	log.Printf("  Processing Rate: %.2f msg/sec",
		float64(metrics.MessagesProcessed)/time.Since(metrics.StartTime).Seconds())
	log.Printf("  Average Latency: %v", time.Duration(metrics.AverageLatency))
	log.Printf("  Max Latency: %v", time.Duration(metrics.MaxLatency))
	log.Printf("  Min Latency: %v", time.Duration(metrics.MinLatency))

	// Stop consumer
	if err := consumer.Stop(); err != nil {
		log.Printf("Error stopping consumer: %v", err)
	}

	log.Println("Custom config test completed")
}

// TestMRELookupConsumerErrorHandling tests error handling
func TestMRELookupConsumerErrorHandling() {
	log.Println("=== Testing MRE Lookup Consumer Error Handling ===")

	// Create consumer with error handling
	config := DefaultMRELookupConsumerConfig("localhost:16001")
	config.MaxRetries = 3
	config.EnableMetrics = true
	config.StatsInterval = 1 * time.Second

	consumer, err := NewMRELookupConsumer(config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Start consuming
	log.Println("Starting consumer with error handling...")
	if err := consumer.Start(); err != nil {
		log.Fatalf("Failed to start consumer: %v", err)
	}

	// Monitor for errors
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			metrics := consumer.GetMetrics()
			if metrics.ProcessingErrors > 0 || metrics.ConnectionErrors > 0 {
				log.Printf("Errors detected - Processing: %d, Connection: %d, Parse: %d, ACK: %d",
					metrics.ProcessingErrors, metrics.ConnectionErrors,
					metrics.ParseErrors, metrics.AckErrors)
			}
		}
	}()

	// Run for 20 seconds
	log.Println("Running consumer with error monitoring for 20 seconds...")
	time.Sleep(20 * time.Second)

	// Get final metrics
	metrics := consumer.GetMetrics()
	log.Printf("Error handling test results:")
	log.Printf("  Messages Received: %d", metrics.MessagesReceived)
	log.Printf("  Messages Processed: %d", metrics.MessagesProcessed)
	log.Printf("  Messages Failed: %d", metrics.MessagesFailed)
	log.Printf("  Messages ACKed: %d", metrics.MessagesAcked)
	log.Printf("  Processing Errors: %d", metrics.ProcessingErrors)
	log.Printf("  Connection Errors: %d", metrics.ConnectionErrors)
	log.Printf("  Parse Errors: %d", metrics.ParseErrors)
	log.Printf("  ACK Errors: %d", metrics.AckErrors)
	log.Printf("  Processing Rate: %.2f msg/sec",
		float64(metrics.MessagesProcessed)/time.Since(metrics.StartTime).Seconds())

	// Stop consumer
	if err := consumer.Stop(); err != nil {
		log.Printf("Error stopping consumer: %v", err)
	}

	log.Println("Error handling test completed")
}
