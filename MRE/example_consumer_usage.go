package MRE

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// ExampleMRELookupConsumerUsage demonstrates how to use the MRE Lookup Consumer
func ExampleMRELookupConsumerUsage() {
	log.Println("=== MRE Lookup Consumer Example ===")

	// Create consumer configuration
	config := DefaultMRELookupConsumerConfig("localhost:16001")

	// Customize configuration if needed
	config.BatchSize = 500
	config.ProcessingWorkers = 2
	config.EnableMetrics = true
	config.StatsInterval = 3 * time.Second

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

	// Set up graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	<-sigChan
	log.Println("Shutdown signal received, stopping consumer...")

	// Stop consumer
	if err := consumer.Stop(); err != nil {
		log.Printf("Error stopping consumer: %v", err)
	}

	log.Println("Consumer stopped successfully")
}

// ExampleMRELookupConsumerWithCustomConfig demonstrates custom configuration
func ExampleMRELookupConsumerWithCustomConfig() {
	log.Println("=== MRE Lookup Consumer with Custom Config ===")

	// Create custom configuration
	config := &MRELookupConsumerConfig{
		ServerAddr:        "localhost:16001",
		TopicName:         "user.lookup",
		StreamName:        "user:lookup",
		ConsumerGroup:     "user-lookup-group",
		ConsumerName:      "mre-lookup-consumer",
		ConnectionTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      15 * time.Second,
		BatchSize:         2000,
		BlockTimeout:      2 * time.Second,
		ConsumerTimeout:   15 * time.Second,
		AutoAck:           false, // Manual ACK for better control
		MaxRetries:        5,
		ChannelBuffer:     20000,
		ProcessingWorkers: 8,
		EnableMetrics:     true,
		StatsInterval:     2 * time.Second,
		ShutdownTimeout:   45 * time.Second,
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

	// Monitor for a specific duration
	log.Println("Monitoring for 30 seconds...")
	time.Sleep(30 * time.Second)

	// Get final metrics
	metrics := consumer.GetMetrics()
	log.Printf("Final metrics - Processed: %d, Failed: %d, Rate: %.2f msg/sec",
		metrics.MessagesProcessed, metrics.MessagesFailed,
		float64(metrics.MessagesProcessed)/time.Since(metrics.StartTime).Seconds())

	// Stop consumer
	if err := consumer.Stop(); err != nil {
		log.Printf("Error stopping consumer: %v", err)
	}
}

// ExampleMRELookupConsumerHighThroughput demonstrates high-throughput configuration
func ExampleMRELookupConsumerHighThroughput() {
	log.Println("=== MRE Lookup Consumer High Throughput ===")

	// High-throughput configuration
	config := &MRELookupConsumerConfig{
		ServerAddr:        "localhost:16001",
		TopicName:         "user.lookup",
		StreamName:        "user:lookup",
		ConsumerGroup:     "user-lookup-group",
		ConsumerName:      "high-throughput-consumer",
		ConnectionTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      10 * time.Second,
		BatchSize:         5000,                   // Large batch size
		BlockTimeout:      500 * time.Millisecond, // Short block timeout
		ConsumerTimeout:   5 * time.Second,        // Short processing timeout
		AutoAck:           true,                   // Auto ACK for speed
		MaxRetries:        2,                      // Fewer retries
		ChannelBuffer:     50000,                  // Large buffer
		ProcessingWorkers: 16,                     // Many workers
		EnableMetrics:     true,
		StatsInterval:     1 * time.Second, // Frequent reporting
		ShutdownTimeout:   60 * time.Second,
	}

	// Create consumer
	consumer, err := NewMRELookupConsumer(config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Start consuming
	log.Println("Starting high-throughput consumer...")
	if err := consumer.Start(); err != nil {
		log.Fatalf("Failed to start consumer: %v", err)
	}

	// Run for a specific duration
	log.Println("Running high-throughput consumer for 60 seconds...")
	time.Sleep(60 * time.Second)

	// Get final metrics
	metrics := consumer.GetMetrics()
	log.Printf("High-throughput results:")
	log.Printf("  Messages Processed: %d", metrics.MessagesProcessed)
	log.Printf("  Messages Failed: %d", metrics.MessagesFailed)
	log.Printf("  Processing Rate: %.2f msg/sec",
		float64(metrics.MessagesProcessed)/time.Since(metrics.StartTime).Seconds())
	log.Printf("  Average Latency: %v", time.Duration(metrics.AverageLatency))
	log.Printf("  Max Latency: %v", time.Duration(metrics.MaxLatency))

	// Stop consumer
	if err := consumer.Stop(); err != nil {
		log.Printf("Error stopping consumer: %v", err)
	}
}

// ExampleMRELookupConsumerWithErrorHandling demonstrates error handling
func ExampleMRELookupConsumerWithErrorHandling() {
	log.Println("=== MRE Lookup Consumer with Error Handling ===")

	// Create consumer with error handling
	config := DefaultMRELookupConsumerConfig("localhost:16001")
	config.MaxRetries = 5
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
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			metrics := consumer.GetMetrics()
			if metrics.ProcessingErrors > 0 || metrics.ConnectionErrors > 0 {
				log.Printf("Errors detected - Processing: %d, Connection: %d",
					metrics.ProcessingErrors, metrics.ConnectionErrors)
			}
		}
	}()

	// Run for 30 seconds
	time.Sleep(30 * time.Second)

	// Stop consumer
	if err := consumer.Stop(); err != nil {
		log.Printf("Error stopping consumer: %v", err)
	}
}
