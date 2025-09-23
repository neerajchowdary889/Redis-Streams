// Redis Streams MQ gRPC Microservice
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	RSconfig "RedisStreams/Config"
	"RedisStreams/Health"
	RDLogging "RedisStreams/Logging"
	"RedisStreams/MRE"
	RDMetrics "RedisStreams/Metrics"
	"RedisStreams/QueueModule"
	pb "RedisStreams/api/proto"
	apiserver "RedisStreams/api/server"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	var (
		grpcPort   = flag.Int("grpc-port", 16001, "gRPC server port")
		healthPort = flag.Int("health-port", 8080, "Health check server port")
		configPath = flag.String("config", "Config/config.yml", "Configuration file path")
		Testing    = flag.Bool("testing", false, "Testing mode")
	)
	flag.Parse()

	if *Testing {

		time.Sleep(2 * time.Second)

		// Run the test
		MRE.TESTmain()
		log.Println("Testing Passed")
		return
	}

	// Load configuration
	config, err := RSconfig.LoadConfigFromPath(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Create logger
	logger := RDLogging.NewLogger(config.Logging)

	// Create Redis Streams MQ client
	mq, err := QueueModule.New(config, logger)
	if err != nil {
		log.Fatalf("Failed to create Redis Streams MQ: %v", err)
	}
	defer mq.Close()

	// Start health check server
	healthServer := Health.NewHealthServer(mq, config, *healthPort, logger, RDMetrics.NewMetrics())
	if err := healthServer.Start(); err != nil {
		log.Fatalf("Failed to start health server: %v", err)
	}
	defer healthServer.Stop()

	// Start gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *grpcPort))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Configure gRPC server with performance optimizations
	grpcServer := grpc.NewServer(
		grpc.MaxConcurrentStreams(uint32(config.Performance.MaxConcurrentStreams)),
		grpc.MaxRecvMsgSize(10*1024*1024), // 10MB max receive
		grpc.MaxSendMsgSize(10*1024*1024), // 10MB max send
	)

	// Register health check service
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthSrv)
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	// Register Redis Streams service
	pb.RegisterRedisStreamsServer(grpcServer, apiserver.NewRedisStreamsServer(mq, config))

	logger.Info("Redis Streams MQ gRPC microservice started",
		"grpc_port", *grpcPort,
		"health_port", *healthPort,
		"config_path", *configPath,
		"topics", len(config.Topics))

	// Start gRPC server in goroutine
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve gRPC: %v", err)
		}
	}()

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("Shutdown signal received, stopping service...")
	grpcServer.GracefulStop()
}

// TestingFunction runs the MRE Consumer continuously for event-driven processing
func TestingFunction() bool {
	// Create configuration
	cfg := MRE.DefaultMREConsumerConfig()
	cfg.Stream = "user:lookup"
	cfg.ConsumerGroup = "user-lookup-group"
	cfg.NumWorkers = 3
	cfg.BatchSize = 10_000 // Much smaller batch for testing
	cfg.LogEveryN = 5      // Log every 5 batches

	log.Printf("Starting MRE Consumer with config:")
	log.Printf("  Stream: %s", cfg.Stream)
	log.Printf("  Consumer Group: %s", cfg.ConsumerGroup)
	log.Printf("  Workers: %d", cfg.NumWorkers)
	log.Printf("  Batch Size: %d", cfg.BatchSize)

	// Create consumer
	consumer, err := MRE.NewMREConsumer(cfg)
	if err != nil {
		log.Printf("Failed to create consumer: %v", err)
		return false
	}

	// Start consuming
	if err := consumer.Start(CustomBatchHandler); err != nil {
		log.Printf("Failed to start consumer: %v", err)
		return false
	}

	// Run continuously - event-driven architecture
	log.Printf("Consumer started. Running continuously for event-driven processing...")
	log.Printf("Press Ctrl+C to stop the consumer")

	// Block indefinitely until interrupted
	select {}
}

// CustomBatchHandler implements the business logic for processing lookup requests
func CustomBatchHandler(ctx context.Context, batch []redis.XMessage) error {
	log.Printf("Processing batch of %d messages", len(batch))

	// Only process first 5 messages for logging
	headCount := 5
	if len(batch) < headCount {
		headCount = len(batch)
	}

	for i := 0; i < headCount; i++ {
		msg := batch[i]
		fields := msg.Values

		// Log some key fields for demonstration
		if queryID, ok := fields["query_id"].(string); ok {
			log.Printf("  Message %d: QueryID=%s, ID=%s", i+1, queryID, msg.ID)
		}

		// Simulate minimal processing time
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Microsecond):
			// Processing complete
		}
	}

	// Process remaining messages without logging
	for i := headCount; i < len(batch); i++ {
		// Simulate minimal processing time
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Microsecond):
			// Processing complete
		}
	}

	log.Printf("Successfully processed batch of %d messages", len(batch))
	return nil
}
