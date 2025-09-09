// Redis Streams MQ gRPC Microservice
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	RSconfig "RedisStreams/Config"
	"RedisStreams/Health"
	RDLogging "RedisStreams/Logging"
	RDMetrics "RedisStreams/Metrics"
	"RedisStreams/QueueModule"
	pb "RedisStreams/api/proto"
	apiserver "RedisStreams/api/server"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	var (
		grpcPort   = flag.Int("grpc-port", 16001, "gRPC server port")
		healthPort = flag.Int("health-port", 8080, "Health check server port")
		configPath = flag.String("config", "Config/config.yml", "Configuration file path")
	)
	flag.Parse()

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

	grpcServer := grpc.NewServer()

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
