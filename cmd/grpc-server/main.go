package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	RSconfig "RedisStreams/Config"
	mq "RedisStreams/QueueModule"
	pb "RedisStreams/api/proto"
	apiserver "RedisStreams/api/server"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	var (
		port       = flag.Int("port", 9090, "gRPC listen port")
		configPath = flag.String("config", "Config/config.yml", "config path")
	)
	flag.Parse()

	cfg, err := RSconfig.LoadConfigFromPath(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	serverLogger := RSconfig.Logger(nil)
	mqClient, err := mq.New(cfg, serverLogger)
	if err != nil {
		log.Fatalf("failed to create MQ: %v", err)
	}
	defer mqClient.Close()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(s, healthSrv)

	pb.RegisterRedisStreamsServer(s, apiserver.NewRedisStreamsServer(mqClient, cfg))

	log.Printf("gRPC Redis Streams server listening on :%d", *port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
