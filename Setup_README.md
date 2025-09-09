# Redis Streams MQ - gRPC Microservice Setup Guide

This guide explains how to set up and use the Redis Streams MQ as a standalone gRPC microservice.

## 📋 Table of Contents

1. [Quick Start](#quick-start)
2. [Project Structure](#project-structure)
3. [Configuration Setup](#configuration-setup)
4. [Code Integration](#code-integration)
5. [gRPC Service Setup](#grpc-service-setup)
6. [Monitoring Setup](#monitoring-setup)
7. [Customization Guide](#customization-guide)
8. [Troubleshooting](#troubleshooting)

## 🚀 Quick Start

### 1. Start the Microservice

```bash
# Start Redis and monitoring
./RDS.sh start

# Start the gRPC microservice
./RDS.sh run-grpc
```

### 2. Connect from Your Application

```go
package main

import (
    "context"
    "log"
    
    pb "RedisStreams/api/proto"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    "google.golang.org/protobuf/types/known/structpb"
)

func main() {
    // Connect to gRPC server
    conn, err := grpc.Dial("localhost:9090", grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()
    
    client := pb.NewRedisStreamsClient(conn)
    
    // Publish message
    resp, err := client.Publish(context.Background(), &pb.PublishRequest{
        Topic: "user.created",
        Json: &structpb.Struct{
            Fields: map[string]*structpb.Value{
                "user_id": {Kind: &structpb.Value_StringValue{StringValue: "123"}},
                "email":   {Kind: &structpb.Value_StringValue{StringValue: "user@example.com"}},
            },
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    
    log.Printf("Published message ID: %s", resp.Id)
    
    // Read messages with limit
    readResp, err := client.ReadStream(context.Background(), &pb.ReadStreamRequest{
        Topic:  "user.created",
        Count:  10,
        StartId: "0",
    })
    if err != nil {
        log.Fatal(err)
    }
    
    log.Printf("Read %d messages", len(readResp.Messages))
}
```

## 🏗️ Architecture

### Microservice Setup

```
Redis Streams MQ gRPC Microservice
├── gRPC Server (port 9090)       # Main API endpoint
├── Health Server (port 8080)     # Health checks and metrics
├── Redis Connection              # Direct Redis access
└── Monitoring                    # Prometheus + Grafana
```

### Client Integration

```
Your Application
├── gRPC Client                   # Connect to microservice
├── Proto Files                   # Generated from .proto
└── Business Logic                # Your application code
```

### Service Endpoints

- **gRPC API**: `localhost:9090` - All Redis Streams operations
- **Health Check**: `localhost:8080/health` - Service health
- **Metrics**: `localhost:8080/metrics/prometheus` - Prometheus metrics
- **Grafana**: `localhost:3000` - Monitoring dashboard

## ⚙️ Configuration Setup

### 1. Create `config.yml`

```yaml
# Redis connection settings
redis:
  host: "localhost"
  port: 6379
  password: ""
  database: 0
  pool_size: 10
  min_idle_conns: 5
  max_retries: 3
  retry_backoff: 1s
  dial_timeout: 5s
  read_timeout: 3s
  write_timeout: 3s
  pool_timeout: 4s
  client_name: "your-service"
  tls:
    enabled: false
    cert_file: ""
    key_file: ""
    ca_file: ""
    insecure_skip_verify: false

# Stream configuration
streams:
  auto_create_streams: true

# Topics configuration
topics:
  - name: "user.created"
    stream_name: "stream:user:created"
    consumer_group: "group:user-service"
    max_len: 10000
    max_age: 24h
    trim_strategy: "MAXLEN"
    retry_attempts: 3
    dead_letter_topic: "user.dlq"
    description: "User creation events"

  - name: "order.processed"
    stream_name: "stream:order:processed"
    consumer_group: "group:order-service"
    max_len: 50000
    max_age: 168h
    trim_strategy: "MAXLEN"
    retry_attempts: 5
    dead_letter_topic: "order.dlq"
    description: "Order processing events"

# Consumer defaults
consumers:
  default_batch_size: 10
  default_block_timeout: 2s
  default_consumer_timeout: 30s
  default_auto_ack: true
  max_pending_messages: 1000
  claim_min_idle_time: 1m
  claim_interval: 30s
  delete_after_ack: false

# Monitoring
monitoring:
  enabled: true
  health_check_port: 8080
  metrics_interval: 30s

# Logging
logging:
  level: "info"
  output: "stdout"
  structured: true
```

### 2. Environment Variables Override

You can override any config value using environment variables:

```bash
export REDIS_STREAMS_REDIS_HOST=redis.example.com
export REDIS_STREAMS_REDIS_PORT=6380
export REDIS_STREAMS_REDIS_PASSWORD=secret
export REDIS_STREAMS_TOPICS_0_NAME=my.topic
export REDIS_STREAMS_CONSUMERS_DEFAULT_AUTO_ACK=false
```

## 🔌 gRPC API Reference

### Available Methods

#### Publishing
- `Publish()` - Publish single message
- `PublishBatch()` - Publish multiple messages

#### Consuming  
- `Subscribe()` - Stream messages (real-time)
- `ReadStream()` - Read messages with limit and pagination
- `ReadRange()` - Read messages in specific range

#### Management
- `ListTopics()` - Get all configured topics
- `StreamInfo()` - Get stream statistics
- `ConsumerGroupInfo()` - Get consumer group details
- `CreateConsumerGroup()` - Create new consumer group
- `DeleteConsumerGroup()` - Delete consumer group
- `Ack()` - Acknowledge message

### 1. Publishing Messages

```go
// Single message
resp, err := client.Publish(ctx, &pb.PublishRequest{
    Topic: "user.created",
    Json: &structpb.Struct{
        Fields: map[string]*structpb.Value{
            "user_id": {Kind: &structpb.Value_StringValue{StringValue: "123"}},
            "email":   {Kind: &structpb.Value_StringValue{StringValue: "user@example.com"}},
        },
    },
    Headers: map[string]string{
        "source": "user-service",
    },
})

// Batch messages
batchResp, err := client.PublishBatch(ctx, &pb.PublishBatchRequest{
    Messages: []*pb.BatchMessage{
        {
            Topic: "order.created",
            Fields: &structpb.Struct{
                Fields: map[string]*structpb.Value{
                    "data": {Kind: &structpb.Value_StringValue{StringValue: `{"order_id":"ORD-001"}`}},
                },
            },
        },
    },
})
```

### 2. Reading Messages

```go
// Read with limit and pagination
readResp, err := client.ReadStream(ctx, &pb.ReadStreamRequest{
    Topic:  "user.created",
    Count:  10,           // Limit to 10 messages
    StartId: "0",         // Start from beginning
    BlockTimeoutMs: 5000, // Block for 5 seconds if no messages
})

// Read specific range
rangeResp, err := client.ReadRange(ctx, &pb.ReadRangeRequest{
    Topic:   "user.created",
    StartId: "1234567890-0",
    EndId:   "+",         // Latest
    Count:   50,
})

// Stream messages (real-time)
stream, err := client.Subscribe(ctx, &pb.SubscribeRequest{
    Topic:        "user.created",
    ConsumerName: "my-service",
    AutoAck:      true,
    BatchSize:    5,
    BlockTimeoutMs: 10000,
})

for {
    msg, err := stream.Recv()
    if err != nil {
        break
    }
    // Process message
}
```

### 3. Stream Management

```go
// Get stream information
info, err := client.StreamInfo(ctx, &pb.StreamInfoRequest{
    Topic: "user.created",
})
// info.Length, info.FirstEntryId, info.LastEntryId, etc.

// List all topics
topics, err := client.ListTopics(ctx, &pb.ListTopicsRequest{})
// topics.Names contains all topic names

// Create consumer group
_, err = client.CreateConsumerGroup(ctx, &pb.CreateConsumerGroupRequest{
    Topic:     "user.created",
    GroupName: "email-processors",
    StartId:   "$", // Start from new messages
})

// Get consumer group info
groups, err := client.ConsumerGroupInfo(ctx, &pb.ConsumerGroupInfoRequest{
    Topic: "user.created",
})
```

### 4. Message Acknowledgment

```go
// Manual acknowledgment
_, err := client.Ack(ctx, &pb.AckRequest{
    Topic:        "user.created",
    ConsumerGroup: "email-processors",
    Id:           "1234567890-0",
})
```

## 🔌 gRPC Service Setup

### 1. Generate Protobuf Files

```bash
# Copy proto files to your project
cp -r RedisStreams/api/proto your-project/api/proto

# Generate Go files
protoc -I api/proto \
  --go_out=paths=source_relative:api/proto \
  --go-grpc_out=paths=source_relative:api/proto \
  api/proto/redis_streams.proto
```

### 2. Create gRPC Server

```go
package main

import (
    "flag"
    "log"
    "net"
    
    RSconfig "RedisStreams/Config"
    "RedisStreams/QueueModule"
    pb "your-project/api/proto"
    "your-project/api/server"
    
    "google.golang.org/grpc"
)

func main() {
    port := flag.Int("port", 9090, "gRPC port")
    configPath := flag.String("config", "config.yml", "Config path")
    flag.Parse()
    
    // Load config and create MQ client
    config, _ := RSconfig.LoadConfigFromPath(*configPath)
    mq, _ := QueueModule.New(config, nil)
    defer mq.Close()
    
    // Start gRPC server
    lis, _ := net.Listen("tcp", fmt.Sprintf(":%d", *port))
    s := grpc.NewServer()
    
    // Register your service
    pb.RegisterRedisStreamsServer(s, server.NewRedisStreamsServer(mq, config))
    
    log.Printf("gRPC server listening on :%d", *port)
    s.Serve(lis)
}
```

### 3. gRPC Client Usage

```go
// Connect to gRPC service
conn, _ := grpc.Dial("localhost:9090", grpc.WithInsecure())
client := pb.NewRedisStreamsClient(conn)

// Publish message
resp, err := client.Publish(context.Background(), &pb.PublishRequest{
    Topic: "user.created",
    Json: &structpb.Struct{
        Fields: map[string]*structpb.Value{
            "user_id": {Kind: &structpb.Value_StringValue{StringValue: "123"}},
            "email":   {Kind: &structpb.Value_StringValue{StringValue: "user@example.com"}},
        },
    },
})

// Subscribe to messages
stream, err := client.Subscribe(context.Background(), &pb.SubscribeRequest{
    Topic:        "user.created",
    ConsumerName: "my-service",
    AutoAck:      true,
})

for {
    msg, err := stream.Recv()
    if err != nil {
        break
    }
    log.Printf("Received: %+v", msg)
}
```

## 📊 Monitoring Setup

### 1. Health Check Integration

```go
import (
    "RedisStreams/Health"
    "RedisStreams/Metrics"
)

func main() {
    // ... existing code ...
    
    // Start health server
    healthServer := Health.NewHealthServer(mq, config, 8080, logger, Metrics.NewMetrics())
    healthServer.Start()
    defer healthServer.Stop()
    
    // Your application logic...
}
```

### 2. Prometheus Metrics

The module automatically exposes metrics at `/metrics/prometheus`:

- `redis_streams_messages_published_total`
- `redis_streams_messages_processed_total`
- `redis_streams_publish_errors_total`
- `redis_streams_processing_errors_total`
- `redis_streams_stream_length`
- `redis_streams_avg_processing_time_ms`
- `redis_streams_uptime_seconds`

### 3. Docker Compose for Monitoring

```yaml
version: '3.8'
services:
  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]
  
  prometheus:
    image: prom/prometheus:latest
    ports: ["9090:9090"]
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml:ro
  
  grafana:
    image: grafana/grafana:latest
    ports: ["3000:3000"]
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin123
```

## 🎨 Customization Guide

### 1. Custom Logger

```go
type CustomLogger struct {
    // Your logger implementation
}

func (l *CustomLogger) Debug(msg string, args ...interface{}) {
    // Your debug logging
}

func (l *CustomLogger) Info(msg string, args ...interface{}) {
    // Your info logging
}

func (l *CustomLogger) Warn(msg string, args ...interface{}) {
    // Your warning logging
}

func (l *CustomLogger) Error(msg string, args ...interface{}) {
    // Your error logging
}

// Use custom logger
mq, err := QueueModule.New(config, &CustomLogger{})
```

### 2. Custom Message Handler

```go
type MessageProcessor struct {
    mq *QueueModule.RedisStreamMQ
}

func (p *MessageProcessor) ProcessUserEvents(ctx context.Context, topic, msgID string, fields map[string]interface{}) error {
    // Your custom processing logic
    return nil
}

func (p *MessageProcessor) ProcessOrderEvents(ctx context.Context, topic, msgID string, fields map[string]interface{}) error {
    // Your custom processing logic
    return nil
}

// Register handlers
mq.Subscribe(QueueModule.ConsumerConfig{
    TopicName: "user.created",
    ConsumerName: "user-processor",
    AutoAck: true,
}, processor.ProcessUserEvents)

mq.Subscribe(QueueModule.ConsumerConfig{
    TopicName: "order.created", 
    ConsumerName: "order-processor",
    AutoAck: true,
}, processor.ProcessOrderEvents)
```

### 3. Environment-Specific Configuration

```go
func loadConfig() (*RSconfig.Config, error) {
    env := os.Getenv("ENVIRONMENT")
    
    var configPath string
    switch env {
    case "production":
        configPath = "config/prod.yml"
    case "staging":
        configPath = "config/staging.yml"
    default:
        configPath = "config/dev.yml"
    }
    
    return RSconfig.LoadConfigFromPath(configPath)
}
```

## 🔧 Troubleshooting

### Common Issues

1. **"topic not configured" error**
   - Ensure topic is defined in `config.yml` under `topics` section
   - Check topic name spelling matches exactly

2. **Connection refused to Redis**
   - Verify Redis is running: `docker ps | grep redis`
   - Check host/port in config
   - Ensure Redis is accessible from your application

3. **Messages not being consumed**
   - Check consumer group configuration
   - Verify `AutoAck` setting matches your needs
   - Look for errors in application logs

4. **High memory usage**
   - Adjust `max_len` and `max_age` for streams
   - Enable `delete_after_ack` for high-throughput scenarios
   - Monitor stream lengths in Grafana

### Debug Commands

```bash
# Check Redis streams
docker exec -it redis-streams redis-cli XINFO STREAMS

# Monitor Redis commands
docker exec -it redis-streams redis-cli MONITOR

# Check consumer groups
docker exec -it redis-streams redis-cli XINFO GROUPS stream:user:created
```

### Performance Tuning

1. **Batch Size**: Increase `default_batch_size` for higher throughput
2. **Connection Pool**: Adjust `pool_size` based on concurrent consumers
3. **Timeouts**: Tune `block_timeout` and `consumer_timeout` for your use case
4. **Stream Limits**: Set appropriate `max_len` and `max_age` values

## 📚 Additional Resources

- [Redis Streams Documentation](https://redis.io/docs/data-types/streams/)
- [Prometheus Metrics](https://prometheus.io/docs/concepts/metric_types/)
- [Grafana Dashboards](https://grafana.com/docs/grafana/latest/dashboards/)
- [gRPC Go Quick Start](https://grpc.io/docs/languages/go/quickstart/)

## 🤝 Support

For issues and questions:
1. Check the troubleshooting section above
2. Review the example code in this repository
3. Check Redis and application logs
4. Verify configuration matches your requirements

---

**Happy Messaging! 🚀**
