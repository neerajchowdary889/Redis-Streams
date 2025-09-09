# Redis Streams MQ (Reusable Module)
Reusable Redis Streams message queue module for Go microservices. Works as a library you can import across projects, with pluggable logging and config-driven topics.

## Install

Add to your `go.mod` (module name is `RedisStreams`):

```bash
go get RedisStreams
```

## Quick start

1) Prepare config (e.g. `config/examples.yml`).

2) Producer example:

```go
config, _ := Config.LoadConfigFromPath("config/examples.yml")
client, _ := QueueModule.New(config, nil)
defer client.Close()
client.Publish("example.topic", map[string]interface{}{"hello": "world"})
```

3) Consumer example:

```go
client.Subscribe(QueueModule.ConsumerConfig{
    TopicName:    "example.topic",
    ConsumerName: "svc-1",
    AutoAck:      true,
}, func(ctx context.Context, topic, id string, fields map[string]interface{}) error {
    // handle
    return nil
})
select {}
```

See `examples/producer` and `examples/consumer`.

## Configuration

Use per-service YAML in `config/` (see `order-service.yml`, etc.) or a minimal example in `config/examples.yml`. Environment variables prefixed with `REDIS_STREAMS_` override values.

## Features

- Config-driven topics and consumer groups
- Publish single/batch, auto-create streams
- Consumer groups with retry and DLQ support
- Lightweight metrics hooks and health checks
- Pluggable logger interface

## Versioning

Module path is `RedisStreams`; keep it consistent across projects.

## RDS.sh commands

Use the helper script for local dev and gRPC service:

- start: start Redis and monitoring via docker-compose
- stop: stop all services
- restart: restart docker services
- logs: tail Redis logs
- redis-cli: open Redis CLI in the container
- monitor: run Redis MONITOR
- clean: remove containers/volumes and build artifacts
- deps: go mod tidy and download
- build: build the example app to `bin/redis_streams`
- run: build and run the example app
- setup: create a `redis.conf` file
- protos: generate protobuf stubs into `api/proto` (same dir as proto)
- run-grpc: build and run the gRPC server (`cmd/grpc-server`)
- debug: basic stream introspection helpers
- health: check container health
- grafana: open Grafana dashboard in browser
- prometheus: open Prometheus metrics in browser
- monitoring: start all monitoring services (Redis, Prometheus, Grafana)

Examples:

```bash
./RDS.sh protos
./RDS.sh run-grpc                       # uses Config/config.yml by default
CONFIG_PATH=Config/config.yml ./RDS.sh run-grpc -- --port 9090
./RDS.sh monitoring                     # start monitoring stack
./RDS.sh grafana                        # open Grafana dashboard
```

## Monitoring & Observability

Complete monitoring stack with Prometheus and Grafana:

- **Grafana Dashboard**: http://localhost:3000 (admin/admin123)
- **Prometheus Metrics**: http://localhost:9090
- **Redis Insight**: http://localhost:5540

The dashboard includes:
- Message throughput (publish/consume rates)
- Stream lengths and processing performance
- Error rates and connection health
- Service uptime and total message counts

See `monitoring/README.md` for detailed setup and customization.
