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
