# Python gRPC Client for Redis Streams MQ

This directory contains Python clients for the Redis Streams MQ gRPC microservice.

## 🚀 Quick Start

### 1. Start the gRPC Server

```bash
# Start Redis and monitoring
./RDS.sh start

# Start the gRPC microservice
./RDS.sh run-microservice
```

### 2. Run Python Client

```bash
# Run the full Python client (continuous publishing/consuming)
./RDS.sh python-client

# Or run just the test script
./RDS.sh python-test
```

## 📁 Files

- `python_client.py` - Full-featured client with continuous publishing/consuming
- `simple_test.py` - Basic test script for gRPC operations
- `generate_protos.py` - Script to generate Python protobuf files
- `run_python_client.sh` - Setup and run script
- `requirements.txt` - Python dependencies

## 🔧 Setup

### Manual Setup

```bash
# Install dependencies
pip3 install -r requirements.txt

# Generate protobuf files
python3 generate_protos.py

# Run client
python3 python_client.py
```

## 📖 Usage Examples

### Basic Operations

```python
from redis_streams_pb2_grpc import RedisStreamsStub
from redis_streams_pb2 import *
import grpc

# Connect to server
channel = grpc.insecure_channel("localhost:9090")
stub = RedisStreamsStub(channel)

# Publish message
response = stub.Publish(PublishRequest(
    topic="user.created",
    json=struct_pb2.Struct({
        "user_id": "123",
        "email": "user@example.com"
    })
))

# Read messages with limit
read_response = stub.ReadStream(ReadStreamRequest(
    topic="user.created",
    count=10,
    start_id="0"
))

# Publish batch
batch_response = stub.PublishBatch(PublishBatchRequest(
    messages=[
        BatchMessage(
            topic="user.created",
            fields=struct_pb2.Struct({
                "data": '{"id":"1","name":"John"}'
            })
        )
    ]
))
```

### Continuous Publishing

```python
import time
import threading

def publish_continuously():
    while True:
        # Publish message
        response = stub.Publish(PublishRequest(
            topic="user.created",
            json=struct_pb2.Struct({
                "id": f"msg_{int(time.time())}",
                "data": f"Message at {time.time()}"
            })
        ))
        print(f"Published: {response.id}")
        time.sleep(1)

# Start in background thread
threading.Thread(target=publish_continuously, daemon=True).start()
```

### Continuous Consuming

```python
def consume_continuously():
    for message in stub.Subscribe(SubscribeRequest(
        topic="user.created",
        consumer_name="python-consumer",
        auto_ack=True
    )):
        print(f"Received: {message.id}")
        # Process message...

# Start consuming
consume_continuously()
```

## 🎯 Features

### Publishing
- **Single Messages**: `Publish()` with JSON, text, or binary data
- **Batch Messages**: `PublishBatch()` for multiple messages
- **Headers**: Custom headers for message metadata

### Consuming
- **Streaming**: Real-time message consumption via `Subscribe()`
- **Polling**: Read messages with limits via `ReadStream()`
- **Range Queries**: Read specific ID ranges via `ReadRange()`

### Management
- **Stream Info**: Get stream statistics
- **Topic Listing**: List all available topics
- **Consumer Groups**: Manage consumer groups

## 🔄 Continuous Operations

The `python_client.py` script demonstrates:

1. **Continuous Publishing**: Publishes messages every 1 second
2. **Batch Publishing**: Publishes batches every 3 seconds
3. **Continuous Consuming**: Polls for messages every 2 seconds
4. **Real-time Streaming**: Subscribes to live message streams

## 📊 Monitoring

- **Stream Statistics**: Length, first/last entry IDs
- **Consumer Groups**: Group information and consumer counts
- **Health Checks**: Server health and connectivity

## 🛠️ Troubleshooting

### Common Issues

1. **"No topics found"**
   - Ensure gRPC server is running: `./RDS.sh run-microservice`
   - Check server configuration has topics defined

2. **"Connection refused"**
   - Verify gRPC server is running on port 9090
   - Check server logs for errors

3. **"Module not found" errors**
   - Run `python3 generate_protos.py` to generate protobuf files
   - Install requirements: `pip3 install -r requirements.txt`

### Debug Commands

```bash
# Check if gRPC server is running
curl http://localhost:8080/health

# Check server logs
./RDS.sh logs

# Test basic connectivity
./RDS.sh python-test
```

## 🎨 Customization

### Custom Message Types

```python
# Define your message structure
def create_user_message(user_id, email, name):
    return struct_pb2.Struct({
        "user_id": user_id,
        "email": email,
        "name": name,
        "timestamp": time.time(),
        "source": "python-client"
    })

# Publish custom message
response = stub.Publish(PublishRequest(
    topic="user.created",
    json=create_user_message("123", "user@example.com", "John Doe")
))
```

### Custom Headers

```python
headers = {
    "source": "python-service",
    "version": "1.0",
    "environment": "production",
    "correlation_id": "req-123"
}

response = stub.Publish(PublishRequest(
    topic="user.created",
    json=message_data,
    headers=headers
))
```

## 📈 Performance Tips

1. **Batch Publishing**: Use `PublishBatch()` for multiple messages
2. **Connection Pooling**: Reuse gRPC channels
3. **Async Operations**: Use threading for concurrent operations
4. **Message Limits**: Use `count` parameter to limit reads

## 🔗 Integration

This Python client can be integrated into:
- **Web Applications**: Django, Flask, FastAPI
- **Data Processing**: Pandas, NumPy workflows
- **Microservices**: Service-to-service communication
- **Monitoring**: Health checks and metrics collection

---

**Happy Python Streaming! 🐍🚀**
