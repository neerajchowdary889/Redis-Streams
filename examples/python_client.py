#!/usr/bin/env python3
"""
Python gRPC client for Redis Streams MQ
Continuously publishes and consumes messages using gRPC
"""

import grpc
import time
import json
import threading
import random
from concurrent import futures
from typing import Iterator

# Import generated protobuf files
import redis_streams_pb2
import redis_streams_pb2_grpc
from google.protobuf import struct_pb2


class RedisStreamsClient:
    def __init__(self, server_address: str = "localhost:16001"):
        self.server_address = server_address
        self.channel = grpc.insecure_channel(server_address)
        self.stub = redis_streams_pb2_grpc.RedisStreamsStub(self.channel)
        self.running = False

    def close(self):
        """Close the gRPC channel"""
        self.channel.close()

    def publish_message(self, topic: str, data: dict, headers: dict = None) -> str:
        """Publish a single message"""
        try:
            # Create JSON payload
            json_data = struct_pb2.Struct()
            for key, value in data.items():
                if isinstance(value, str):
                    json_data[key] = value
                elif isinstance(value, (int, float)):
                    json_data[key] = value
                elif isinstance(value, bool):
                    json_data[key] = value
                else:
                    json_data[key] = str(value)

            # Create request
            request = redis_streams_pb2.PublishRequest(
                topic=topic,
                json=json_data,
                headers=headers or {}
            )

            # Publish message
            response = self.stub.Publish(request)
            return response.id

        except grpc.RpcError as e:
            print(f"Error publishing message: {e}")
            return None

    def publish_batch(self, messages: list) -> bool:
        """Publish multiple messages in batch"""
        try:
            batch_messages = []
            for msg in messages:
                # Create fields for batch message
                fields = struct_pb2.Struct()
                for key, value in msg.get('fields', {}).items():
                    fields[key] = str(value)

                batch_msg = redis_streams_pb2.BatchMessage(
                    topic=msg['topic'],
                    id=msg.get('id', '*'),
                    fields=fields
                )
                batch_messages.append(batch_msg)

            request = redis_streams_pb2.PublishBatchRequest(messages=batch_messages)
            response = self.stub.PublishBatch(request)
            return True

        except grpc.RpcError as e:
            print(f"Error publishing batch: {e}")
            return False

    def read_stream(self, topic: str, count: int = 10, start_id: str = "0") -> list:
        """Read messages from stream with limit"""
        try:
            request = redis_streams_pb2.ReadStreamRequest(
                topic=topic,
                count=count,
                start_id=start_id
            )
            response = self.stub.ReadStream(request)
            return list(response.messages)

        except grpc.RpcError as e:
            print(f"Error reading stream: {e}")
            return []

    def read_range(self, topic: str, start_id: str = "0", end_id: str = "+", count: int = 50) -> list:
        """Read messages in specific range"""
        try:
            request = redis_streams_pb2.ReadRangeRequest(
                topic=topic,
                start_id=start_id,
                end_id=end_id,
                count=count
            )
            response = self.stub.ReadRange(request)
            return list(response.messages)

        except grpc.RpcError as e:
            print(f"Error reading range: {e}")
            return []

    def subscribe_stream(self, topic: str, consumer_name: str, auto_ack: bool = True) -> Iterator:
        """Subscribe to stream for real-time messages"""
        try:
            request = redis_streams_pb2.SubscribeRequest(
                topic=topic,
                consumer_name=consumer_name,
                auto_ack=auto_ack,
                batch_size=5,
                block_timeout_ms=5000
            )
            
            for message in self.stub.Subscribe(request):
                yield message

        except grpc.RpcError as e:
            print(f"Error subscribing to stream: {e}")

    def get_stream_info(self, topic: str) -> dict:
        """Get stream information"""
        try:
            request = redis_streams_pb2.StreamInfoRequest(topic=topic)
            response = self.stub.StreamInfo(request)
            return {
                'length': response.length,
                'first_entry_id': response.first_entry_id,
                'last_entry_id': response.last_entry_id,
                'entries_added': response.entries_added,
                'groups': response.groups,
                'consumers': response.consumers
            }
        except grpc.RpcError as e:
            print(f"Error getting stream info: {e}")
            return {}

    def list_topics(self) -> list:
        """List all available topics"""
        try:
            request = redis_streams_pb2.ListTopicsRequest()
            response = self.stub.ListTopics(request)
            return list(response.names)
        except grpc.RpcError as e:
            print(f"Error listing topics: {e}")
            return []


class MessageProducer:
    def __init__(self, client: RedisStreamsClient, topic: str):
        self.client = client
        self.topic = topic
        self.running = False

    def start_producing(self, interval: float = 1.0):
        """Start producing messages continuously"""
        self.running = True
        print(f"🚀 Starting message producer for topic: {self.topic}")
        
        while self.running:
            try:
                # Generate sample data
                message_data = {
                    "id": f"msg_{int(time.time() * 1000)}",
                    "timestamp": time.time(),
                    "data": f"Sample message {random.randint(1, 1000)}",
                    "source": "python-producer",
                    "value": random.uniform(10.0, 100.0)
                }

                headers = {
                    "source": "python-client",
                    "version": "1.0",
                    "timestamp": str(int(time.time()))
                }

                # Publish message
                msg_id = self.client.publish_message(self.topic, message_data, headers)
                if msg_id:
                    print(f"📤 Published message: {msg_id}")

                time.sleep(interval)

            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Error in producer: {e}")
                time.sleep(1)

        print("🛑 Producer stopped")

    def start_batch_producing(self, batch_size: int = 5, interval: float = 2.0):
        """Start producing batch messages"""
        self.running = True
        print(f"🚀 Starting batch producer for topic: {self.topic}")

        while self.running:
            try:
                # Generate batch messages
                batch_messages = []
                for i in range(batch_size):
                    message_data = {
                        "id": f"batch_msg_{int(time.time() * 1000)}_{i}",
                        "timestamp": time.time(),
                        "data": f"Batch message {i+1}",
                        "batch_id": f"batch_{int(time.time())}",
                        "value": random.uniform(1.0, 50.0)
                    }

                    batch_msg = {
                        "topic": self.topic,
                        "id": "*",
                        "fields": {
                            "data": json.dumps(message_data),
                            "content_type": "application/json",
                            "timestamp": str(int(time.time()))
                        }
                    }
                    batch_messages.append(batch_msg)

                # Publish batch
                success = self.client.publish_batch(batch_messages)
                if success:
                    print(f"📦 Published batch of {len(batch_messages)} messages")

                time.sleep(interval)

            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Error in batch producer: {e}")
                time.sleep(1)

        print("🛑 Batch producer stopped")

    def stop(self):
        """Stop the producer"""
        self.running = False


class MessageConsumer:
    def __init__(self, client: RedisStreamsClient, topic: str, consumer_name: str):
        self.client = client
        self.topic = topic
        self.consumer_name = consumer_name
        self.running = False

    def start_consuming_stream(self):
        """Start consuming messages via streaming"""
        self.running = True
        print(f"🎧 Starting stream consumer: {self.consumer_name} for topic: {self.topic}")

        try:
            for message in self.client.subscribe_stream(self.topic, self.consumer_name):
                if not self.running:
                    break
                
                print(f"📥 Stream received: ID={message.id}, Topic={message.topic}")
                # Process message fields
                for key, value in message.fields.items():
                    print(f"   {key}: {value}")

        except KeyboardInterrupt:
            pass
        except Exception as e:
            print(f"Error in stream consumer: {e}")

        print("🛑 Stream consumer stopped")

    def start_consuming_polling(self, interval: float = 2.0, batch_size: int = 10):
        """Start consuming messages via polling"""
        self.running = True
        print(f"🎧 Starting polling consumer: {self.consumer_name} for topic: {self.topic}")

        while self.running:
            try:
                # Read messages with limit
                messages = self.client.read_stream(self.topic, count=batch_size)
                
                if messages:
                    print(f"📥 Polling received {len(messages)} messages:")
                    for msg in messages:
                        print(f"   ID: {msg.id}, Topic: {msg.topic}")
                        # Process message fields
                        for key, value in msg.fields.items():
                            print(f"     {key}: {value}")
                else:
                    print("📭 No messages available")

                time.sleep(interval)

            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Error in polling consumer: {e}")
                time.sleep(1)

        print("🛑 Polling consumer stopped")

    def stop(self):
        """Stop the consumer"""
        self.running = False


def main():
    """Main function demonstrating continuous publishing and consuming"""
    print("🚀 Redis Streams MQ Python gRPC Client")
    print("=" * 50)

    # Create client
    client = RedisStreamsClient("localhost:16001")

    try:
        # List available topics
        print("📋 Available topics:")
        topics = client.list_topics()
        for topic in topics:
            print(f"   - {topic}")

        if not topics:
            print("❌ No topics found. Make sure the gRPC server is running and configured.")
            return

        # Use first topic or default
        topic = topics[0] if topics else "user.created"
        print(f"\n🎯 Using topic: {topic}")

        # Get stream info
        info = client.get_stream_info(topic)
        print(f"📊 Stream info: {info}")

        # Create producer and consumer
        producer = MessageProducer(client, topic)
        consumer = MessageConsumer(client, topic, "python-consumer")

        # Start producer in separate thread
        producer_thread = threading.Thread(
            target=producer.start_producing,
            kwargs={"interval": 1.0}
        )
        producer_thread.daemon = True
        producer_thread.start()

        # Start batch producer in separate thread
        batch_producer_thread = threading.Thread(
            target=producer.start_batch_producing,
            kwargs={"batch_size": 3, "interval": 3.0}
        )
        batch_producer_thread.daemon = True
        batch_producer_thread.start()

        # Start consumer in separate thread
        consumer_thread = threading.Thread(
            target=consumer.start_consuming_polling,
            kwargs={"interval": 2.0, "batch_size": 5}
        )
        consumer_thread.daemon = True
        consumer_thread.start()

        print("\n🔄 Running... Press Ctrl+C to stop")
        print("📤 Producer: Publishing messages every 1s")
        print("📦 Batch Producer: Publishing batches every 3s")
        print("📥 Consumer: Polling every 2s")

        # Keep main thread alive
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n🛑 Shutting down...")
            producer.stop()
            consumer.stop()
            time.sleep(2)  # Give threads time to stop

    finally:
        client.close()
        print("✅ Client closed")


if __name__ == "__main__":
    main()
