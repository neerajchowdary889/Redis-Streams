#!/usr/bin/env python3
"""
Simple test script for Redis Streams gRPC client
"""

import grpc
import time
import json
from google.protobuf import struct_pb2

# Import generated protobuf files
import redis_streams_pb2
import redis_streams_pb2_grpc


def test_basic_operations():
    """Test basic gRPC operations"""
    print("🧪 Testing Redis Streams gRPC Client")
    print("=" * 40)

    # Connect to gRPC server
    channel = grpc.insecure_channel("localhost:16001")
    stub = redis_streams_pb2_grpc.RedisStreamsStub(channel)

    try:
        # Test 1: List topics
        print("1️⃣ Listing topics...")
        topics_response = stub.ListTopics(redis_streams_pb2.ListTopicsRequest())
        print(f"   Available topics: {list(topics_response.names)}")

        if not topics_response.names:
            print("❌ No topics found. Make sure the server is configured.")
            return

        topic = topics_response.names[0]
        print(f"   Using topic: {topic}")

        # Test 2: Publish a message
        print("\n2️⃣ Publishing a message...")
        message_data = {
            "id": f"test_{int(time.time())}",
            "message": "Hello from Python!",
            "timestamp": time.time(),
            "value": 42.5
        }

        json_data = struct_pb2.Struct()
        for key, value in message_data.items():
            json_data[key] = value

        publish_response = stub.Publish(redis_streams_pb2.PublishRequest(
            topic=topic,
            json=json_data,
            headers={"source": "python-test"}
        ))
        print(f"   Published message ID: {publish_response.id}")

        # Test 3: Read messages
        print("\n3️⃣ Reading messages...")
        read_response = stub.ReadStream(redis_streams_pb2.ReadStreamRequest(
            topic=topic,
            count=5,
            start_id="0"
        ))
        print(f"   Found {len(read_response.messages)} messages")
        for i, msg in enumerate(read_response.messages):
            print(f"   Message {i+1}: ID={msg.id}")
            for key, value in msg.fields.items():
                print(f"     {key}: {value}")

        # Test 4: Publish batch
        print("\n4️⃣ Publishing batch messages...")
        batch_messages = []
        for i in range(3):
            fields = struct_pb2.Struct()
            fields["data"] = json.dumps({
                "id": f"batch_{i}",
                "message": f"Batch message {i+1}",
                "timestamp": time.time()
            })
            fields["content_type"] = "application/json"

            batch_msg = redis_streams_pb2.BatchMessage(
                topic=topic,
                id="*",
                fields=fields
            )
            batch_messages.append(batch_msg)

        batch_response = stub.PublishBatch(redis_streams_pb2.PublishBatchRequest(
            messages=batch_messages
        ))
        print(f"   Batch published successfully")

        # Test 5: Get stream info
        print("\n5️⃣ Getting stream info...")
        info_response = stub.StreamInfo(redis_streams_pb2.StreamInfoRequest(topic=topic))
        print(f"   Stream length: {info_response.length}")
        print(f"   First entry: {info_response.first_entry_id}")
        print(f"   Last entry: {info_response.last_entry_id}")
        print(f"   Groups: {info_response.groups}")
        print(f"   Consumers: {info_response.consumers}")

        print("\n✅ All tests passed!")

    except grpc.RpcError as e:
        print(f"❌ gRPC Error: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        channel.close()


if __name__ == "__main__":
    test_basic_operations()
