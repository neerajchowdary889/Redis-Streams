#!/usr/bin/env python3
"""
Generate Python protobuf files from .proto
"""

import subprocess
import sys
import os

def generate_protos():
    """Generate Python protobuf files"""
    proto_file = "../api/proto/redis_streams.proto"
    output_dir = "."
    
    if not os.path.exists(proto_file):
        print(f"❌ Proto file not found: {proto_file}")
        return False
    
    try:
        cmd = [
            "python", "-m", "grpc_tools.protoc",
            f"--proto_path=../api/proto",
            f"--python_out={output_dir}",
            f"--grpc_python_out={output_dir}",
            proto_file
        ]
        
        print("🔧 Generating Python protobuf files...")
        print(f"Command: {' '.join(cmd)}")
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Python protobuf files generated successfully!")
            print("Generated files:")
            for file in ["redis_streams_pb2.py", "redis_streams_pb2_grpc.py"]:
                if os.path.exists(file):
                    print(f"   - {file}")
            return True
        else:
            print(f"❌ Error generating protobuf files:")
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False

if __name__ == "__main__":
    success = generate_protos()
    sys.exit(0 if success else 1)
