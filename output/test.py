import json

with open('redis_streams_data.json', 'r') as f:
    data = json.load(f)

print(len(data))
