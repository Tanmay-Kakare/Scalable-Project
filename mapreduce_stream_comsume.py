import boto3
import json
import time

stream_name = "review-stream"
region = "us-east-1"

client = boto3.client("kinesis", region_name=region)
stream = client.describe_stream(StreamName=stream_name)
shard_id = stream["StreamDescription"]["Shards"][0]["ShardId"]
shard_iterator = client.get_shard_iterator(
    StreamName=stream_name,
    ShardId=shard_id,
    ShardIteratorType="TRIM_HORIZON"
)["ShardIterator"]

print("Checking for records in the stream...")
while True:
    response = client.get_records(ShardIterator=shard_iterator, Limit=10)
    shard_iterator = response["NextShardIterator"]
    for record in response["Records"]:
        print("Record:", record["Data"].decode("utf-8"))
    if not response["Records"]:
        print("No new records.")
        break
    time.sleep(2)
