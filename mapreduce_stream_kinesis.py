import boto3
import pandas as pd
import json
import time
from io import StringIO


bucket = "scalableprojectamazon"
key = "input/1429_1.csv"
stream_name = "review-stream"
region = "us-east-1"

s3 = boto3.client('s3', region_name=region)
kinesis = boto3.client('kinesis', region_name=region)

print("Fetching CSV from S3...")
response = s3.get_object(Bucket=bucket, Key=key)
df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
print("Dataset loaded. Total records:", len(df))

df = df[df['reviews.text'].notna()]
records = df['reviews.text'].tolist()

batch_size = 50
delay_between_batches = 1 

print(f"Streaming data to Kinesis in batches of {batch_size}...")
for i in range(0, len(records), batch_size):
    batch = records[i:i + batch_size]
    for review in batch:
        payload = json.dumps({"text": review})
        kinesis.put_record(StreamName=stream_name, Data=payload, PartitionKey="partitionKey")
    print(f"Batch {i//batch_size + 1} sent")
    time.sleep(delay_between_batches)

print("✅ Streaming complete.")
