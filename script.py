import boto3
import json
import ray
import os
import random
import string
import logging
import sys

logging.basicConfig(level=logging.INFO)

s3 = boto3.client("s3")

@ray.remote
def process_file(bucket, key, out_bucket):
    try:
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(obj["Body"].read())

        if 'base_words' in data and isinstance(data['base_words'], list):
            data['base_words'].append(''.join(random.choices(string.ascii_lowercase, k=2)))

        s3.put_object(
            Bucket=out_bucket,
            Key=key,
            Body=json.dumps(data).encode('utf-8'),
            ContentType='application/json'
        )
        return {"key": key, "status": "success"}
    except Exception as e:
        logging.error(f"Failed to process {key}: {e}")
        return {"key": key, "status": "error", "error": str(e)}

def list_all_keys(bucket):
    """バケット内のすべてのキーを取得（ページネーション対応）"""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        contents = page.get("Contents", [])
        for obj in contents:
            keys.append(obj["Key"])
    return keys

if __name__ == "__main__":
    ray.init()
    input_bucket = os.environ['INPUT_BUCKET']
    output_bucket = os.environ['OUTPUT_BUCKET']

    keys = list_all_keys(input_bucket)
    print(f"Found {len(keys)} files in {input_bucket}")

    tasks = [process_file.remote(input_bucket, key, output_bucket) for key in keys]
    results = ray.get(tasks)

    success_count = sum(1 for r in results if r['status'] == 'success')
    error_count = len(results) - success_count
    print(f"Completed. Success: {success_count}, Error: {error_count}")

    if error_count > 0:
        sys.exit(1)
    else:
        sys.exit(0)
