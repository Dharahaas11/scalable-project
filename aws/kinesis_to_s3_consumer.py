"""Read Kinesis records into JSONL and optionally upload the file to S3."""

import argparse
import base64
import json
import sys
import time
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError


def parse_args():
    parser = argparse.ArgumentParser(description="Consume events from Kinesis.")
    parser.add_argument("--stream-name", default="ecommerce-cart-events")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--output", default="results/kinesis_events.jsonl")
    parser.add_argument("--s3-bucket", default=None)
    parser.add_argument("--s3-prefix", default="raw-events")
    parser.add_argument("--limit", type=int, default=1000)
    return parser.parse_args()


def main():
    args = parse_args()
    if args.limit <= 0:
        print("Error: --limit must be positive.", file=sys.stderr)
        return 1
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    kinesis = boto3.client("kinesis", region_name=args.region)

    try:
        description = kinesis.describe_stream_summary(StreamName=args.stream_name)
        shard_count = description["StreamDescriptionSummary"]["OpenShardCount"]
        shards = kinesis.list_shards(StreamName=args.stream_name).get("Shards", [])
        if not shards:
            print("No open shards were found.", file=sys.stderr)
            return 1
        print(f"Reading up to {args.limit} records from {shard_count} shard(s).")

        written = 0
        with output_path.open("w", encoding="utf-8") as output_file:
            for shard in shards:
                iterator = kinesis.get_shard_iterator(
                    StreamName=args.stream_name,
                    ShardId=shard["ShardId"],
                    ShardIteratorType="TRIM_HORIZON",
                )["ShardIterator"]
                empty_reads = 0
                while iterator and written < args.limit and empty_reads < 5:
                    response = kinesis.get_records(ShardIterator=iterator, Limit=100)
                    iterator = response.get("NextShardIterator")
                    records = response.get("Records", [])
                    empty_reads = empty_reads + 1 if not records else 0
                    for record in records:
                        if written >= args.limit:
                            break
                        payload = json.loads(
                            base64.b64decode(
                                base64.b64encode(record["Data"])
                            ).decode("utf-8")
                        )
                        output_file.write(json.dumps(payload) + "\n")
                        written += 1
                    if not records:
                        time.sleep(1)
                if written >= args.limit:
                    break
        print(f"Wrote {written} records to {output_path}")

        if args.s3_bucket:
            s3 = boto3.client("s3", region_name=args.region)
            prefix = args.s3_prefix.strip("/")
            key = f"{prefix}/{output_path.name}" if prefix else output_path.name
            s3.upload_file(str(output_path), args.s3_bucket, key)
            print(f"Uploaded to s3://{args.s3_bucket}/{key}")
    except (BotoCoreError, ClientError, KeyError, json.JSONDecodeError) as error:
        print(f"Consumer error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
