"""Replay RetailRocket events into an AWS Kinesis Data Stream."""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError


def parse_args():
    parser = argparse.ArgumentParser(description="Send CSV events to Kinesis.")
    parser.add_argument("--input", default="data/events.csv")
    parser.add_argument("--stream-name", default="ecommerce-cart-events")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--delay", type=float, default=0.01)
    return parser.parse_args()


def clean_record(record):
    clean = {}
    for key, value in record.items():
        if pd.isna(value):
            clean[key] = None
        elif hasattr(value, "item"):
            clean[key] = value.item()
        else:
            clean[key] = value
    clean["ingestion_time"] = datetime.now(timezone.utc).isoformat()
    return clean


def main():
    args = parse_args()
    path = Path(args.input)
    if not path.is_file():
        print(f"Error: input file not found: {path}", file=sys.stderr)
        return 1
    if args.limit <= 0 or args.delay < 0:
        print("Error: --limit must be positive and --delay non-negative.", file=sys.stderr)
        return 1

    # boto3 obtains credentials from the AWS Academy environment; none are stored here.
    client = boto3.client("kinesis", region_name=args.region)
    successful = failed = 0
    try:
        data = pd.read_csv(path, nrows=args.limit)
        for raw_record in data.to_dict(orient="records"):
            record = clean_record(raw_record)
            try:
                client.put_record(
                    StreamName=args.stream_name,
                    Data=json.dumps(record).encode("utf-8"),
                    PartitionKey=str(record["visitorid"]),
                )
                successful += 1
            except (BotoCoreError, ClientError) as error:
                failed += 1
                print(f"Failed to send record: {error}", file=sys.stderr)
            time.sleep(args.delay)
    except (BotoCoreError, ClientError) as error:
        print(f"AWS error: {error}", file=sys.stderr)
        return 1
    finally:
        print(f"Successful records: {successful}")
        print(f"Failed records: {failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
