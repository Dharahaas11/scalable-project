"""Replay CSV rows locally to simulate a live event producer."""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(description="Replay events as JSON.")
    parser.add_argument("--input", default="data/events.csv")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--delay", type=float, default=0.1)
    return parser.parse_args()


def clean_record(record):
    return {key: (None if pd.isna(value) else value) for key, value in record.items()}


def main():
    args = parse_args()
    path = Path(args.input)
    if not path.is_file():
        print(f"Error: input file not found: {path}", file=sys.stderr)
        return 1
    if args.limit < 0 or args.delay < 0:
        print("Error: --limit and --delay must be non-negative.", file=sys.stderr)
        return 1

    for chunk in pd.read_csv(path, chunksize=max(1, min(args.limit, 10_000))):
        for record in chunk.to_dict(orient="records"):
            record = clean_record(record)
            record["ingestion_time"] = datetime.now(timezone.utc).isoformat()
            print(json.dumps(record, default=str), flush=True)
            time.sleep(args.delay)
            args.limit -= 1
            if args.limit == 0:
                return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
