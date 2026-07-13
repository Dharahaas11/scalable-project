"""Simulate a speed layer using five-minute windows sliding every minute."""

import argparse
import json
import sys
from pathlib import Path

import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(description="Run sliding-window stream simulation.")
    parser.add_argument("--input", default="data/events.csv")
    parser.add_argument("--limit", type=int, default=50_000)
    parser.add_argument("--window-minutes", type=int, default=5)
    parser.add_argument("--output", default="results")
    return parser.parse_args()


def main():
    args = parse_args()
    path, output_dir = Path(args.input), Path(args.output)
    if not path.is_file():
        print(f"Error: input file not found: {path}", file=sys.stderr)
        return 1
    if args.limit <= 0 or args.window_minutes <= 0:
        print("Error: --limit and --window-minutes must be positive.", file=sys.stderr)
        return 1
    output_dir.mkdir(parents=True, exist_ok=True)

    data = pd.read_csv(path, nrows=args.limit)
    required = {"timestamp", "visitorid", "event", "itemid"}
    if not required.issubset(data.columns):
        print(f"Error: required columns are {sorted(required)}", file=sys.stderr)
        return 1
    data["datetime"] = pd.to_datetime(data["timestamp"], unit="ms", errors="coerce")
    data = data.dropna(subset=["datetime"]).sort_values("datetime")
    if data.empty:
        print("Error: no valid timestamped records found.", file=sys.stderr)
        return 1

    window_delta = pd.Timedelta(minutes=args.window_minutes)
    first_end = data["datetime"].min().floor("min") + window_delta
    last_end = data["datetime"].max().ceil("min")
    window_ends = pd.date_range(first_end, last_end, freq="1min")
    rows = []
    for window_end in window_ends:
        window_start = window_end - window_delta
        window = data[
            (data["datetime"] >= window_start) & (data["datetime"] < window_end)
        ]
        if window.empty:
            continue
        adds = window[window["event"] == "addtocart"]
        transactions = window[window["event"] == "transaction"]
        add_pairs = adds[["visitorid", "itemid"]].drop_duplicates()
        transaction_pairs = transactions[["visitorid", "itemid"]].drop_duplicates()
        abandoned_pairs = add_pairs.merge(
            transaction_pairs,
            on=["visitorid", "itemid"],
            how="left",
            indicator=True,
        )
        abandoned_pairs = abandoned_pairs[abandoned_pairs["_merge"] == "left_only"]
        estimated_rate = len(abandoned_pairs) / len(add_pairs) * 100 if len(add_pairs) else 0.0
        top_items = (
            abandoned_pairs["itemid"].value_counts().head(5).index.astype(str).tolist()
        )
        rows.append(
            {
                "window_start": window_start,
                "window_end": window_end,
                "event_count": len(window),
                "addtocart_count": len(adds),
                "transaction_count": len(transactions),
                "estimated_abandoned_count": len(abandoned_pairs),
                "estimated_abandonment_rate": round(estimated_rate, 4),
                "top_5_abandoned_item_ids": json.dumps(top_items),
                "alert_status": "HIGH_RISK" if estimated_rate > 60 else "NORMAL",
            }
        )

    results = pd.DataFrame(rows)
    results.to_csv(output_dir / "speed_window_results.csv", index=False)
    print(f"Processed {len(data):,} records into {len(results):,} sliding windows.")
    print("\nMost recent window summaries:")
    print(results.tail(10).to_string(index=False))
    print(f"\nSaved: {output_dir / 'speed_window_results.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
