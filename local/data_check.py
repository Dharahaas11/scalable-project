"""Inspect the RetailRocket event data and estimate cart abandonment."""

import argparse
import sys
from pathlib import Path

import pandas as pd


REQUIRED_COLUMNS = {"timestamp", "visitorid", "event", "itemid", "transactionid"}


def parse_args():
    parser = argparse.ArgumentParser(description="Check the RetailRocket events dataset.")
    parser.add_argument("--input", default="data/events.csv", help="Path to events.csv")
    return parser.parse_args()


def main():
    args = parse_args()
    input_path = Path(args.input)
    if not input_path.is_file():
        print(f"Error: input file not found: {input_path}", file=sys.stderr)
        return 1

    data = pd.read_csv(input_path)
    missing_columns = REQUIRED_COLUMNS.difference(data.columns)
    if missing_columns:
        print(f"Error: missing required columns: {sorted(missing_columns)}", file=sys.stderr)
        return 1

    print(f"Total rows: {len(data):,}")
    print(f"Column names: {list(data.columns)}")
    print("\nFirst 5 rows:")
    print(data.head().to_string(index=False))
    print("\nEvent type counts:")
    print(data["event"].value_counts(dropna=False).to_string())
    print("\nMissing values:")
    print(data.isna().sum().to_string())

    counts = data["event"].value_counts()
    print(f"\nTotal view events: {int(counts.get('view', 0)):,}")
    print(f"Total addtocart events: {int(counts.get('addtocart', 0)):,}")
    print(f"Total transaction events: {int(counts.get('transaction', 0)):,}")

    pair_times = data[data["event"].isin(["addtocart", "transaction"])].pivot_table(
        index=["visitorid", "itemid"],
        columns="event",
        values="timestamp",
        aggfunc="max",
    )
    add_pairs = pair_times[pair_times["addtocart"].notna()].copy()
    abandoned = add_pairs[
        add_pairs.get("transaction", pd.Series(index=add_pairs.index, dtype=float)).isna()
        | (add_pairs["transaction"] <= add_pairs["addtocart"])
    ]
    rate = (len(abandoned) / len(add_pairs) * 100) if len(add_pairs) else 0.0
    print(f"\nUnique add-to-cart visitor-item pairs: {len(add_pairs):,}")
    print(f"Abandoned visitor-item pairs: {len(abandoned):,}")
    print(f"Approximate cart abandonment rate: {rate:.2f}%")
    print("\nTop 10 abandoned item IDs:")
    print(
        abandoned.reset_index()["itemid"]
        .value_counts()
        .head(10)
        .rename_axis("itemid")
        .reset_index(name="abandoned_count")
        .to_string(index=False)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
