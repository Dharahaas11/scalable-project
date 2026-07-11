"""Pandas implementation of the historical batch layer."""

import argparse
import sys
from pathlib import Path

import pandas as pd


REQUIRED_COLUMNS = {"timestamp", "visitorid", "event", "itemid"}


def parse_args():
    parser = argparse.ArgumentParser(description="Run local batch cart-abandonment analysis.")
    parser.add_argument("--input", default="data/events.csv", help="Path to events.csv")
    parser.add_argument("--output", default="results", help="Output directory")
    return parser.parse_args()


def main():
    args = parse_args()
    input_path, output_dir = Path(args.input), Path(args.output)
    if not input_path.is_file():
        print(f"Error: input file not found: {input_path}", file=sys.stderr)
        return 1
    output_dir.mkdir(parents=True, exist_ok=True)

    data = pd.read_csv(input_path)
    missing = REQUIRED_COLUMNS.difference(data.columns)
    if missing:
        print(f"Error: missing required columns: {sorted(missing)}", file=sys.stderr)
        return 1

    data["datetime"] = pd.to_datetime(data["timestamp"], unit="ms", errors="coerce")
    data["date"] = data["datetime"].dt.date
    data["hour"] = data["datetime"].dt.floor("h")
    event_counts = data["event"].value_counts()

    pair_times = data[data["event"].isin(["addtocart", "transaction"])].pivot_table(
        index=["visitorid", "itemid"], columns="event", values="timestamp", aggfunc="max"
    )
    add_pairs = pair_times[pair_times["addtocart"].notna()].copy()
    transaction_times = add_pairs.get(
        "transaction", pd.Series(index=add_pairs.index, dtype=float)
    )
    abandoned = add_pairs[
        transaction_times.isna() | (transaction_times <= add_pairs["addtocart"])
    ].copy()
    abandonment_rate = len(abandoned) / len(add_pairs) * 100 if len(add_pairs) else 0.0

    summary = pd.DataFrame(
        [
            {
                "total_rows": len(data),
                "total_views": int(event_counts.get("view", 0)),
                "total_addtocart_events": int(event_counts.get("addtocart", 0)),
                "total_transactions": int(event_counts.get("transaction", 0)),
                "unique_visitors": data["visitorid"].nunique(),
                "unique_items": data["itemid"].nunique(),
                "unique_addtocart_visitor_item_pairs": len(add_pairs),
                "abandoned_visitor_item_pairs": len(abandoned),
                "overall_cart_abandonment_rate": round(abandonment_rate, 4),
            }
        ]
    )
    summary.to_csv(output_dir / "batch_summary.csv", index=False)

    top_products = (
        abandoned.reset_index()["itemid"]
        .value_counts()
        .head(10)
        .rename_axis("itemid")
        .reset_index(name="abandoned_count")
    )
    top_products.to_csv(output_dir / "top_abandoned_products.csv", index=False)

    hourly = (
        data.groupby(["hour", "event"]).size().unstack(fill_value=0).reset_index()
    )
    for event_name in ("view", "addtocart", "transaction"):
        if event_name not in hourly:
            hourly[event_name] = 0
    hourly = hourly.rename(
        columns={
            "view": "view_count",
            "addtocart": "addtocart_count",
            "transaction": "transaction_count",
        }
    )
    hourly["total_event_count"] = (
        hourly["view_count"] + hourly["addtocart_count"] + hourly["transaction_count"]
    )
    hourly.to_csv(output_dir / "hourly_event_trend.csv", index=False)

    abandoned_pairs = abandoned.reset_index()[["visitorid", "itemid"]]
    add_events = data[data["event"] == "addtocart"][
        ["visitorid", "itemid", "hour"]
    ].drop_duplicates(["visitorid", "itemid"])
    abandoned_adds = add_events.merge(
        abandoned_pairs, on=["visitorid", "itemid"], how="inner"
    )
    hourly_adds = add_events.groupby("hour").size().rename("addtocart_pair_count")
    hourly_abandoned = (
        abandoned_adds.groupby("hour").size().rename("abandoned_pair_count")
    )
    hourly_abandonment = pd.concat([hourly_adds, hourly_abandoned], axis=1).fillna(0)
    hourly_abandonment["abandonment_rate"] = (
        hourly_abandonment["abandoned_pair_count"]
        / hourly_abandonment["addtocart_pair_count"].replace(0, pd.NA)
        * 100
    ).fillna(0)
    hourly_abandonment.reset_index().to_csv(
        output_dir / "hourly_abandonment_trend.csv", index=False
    )

    print("\nBatch analysis complete")
    for column, value in summary.iloc[0].items():
        if column == "overall_cart_abandonment_rate":
            print(f"  {column}: {value:.2f}%")
        else:
            print(f"  {column}: {int(value):,}")
    print(f"\nCSV results saved in: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
