"""Combine historical batch metrics with the latest speed-layer metrics."""

import argparse
import ast
import sys
from pathlib import Path

import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(description="Build the serving-layer view.")
    parser.add_argument("--results", default="results", help="Results directory")
    return parser.parse_args()


def first_recent_item(value):
    try:
        values = ast.literal_eval(str(value))
        return values[0] if values else ""
    except (ValueError, SyntaxError):
        return ""


def main():
    args = parse_args()
    results_dir = Path(args.results)
    paths = {
        "summary": results_dir / "batch_summary.csv",
        "top": results_dir / "top_abandoned_products.csv",
        "speed": results_dir / "speed_window_results.csv",
    }
    missing = [str(path) for path in paths.values() if not path.is_file()]
    if missing:
        print("Error: run the batch and speed scripts first. Missing:", file=sys.stderr)
        print("\n".join(missing), file=sys.stderr)
        return 1

    batch = pd.read_csv(paths["summary"]).iloc[0]
    top = pd.read_csv(paths["top"])
    speed = pd.read_csv(paths["speed"])
    if speed.empty:
        print("Error: speed_window_results.csv contains no windows.", file=sys.stderr)
        return 1
    latest = speed.sort_values("window_end").iloc[-1]
    historical_item = top.iloc[0]["itemid"] if not top.empty else ""
    recent_item = first_recent_item(latest.get("top_5_abandoned_item_ids", "[]"))

    view = pd.DataFrame(
        [
            {
                "historical_abandonment_rate": batch["overall_cart_abandonment_rate"],
                "latest_speed_window_abandonment_rate": latest[
                    "estimated_abandonment_rate"
                ],
                "latest_alert_status": latest["alert_status"],
                "total_historical_addtocart_events": batch["total_addtocart_events"],
                "total_historical_transactions": batch["total_transactions"],
                "top_historical_abandoned_item": historical_item,
                "recent_top_abandoned_item": recent_item,
            }
        ]
    )
    view.to_csv(results_dir / "serving_view.csv", index=False)

    message = (
        f"Historical abandonment was {batch['overall_cart_abandonment_rate']:.2f}%. "
        f"The latest {latest['window_start']} to {latest['window_end']} window was "
        f"{latest['estimated_abandonment_rate']:.2f}% ({latest['alert_status']}). "
        f"Top historical abandoned item: {historical_item}; "
        f"recent top abandoned item: {recent_item or 'not available'}."
    )
    pd.DataFrame(
        [
            {
                "generated_from_window_end": latest["window_end"],
                "alert_status": latest["alert_status"],
                "report_summary": message,
            }
        ]
    ).to_csv(results_dir / "final_alert_summary.csv", index=False)
    print(message)
    print(f"Serving-layer files saved in: {results_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
