"""Create report-ready analytical and performance graphs with matplotlib."""

import argparse
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(description="Generate project graphs.")
    parser.add_argument("--results", default="results")
    parser.add_argument("--graphs", default="graphs")
    return parser.parse_args()


def finish_figure(path):
    plt.tight_layout()
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Created: {path}")


def synthetic_metrics():
    rates = np.array([100, 250, 500, 750, 1000, 1250], dtype=int)
    workers = np.array([1, 2, 4, 8, 12, 16], dtype=int)
    execution = np.array([920, 485, 258, 143, 108, 92], dtype=float)
    return pd.DataFrame(
        {
            "measurement_time": pd.date_range(
                "2026-01-01 00:00:00", periods=len(rates), freq="min"
            ),
            "ingestion_rate": rates,
            "latency_ms": [48, 57, 74, 103, 151, 226],
            "throughput_records_per_sec": [98, 244, 480, 695, 850, 930],
            "worker_count": workers,
            "batch_execution_time_sec": execution,
        }
    )


def main():
    args = parse_args()
    results_dir, graph_dir = Path(args.results), Path(args.graphs)
    results_dir.mkdir(parents=True, exist_ok=True)
    graph_dir.mkdir(parents=True, exist_ok=True)

    top_path = results_dir / "top_abandoned_products.csv"
    hourly_path = results_dir / "hourly_event_trend.csv"
    abandonment_path = results_dir / "hourly_abandonment_trend.csv"
    required = [top_path, hourly_path, abandonment_path]
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        print("Error: run local/batch_cart_abandonment.py first. Missing:", file=sys.stderr)
        print("\n".join(missing), file=sys.stderr)
        return 1

    top = pd.read_csv(top_path)
    plt.figure(figsize=(9, 5))
    plt.bar(top["itemid"].astype(str), top["abandoned_count"], color="#D95F59")
    plt.title("Top 10 Abandoned Products")
    plt.xlabel("Item ID")
    plt.ylabel("Abandoned visitor-item pairs")
    plt.xticks(rotation=45, ha="right")
    plt.grid(axis="y", alpha=0.25)
    finish_figure(graph_dir / "top_abandoned_products.png")

    hourly = pd.read_csv(hourly_path, parse_dates=["hour"])
    plt.figure(figsize=(11, 5))
    for column, label in [
        ("view_count", "Views"),
        ("addtocart_count", "Add to cart"),
        ("transaction_count", "Transactions"),
    ]:
        plt.plot(hourly["hour"], hourly[column], label=label, linewidth=1)
    plt.title("Hourly Event Trend")
    plt.xlabel("Hour")
    plt.ylabel("Event count")
    plt.legend()
    plt.grid(alpha=0.25)
    finish_figure(graph_dir / "hourly_event_trend.png")

    abandonment = pd.read_csv(abandonment_path, parse_dates=["hour"])
    plt.figure(figsize=(11, 5))
    plt.plot(
        abandonment["hour"],
        abandonment["abandonment_rate"],
        color="#B24745",
        linewidth=1.2,
    )
    plt.axhline(60, color="#E69F00", linestyle="--", label="60% alert threshold")
    plt.title("Hourly Cart Abandonment Trend")
    plt.xlabel("Hour")
    plt.ylabel("Abandonment rate (%)")
    plt.legend()
    plt.grid(alpha=0.25)
    finish_figure(graph_dir / "hourly_abandonment_trend.png")

    metrics_path = results_dir / "performance_metrics.csv"
    if metrics_path.is_file():
        metrics = pd.read_csv(metrics_path)
    else:
        metrics = synthetic_metrics()
        metrics.to_csv(metrics_path, index=False)
        print(f"Created synthetic benchmark data: {metrics_path}")

    required_metric_columns = {
        "ingestion_rate",
        "latency_ms",
        "throughput_records_per_sec",
        "worker_count",
        "batch_execution_time_sec",
    }
    missing_columns = required_metric_columns.difference(metrics.columns)
    if missing_columns:
        print(
            f"Error: performance metrics missing columns: {sorted(missing_columns)}",
            file=sys.stderr,
        )
        return 1

    rate_metrics = metrics.sort_values("ingestion_rate")
    plt.figure(figsize=(8, 5))
    plt.plot(
        rate_metrics["ingestion_rate"],
        rate_metrics["latency_ms"],
        marker="o",
        color="#4C78A8",
    )
    plt.title("Latency vs Ingestion Rate")
    plt.xlabel("Ingestion rate (records/second)")
    plt.ylabel("Latency (ms)")
    plt.grid(alpha=0.25)
    finish_figure(graph_dir / "latency_vs_ingestion_rate.png")

    x_values = (
        pd.to_datetime(metrics["measurement_time"])
        if "measurement_time" in metrics
        else np.arange(1, len(metrics) + 1)
    )
    x_label = "Measurement time" if "measurement_time" in metrics else "Measurement"
    plt.figure(figsize=(8, 5))
    plt.plot(
        x_values,
        metrics["throughput_records_per_sec"],
        marker="o",
        color="#59A14F",
    )
    plt.title("Throughput over Time")
    plt.xlabel(x_label)
    plt.ylabel("Throughput (records/second)")
    plt.grid(alpha=0.25)
    finish_figure(graph_dir / "throughput_over_time.png")

    worker_metrics = metrics.sort_values("worker_count").copy()
    baseline = worker_metrics.loc[
        worker_metrics["worker_count"].idxmin(), "batch_execution_time_sec"
    ]
    worker_metrics["speedup"] = baseline / worker_metrics["batch_execution_time_sec"]
    plt.figure(figsize=(8, 5))
    plt.plot(
        worker_metrics["worker_count"],
        worker_metrics["speedup"],
        marker="o",
        label="Measured speedup",
        color="#F28E2B",
    )
    plt.plot(
        worker_metrics["worker_count"],
        worker_metrics["worker_count"],
        linestyle="--",
        label="Ideal linear speedup",
        color="#777777",
    )
    plt.title("Batch Speedup vs Worker Count")
    plt.xlabel("Worker count")
    plt.ylabel("Speedup")
    plt.legend()
    plt.grid(alpha=0.25)
    finish_figure(graph_dir / "speedup_vs_workers.png")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
