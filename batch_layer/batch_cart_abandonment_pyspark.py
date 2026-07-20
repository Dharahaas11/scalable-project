"""Distributed PySpark batch layer for local Spark or AWS EMR.

Local example:
  spark-submit batch_layer/batch_cart_abandonment_pyspark.py \
    --input data/events.csv --output results/pyspark_batch

EMR example (after uploading code/data to S3):
  spark-submit batch_cart_abandonment_pyspark.py \
    --input s3://BUCKET/raw/events.csv --output s3://BUCKET/results/pyspark_batch
"""

import argparse

from pyspark.sql import SparkSession, functions as F


def parse_args():
    parser = argparse.ArgumentParser(description="PySpark cart-abandonment batch job.")
    parser.add_argument("--input", default="data/events.csv")
    parser.add_argument("--output", default="results/pyspark_batch")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = (
        SparkSession.builder.appName("RetailRocketCartAbandonmentBatch")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        events = spark.read.option("header", True).option("inferSchema", True).csv(
            args.input
        )
        required = {"timestamp", "visitorid", "event", "itemid"}
        missing = required.difference(events.columns)
        if missing:
            raise ValueError(f"Input is missing required columns: {sorted(missing)}")

        events = events.withColumn(
            "event_datetime",
            F.to_timestamp(F.from_unixtime(F.col("timestamp") / F.lit(1000))),
        ).withColumn("hour", F.date_trunc("hour", F.col("event_datetime")))
        events.cache()

        event_counts = events.groupBy("event").count()
        count_map = {row["event"]: row["count"] for row in event_counts.collect()}
        total_rows = events.count()
        unique_visitors = events.select("visitorid").distinct().count()
        unique_items = events.select("itemid").distinct().count()

        pair_times = (
            events.filter(F.col("event").isin("addtocart", "transaction"))
            .groupBy("visitorid", "itemid")
            .agg(
                F.max(
                    F.when(F.col("event") == "addtocart", F.col("timestamp"))
                ).alias("last_add_timestamp"),
                F.max(
                    F.when(F.col("event") == "transaction", F.col("timestamp"))
                ).alias("last_transaction_timestamp"),
            )
        )
        add_pairs = pair_times.filter(F.col("last_add_timestamp").isNotNull())
        abandoned = add_pairs.filter(
            F.col("last_transaction_timestamp").isNull()
            | (F.col("last_transaction_timestamp") <= F.col("last_add_timestamp"))
        )
        add_pair_count = add_pairs.count()
        abandoned_count = abandoned.count()
        rate = abandoned_count / add_pair_count * 100 if add_pair_count else 0.0

        summary = spark.createDataFrame(
            [
                (
                    total_rows,
                    int(count_map.get("view", 0)),
                    int(count_map.get("addtocart", 0)),
                    int(count_map.get("transaction", 0)),
                    unique_visitors,
                    unique_items,
                    add_pair_count,
                    abandoned_count,
                    float(rate),
                )
            ],
            [
                "total_rows",
                "total_views",
                "total_addtocart_events",
                "total_transactions",
                "unique_visitors",
                "unique_items",
                "unique_addtocart_visitor_item_pairs",
                "abandoned_visitor_item_pairs",
                "overall_cart_abandonment_rate",
            ],
        )
        top_products = (
            abandoned.groupBy("itemid")
            .count()
            .withColumnRenamed("count", "abandoned_count")
            .orderBy(F.desc("abandoned_count"), F.asc("itemid"))
            .limit(10)
        )
        hourly = (
            events.groupBy("hour")
            .pivot("event", ["view", "addtocart", "transaction"])
            .count()
            .fillna(0)
            .orderBy("hour")
        )

        # Spark writes each result as a directory of part files, which scales on EMR.
        summary.coalesce(1).write.mode("overwrite").option("header", True).csv(
            f"{args.output}/batch_summary"
        )
        top_products.coalesce(1).write.mode("overwrite").option("header", True).csv(
            f"{args.output}/top_abandoned_products"
        )
        hourly.write.mode("overwrite").option("header", True).csv(
            f"{args.output}/hourly_event_trend"
        )
        print(f"PySpark results written to: {args.output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
