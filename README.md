# Real-Time E-Commerce Cart Abandonment Analytics Using AWS Lambda Architecture

MSc Cloud Computing — Scalable Cloud Programming, National College of Ireland

## Project overview

This Python project analyses e-commerce clickstream events to identify products
that visitors add to a cart but do not later purchase. It provides a complete
academic prototype of Lambda Architecture: replayed ingestion, historical batch
processing, recent sliding-window processing, a merged serving view, visual
results, and benchmark graphs.

The project uses the
[RetailRocket E-Commerce Dataset](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset).
The main input is `data/events.csv`, whose events are `view`, `addtocart`, and
`transaction`. `data/category_tree.csv` is optional metadata and is not needed by
the supplied jobs.

## Lambda Architecture

1. **Ingestion:** a local producer replays CSV records; the AWS producer sends
   the same JSON records to Amazon Kinesis Data Streams.
2. **Batch layer:** pandas supports local validation, while PySpark processes
   the full history locally or on Amazon EMR.
3. **Speed layer:** the local simulation uses a configurable window sliding once
   per minute. An AWS Lambda handler calculates low-latency batch estimates from
   Kinesis records.
4. **Serving layer:** historical metrics and the latest speed-window result are
   merged into one query-friendly CSV view and alert summary.
5. **Visualisation:** matplotlib generates product, trend, latency, throughput,
   and scaling graphs.

Abandonment in the historical jobs means that the latest add-to-cart event for
a visitor-item pair has no later transaction for the same pair. Speed-layer
results are recent-window estimates and are labelled accordingly.

## Folder structure

```text
.
├── local/          # Local checks, batch/speed simulations and graphs
├── aws/            # Kinesis producer and consumer
├── batch_layer/    # Distributed PySpark batch job
├── speed_layer/    # AWS Lambda handler
├── serving_layer/  # Batch and speed merge
├── data/           # Downloaded dataset (not committed)
├── results/        # Generated CSV/JSONL outputs
└── graphs/         # Generated PNG figures
```

## Local execution

Run these commands from the project root:

```bash
pip install -r requirements.txt
python local/data_check.py --input data/events.csv
python local/batch_cart_abandonment.py --input data/events.csv --output results
python local/producer_simulation.py --input data/events.csv --limit 100 --delay 0.1
python local/speed_layer_simulation.py --input data/events.csv --limit 50000 --window-minutes 5 --output results
python serving_layer/serving_layer_merge.py --results results
python local/performance_graphs.py --results results --graphs graphs
```

Optional distributed local run:

```bash
spark-submit batch_layer/batch_cart_abandonment_pyspark.py --input data/events.csv --output results/pyspark_batch
```

## AWS execution plan

- Create a Kinesis stream named `ecommerce-cart-events` in the Learner Lab.
- Run `aws/kinesis_producer.py` from an authenticated Learner Lab environment.
- Consume raw events with `aws/kinesis_to_s3_consumer.py` and optionally archive
  its JSONL file in S3.
- Upload `events.csv` privately to S3 and submit the PySpark job as an EMR step.
- Connect Kinesis to the Lambda handler for speed estimates. For a durable
  production sliding window, store recent state in DynamoDB or S3.
- Store batch and speed outputs in S3/DynamoDB for a deployed serving API.
- Use Kinesis on-demand mode or shard scaling, Lambda reserved concurrency, and
  EMR managed scaling to demonstrate auto-scaling.
- Collect CloudWatch metrics at several replay rates and worker counts, then
  replace the synthetic `performance_metrics.csv` values with measured results.

boto3 uses the default AWS Academy credential chain. No access keys or tokens
belong in source code or Git.

## Expected outputs

`results/` contains batch summaries, top products, hourly trends, speed windows,
the serving view, final alert summary, and performance metrics.

`graphs/` contains six PNG figures: top abandoned products, hourly events,
hourly abandonment, latency versus ingestion rate, throughput over time, and
speedup versus workers.

## Repository

GitHub repository: `https://github.com/Dharahaas11/scalable-project.git`

Commit source code, documentation, dependency metadata, and small representative
outputs if required. Do **not** commit the full RetailRocket dataset, AWS
credentials, Learner Lab tokens, large Spark output directories, or generated
raw Kinesis captures. The included `.gitignore` helps prevent this.
