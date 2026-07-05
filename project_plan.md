# Project Plan

## Problem statement

E-commerce teams need to detect when shoppers add products to carts but leave
without purchasing. Historical analysis alone is accurate but slow, while
stream-only estimates cannot efficiently rebuild a long history.

## Real-time question

What is the recent cart-abandonment rate, which products are most affected, and
does the current rate exceed the 60% high-risk threshold?

## Objectives

- Replay a public clickstream at controlled rates through local and AWS ingestion.
- calculate accurate historical abandonment with pandas and distributed PySpark;
- calculate low-latency estimates with sliding windows and AWS Lambda batches;
- combine historical and recent values in a serving layer;
- visualise trends and evaluate throughput, latency, and parallel speedup;
- demonstrate scaling choices compatible with AWS Academy Learner Lab.

## Dataset description

The RetailRocket `events.csv` contains Unix-millisecond timestamps, visitor IDs,
event types, item IDs, and transaction IDs. Only this file is the analytical
input. `category_tree.csv` is optional metadata. Large item-property files are
outside project scope.

## Why Lambda Architecture is suitable

The batch layer recomputes reliable metrics from all immutable events. The speed
layer provides a recent answer before batch recomputation completes. The serving
layer exposes both, balancing accuracy, scale, and low latency.

## Architecture workflow

`events.csv → controlled replay → Kinesis → S3 raw history → EMR/PySpark batch`

`Kinesis → Lambda/recent-window state → speed results`

`batch results + speed results → serving view → CSV reports and graphs`

## Batch layer plan

Validate locally with pandas, then use PySpark on EMR to group the full history
by visitor-item pair. A pair is abandoned when its last add-to-cart has no later
transaction. Produce summary, product, and hourly datasets.

## Speed layer plan

Use a five-minute configurable window sliding once per minute for local
simulation. Estimate recent abandoned pairs, list top items, and raise
`HIGH_RISK` above 60%. The Lambda prototype handles Kinesis batches; a production
version stores window state in DynamoDB or S3.

## Serving layer plan

Read the latest speed window and the batch summary, then publish a compact
serving view and a readable alert statement. A deployed version can store these
records in DynamoDB or expose them through API Gateway.

## Performance evaluation plan

- Replay multiple ingestion rates and record end-to-end latency.
- Sample achieved throughput over time from producer and CloudWatch metrics.
- Run the same PySpark workload with increasing EMR worker counts.
- Calculate speedup as `one-worker execution time / N-worker execution time`.
- Plot latency versus ingestion rate, throughput over time, and speedup versus
  worker count. Synthetic values are scaffolding only and should be replaced by
  recorded experiment values in the final report.

## AWS services planned

- Amazon Kinesis Data Streams for ingestion
- Amazon S3 for raw events and durable results
- AWS Lambda for low-latency event processing
- Amazon EMR with Spark for distributed batch processing
- DynamoDB or S3 for speed-window state and serving data
- Amazon CloudWatch for metrics and alarms
- Kinesis on-demand/shard scaling, Lambda concurrency, and EMR managed scaling

## Deliverables

- Python source code and dependency file
- GitHub repository without dataset or credentials
- Generated result CSVs and six report-ready graphs
- Lambda Architecture and benchmark evidence
- IEEE-format report and demonstration video
