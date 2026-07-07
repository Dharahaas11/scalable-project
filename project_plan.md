# Project Plan

## Project Title
Real-Time E-Commerce Cart Abandonment Analytics Using AWS Lambda Architecture

## Project Idea
This project will analyse e-commerce user behaviour in real time. The dataset contains user events such as product views, add-to-cart actions, and transactions. The main aim is to identify cart abandonment patterns using both historical and real-time analysis.

## Dataset
RetailRocket E-commerce Dataset from Kaggle.

## Planned Architecture
- Python producer will replay dataset records as a live stream.
- AWS Kinesis will ingest the stream.
- Amazon S3 will store raw data.
- PySpark will be used for batch analysis.
- Speed layer will calculate recent cart abandonment activity.
- Final results will be shown using charts and performance graphs.

## Expected Output
- Overall cart abandonment rate
- Top abandoned products
- Recent high-risk products
- Batch and speed layer results
- Performance graphs for latency, throughput, and speedup
