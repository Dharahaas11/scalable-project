"""AWS Lambda speed-layer processor for Kinesis event batches."""

import base64
import json


def lambda_handler(event, context):
    """Decode Kinesis records and estimate abandonment for the current batch.

    A production sliding window can persist recent visitor-item state and timestamps
    in DynamoDB or S3. This submission keeps the handler stateless and reports a
    fast estimate for each incoming Lambda batch.
    """
    decoded_events = []
    failed_records = 0
    for record in event.get("Records", []):
        try:
            encoded = record["kinesis"]["data"]
            payload = base64.b64decode(encoded).decode("utf-8")
            decoded_events.append(json.loads(payload))
        except (KeyError, ValueError, json.JSONDecodeError, UnicodeDecodeError):
            failed_records += 1

    event_counts = {"view": 0, "addtocart": 0, "transaction": 0}
    for item in decoded_events:
        event_name = item.get("event")
        if event_name in event_counts:
            event_counts[event_name] += 1

    add_count = event_counts["addtocart"]
    transaction_count = event_counts["transaction"]
    estimated_abandoned = max(add_count - transaction_count, 0)
    rate = estimated_abandoned / add_count * 100 if add_count else 0.0
    result = {
        "processed_record_count": len(decoded_events),
        "failed_record_count": failed_records,
        "event_counts": event_counts,
        "addtocart_count": add_count,
        "transaction_count": transaction_count,
        "estimated_abandoned_count": estimated_abandoned,
        "estimated_abandonment_rate": round(rate, 2),
        "alert_status": "HIGH_RISK" if rate > 60 else "NORMAL",
    }
    print(json.dumps(result))
    return {
        "statusCode": 200,
        "body": json.dumps(result),
    }
