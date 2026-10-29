"""Sample event-driven app: POST /items -> process-item -> (Items table, events-queue) -> consume-event -> Processed table.

It deliberately has resilience gaps (no retries, no DLQ, no idempotency) for the chaos tool to find.
The only chaos-related lines are the `chaos_lambda` decorators and using the adapters instead of raw boto3.
"""
import json
import uuid

from serverless_chaos.adapters import build_adapters
from serverless_chaos.runtime import chaos_lambda

ad = build_adapters()          # endpoint + control table come from env (set by deploy.py); one control-plane client per Lambda


@chaos_lambda("process-item", runtime=ad.runtime)
def process_item(event, context):
    item_id = event.get("id") or uuid.uuid4().hex
    ad.storage.put("Items", {"id": item_id, "payload": event.get("payload", "")})
    ad.queue.send("events-queue", {"id": item_id})
    return {"statusCode": 200, "body": json.dumps({"id": item_id})}


@chaos_lambda("consume-event", runtime=ad.runtime)
def consume_event(event, context):
    for rec in event["Records"]:                       # SQS event source mapping
        body = json.loads(rec["body"])
        ad.storage.put("Processed", {"id": body["id"], "status": "done"})
    return {"processed": len(event["Records"])}
