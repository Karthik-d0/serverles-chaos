"""Deploy the sample app to LocalStack. Idempotent: safe to run any number of times.

    python examples/items_app/deploy.py [--endpoint URL] [--lambda-endpoint URL]

Existing tables, queue and functions are reused (functions get their code/config updated in place), and the
SQS -> consume-event event source mapping is created only if it does not exist yet (and is re-enabled if disabled).
Settings come from the project-root .env automatically; --lambda-endpoint is the URL the *Lambda containers* use to
reach LocalStack (it differs from your host URL).
"""
import argparse
import io
import os
import sys
import time
import zipfile

import boto3
from botocore.exceptions import ClientError

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "src"))
from serverless_chaos.env import setup_environment  # noqa: E402

ROLE = "arn:aws:iam::000000000000:role/lambda-role"
FUNCTIONS = (("process-item", "handlers.process_item"), ("consume-event", "handlers.consume_event"))
TABLES = ("Items", "Processed", "chaos-control")
QUEUE = "events-queue"


def build_zip() -> bytes:
    buf = io.BytesIO()
    pkg = os.path.join(ROOT, "src", "serverless_chaos")
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(os.path.join(os.path.dirname(__file__), "handlers.py"), "handlers.py")
        z.write(os.path.join(pkg, "__init__.py"), "serverless_chaos/__init__.py")
        for sub in ("runtime", "adapters"):           # only the dependency-light packages ship to Lambda
            for fn in sorted(os.listdir(os.path.join(pkg, sub))):
                if fn.endswith(".py"):
                    z.write(os.path.join(pkg, sub, fn), f"serverless_chaos/{sub}/{fn}")
    return buf.getvalue()


def _code(e: ClientError) -> str:
    return e.response.get("Error", {}).get("Code", "")


def ensure_table(ddb, name: str) -> str:
    try:
        ddb.create_table(TableName=name, AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
                         KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}], BillingMode="PAY_PER_REQUEST")
        return "created"
    except ClientError as e:
        if _code(e) != "ResourceInUseException":
            raise
        return "reused"


def ensure_queue(sqs, name: str):
    url = sqs.create_queue(QueueName=name)["QueueUrl"]          # idempotent when attributes are unchanged
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    return url, arn


def wait_function_ready(lam, name: str, timeout_s: float = 90.0) -> None:
    end = time.time() + timeout_s
    while True:
        cfg = lam.get_function_configuration(FunctionName=name)
        state, upd = cfg.get("State", "Active"), cfg.get("LastUpdateStatus", "Successful")
        if state == "Failed" or upd == "Failed":
            raise RuntimeError(f"function {name} failed to become ready: {cfg.get('StateReason') or cfg.get('LastUpdateStatusReason')}")
        if state == "Active" and upd == "Successful":
            return
        if time.time() > end:
            raise TimeoutError(f"function {name} not ready after {timeout_s}s (State={state}, LastUpdateStatus={upd})")
        time.sleep(0.5)


def ensure_function(lam, name: str, handler: str, code: bytes, env: dict) -> str:
    try:
        lam.get_function_configuration(FunctionName=name)
    except ClientError as e:
        if _code(e) != "ResourceNotFoundException":
            raise
        lam.create_function(FunctionName=name, Runtime="python3.12", Role=ROLE, Handler=handler, Code={"ZipFile": code},
                            Timeout=10, MemorySize=128, Environment=env)
        wait_function_ready(lam, name)
        return "created"
    lam.update_function_code(FunctionName=name, ZipFile=code)       # keeps the function (and its event source mappings)
    wait_function_ready(lam, name)
    lam.update_function_configuration(FunctionName=name, Handler=handler, Timeout=10, MemorySize=128, Environment=env)
    wait_function_ready(lam, name)
    return "updated"


def _mappings(lam, function: str, queue_arn: str):
    return lam.list_event_source_mappings(FunctionName=function, EventSourceArn=queue_arn)["EventSourceMappings"]


def ensure_event_source_mapping(lam, function: str, queue_arn: str, batch_size: int = 1, timeout_s: float = 60.0):
    """Create the SQS trigger once; reuse (and re-enable) an existing one. Returns (uuid, 'created'|'reused')."""
    existing = _mappings(lam, function, queue_arn)
    status = "reused"
    if not existing:
        try:
            lam.create_event_source_mapping(EventSourceArn=queue_arn, FunctionName=function, BatchSize=batch_size)
            status = "created"
        except ClientError as e:
            if _code(e) != "ResourceConflictException":     # lost a race / stale listing: the mapping exists, reuse it
                raise
        existing = _mappings(lam, function, queue_arn)
    m = existing[0]
    if m.get("State") == "Disabled":
        lam.update_event_source_mapping(UUID=m["UUID"], Enabled=True)
    end = time.time() + timeout_s
    while True:
        state = lam.get_event_source_mapping(UUID=m["UUID"]).get("State")
        if state == "Enabled":
            return m["UUID"], status
        if time.time() > end:
            raise TimeoutError(f"event source mapping {m['UUID']} is '{state}' after {timeout_s}s (expected Enabled)")
        time.sleep(0.5)


def deploy_all(ddb, sqs, lam, lambda_endpoint: str, log=print) -> dict:
    summary = {}
    for t in TABLES:
        summary[f"table {t}"] = ensure_table(ddb, t)
    _, qarn = ensure_queue(sqs, QUEUE)
    summary[f"queue {QUEUE}"] = "ok"
    env = {"Variables": {"SC_ENDPOINT_URL": lambda_endpoint,
                         "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "test"),
                         "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
                         "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION", "us-east-1")}}
    code = build_zip()
    for name, handler in FUNCTIONS:
        summary[f"function {name}"] = ensure_function(lam, name, handler, code, env)
    uuid_, status = ensure_event_source_mapping(lam, "consume-event", qarn)
    summary[f"event source mapping {QUEUE} -> consume-event"] = f"{status} ({uuid_})"
    for k, v in summary.items():
        log(f"  {k}: {v}")
    return summary


def main():
    setup_environment()
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", default=os.environ["SC_ENDPOINT_URL"])
    ap.add_argument("--lambda-endpoint", default=os.getenv("SC_LAMBDA_ENDPOINT_URL", "http://localhost.localstack.cloud:4566"))
    a = ap.parse_args()
    kw = dict(endpoint_url=a.endpoint, region_name=os.environ["AWS_DEFAULT_REGION"])
    print(f"deploying to {a.endpoint} (Lambdas will use {a.lambda_endpoint})")
    deploy_all(boto3.client("dynamodb", **kw), boto3.client("sqs", **kw), boto3.client("lambda", **kw), a.lambda_endpoint)
    print("done")


if __name__ == "__main__":
    main()
