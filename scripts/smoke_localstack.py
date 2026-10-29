"""Staged smoke test against a RUNNING real LocalStack (never moto) with the sample app deployed.
Human-friendly twin of `pytest -m integration`: same checks, but each failure prints a hint about the likely layer.

    docker compose up -d && python examples/items_app/deploy.py
    python scripts/smoke_localstack.py

Each stage checks one thing moto cannot (Lambda really running, SQS triggering Lambda, Lambda reaching LocalStack,
faults crossing from your machine into the Lambdas). On failure it prints a hint pointing at the likely layer.
"""
import os
import sys
import time
import urllib.request
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from serverless_chaos.adapters import build_adapters  # noqa: E402
from serverless_chaos.env import setup_environment  # noqa: E402

setup_environment()   # project .env (any cwd) + LocalStack defaults; explicit env vars win

results = []


def stage(name, hint):
    def deco(fn):
        t0 = time.time()
        try:
            detail = fn() or ""
            results.append((name, True)); print(f"PASS  {name}  {detail}  ({time.time() - t0:.1f}s)")
        except Exception as e:
            results.append((name, False)); print(f"FAIL  {name}: {type(e).__name__}: {e}\n      hint: {hint}")
        return fn
    return deco


def wait_for(cond, timeout, every=0.5):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(every)
    return False


def settle():
    time.sleep(1.2)  # Lambdas cache the fault list for 0.5 s


@stage("1. LocalStack is up", "docker compose up -d; docker compose logs localstack (token errors show here)")
def _():
    body = urllib.request.urlopen(os.environ["SC_ENDPOINT_URL"] + "/_localstack/health", timeout=5).read().decode()
    assert "lambda" in body and "dynamodb" in body and "sqs" in body, body[:200]

ad = build_adapters()


@stage("2. Sample app is deployed", "run: python examples/items_app/deploy.py (check it finished without errors)")
def _():
    names = {f["FunctionName"] for f in ad.compute.client.list_functions()["Functions"]}
    assert {"process-item", "consume-event"} <= names, names
    ad.storage.client.describe_table(TableName="chaos-control")


def invoke(payload):
    return ad.compute.invoke("process-item", payload)


@stage("3. Lambda runs and reaches LocalStack from inside its container",
       "function_error below shows the cause. Connection refused/timeout => change --lambda-endpoint in deploy.py "
       "(try http://host.docker.internal:4566 or http://localhost.localstack.cloud:4566) and redeploy")
def _():
    i = uuid.uuid4().hex
    r = invoke({"id": i})
    assert r.ok, f"function_error={r.function_error} payload={str(r.payload)[:300]}"
    assert ad.storage.get("Items", {"id": i}), "Lambda returned 200 but item is not in Items"
    return f"({r.latency_ms:.0f} ms, includes cold start)"


@stage("4. SQS triggers consume-event (event flow end to end)",
       "event-source mapping missing or not enabled: check `awslocal lambda list-event-source-mappings`; "
       "check consume-event logs: `awslocal logs tail /aws/lambda/consume-event`")
def _():
    i = uuid.uuid4().hex
    assert invoke({"id": i}).ok
    assert wait_for(lambda: ad.storage.get("Processed", {"id": i}), 30), "event never reached Processed within 30 s"


@stage("5. QueueFault(drop) crosses into the Lambda and loses the event",
       "faults not reaching Lambdas: the Lambda cannot read chaos-control (same networking issue as stage 3) "
       "or the app does not use the adapters")
def _():
    ad.control.activate({"type": "QueueFault", "queue": "events-queue", "effect": "drop"}, ttl_s=60); settle()
    i = uuid.uuid4().hex
    try:
        assert invoke({"id": i}).ok
        assert not wait_for(lambda: ad.storage.get("Processed", {"id": i}), 8), "event arrived although it should be dropped"
    finally:
        ad.control.clear(); settle()
    j = uuid.uuid4().hex
    assert invoke({"id": j}).ok
    assert wait_for(lambda: ad.storage.get("Processed", {"id": j}), 30), "system did not recover after rollback"
    return "(dropped while active, recovered after clear)"


@stage("6. LambdaError(exception) fails the invocation",
       "handler-side fault not applied: check that handlers.py uses @chaos_lambda with the right function name")
def _():
    ad.control.activate({"type": "LambdaError", "function": "process-item", "error": "exception"}, ttl_s=60); settle()
    try:
        r = invoke({"id": uuid.uuid4().hex})
        assert not r.ok and r.function_error, f"expected a function error, got ok={r.ok}"
    finally:
        ad.control.clear(); settle()
    assert invoke({"id": uuid.uuid4().hex}).ok, "did not recover"


@stage("7. DynamoDBFault(throttle) breaks the write path",
       "storage faults not applied inside the Lambda")
def _():
    ad.control.activate({"type": "DynamoDBFault", "table": "Items", "effect": "throttle"}, ttl_s=60); settle()
    try:
        r = invoke({"id": uuid.uuid4().hex})
        assert not r.ok, "write succeeded although Items is throttled"
    finally:
        ad.control.clear(); settle()


@stage("8. LambdaLatency adds the configured delay",
       "latency not applied (see stage 6 hint)")
def _():
    ad.control.activate({"type": "LambdaLatency", "function": "process-item", "delay_ms": 2000}, ttl_s=60); settle()
    try:
        r = invoke({"id": uuid.uuid4().hex})
        assert r.latency_ms >= 2000, f"latency was only {r.latency_ms:.0f} ms"
    finally:
        ad.control.clear(); settle()


ok = sum(1 for _, p in results if p)
print(f"\n{ok}/{len(results)} stages passed")
sys.exit(0 if ok == len(results) else 1)
