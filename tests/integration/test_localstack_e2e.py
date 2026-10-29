import uuid

import boto3
import pytest

from serverless_chaos.config import build_all
from serverless_chaos.experiment.runner import ExperimentRunner
from serverless_chaos.experiment.workload import run_for, summarize
from serverless_chaos.scenario import Scenario

from .conftest import ROOT, settle, wait_for

CFG = str(ROOT / "examples" / "items_app" / "experiment.json")


def invoke(ad, **payload):
    return ad.compute.invoke("process-item", {"id": uuid.uuid4().hex, **payload})


# ---------------- infrastructure ----------------
def test_01_localstack_has_required_services(localstack):
    for svc in ("lambda", "dynamodb", "sqs"):
        assert localstack["services"].get(svc) in ("available", "running"), localstack["services"]


def test_02_deploy_is_idempotent(deployed, deploy_module, localstack):
    import os
    kw = dict(endpoint_url=localstack["url"], region_name=os.environ["AWS_DEFAULT_REGION"])
    ddb, sqs, lam = (boto3.client(s, **kw) for s in ("dynamodb", "sqs", "lambda"))
    again = deploy_module.deploy_all(ddb, sqs, lam, os.getenv("SC_LAMBDA_ENDPOINT_URL", "http://localhost.localstack.cloud:4566"), log=lambda *_: None)
    assert again["table Items"] == "reused" and again["function process-item"] == "updated"
    assert again["event source mapping events-queue -> consume-event"].startswith("reused")
    arn = sqs.get_queue_attributes(QueueUrl=sqs.get_queue_url(QueueName="events-queue")["QueueUrl"], AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    assert len(lam.list_event_source_mappings(FunctionName="consume-event", EventSourceArn=arn)["EventSourceMappings"]) == 1


# ---------------- the stages moto cannot cover ----------------
def test_03_lambda_runs_reaches_localstack_and_persists(ad, probe):
    r = invoke(ad, id="it-persist")
    assert r.ok, f"function_error={r.function_error} payload={str(r.payload)[:300]} -> Lambda cannot reach LocalStack? check --lambda-endpoint"
    assert probe.storage.get("Items", {"id": "it-persist"}) is not None


def test_04_sqs_triggers_consumer_end_to_end(ad, probe):
    eid = uuid.uuid4().hex
    assert ad.compute.invoke("process-item", {"id": eid}).ok
    assert wait_for(lambda: probe.storage.get("Processed", {"id": eid}), 30), "event never reached Processed"


# ---------------- fault injection + rollback + recovery ----------------
def test_05_queue_drop_loses_event_then_system_recovers(ad, probe):
    ad.control.activate({"type": "QueueFault", "queue": "events-queue", "effect": "drop"}, 60); settle()
    eid = uuid.uuid4().hex
    assert ad.compute.invoke("process-item", {"id": eid}).ok
    assert not wait_for(lambda: probe.storage.get("Processed", {"id": eid}), 8), "dropped event was delivered"
    ad.control.clear(); settle()
    eid2 = uuid.uuid4().hex
    assert ad.compute.invoke("process-item", {"id": eid2}).ok
    assert wait_for(lambda: probe.storage.get("Processed", {"id": eid2}), 30), "no recovery after rollback"


def test_06_lambda_exception_fault_and_rollback(ad):
    ad.control.activate({"type": "LambdaError", "function": "process-item", "error": "exception"}, 60); settle()
    r = invoke(ad)
    assert not r.ok and r.function_error
    ad.control.clear(); settle()
    assert invoke(ad).ok


def test_07_dynamodb_throttle_breaks_write_path_and_rolls_back(ad):
    ad.control.activate({"type": "DynamoDBFault", "table": "Items", "effect": "throttle"}, 60); settle()
    assert not invoke(ad).ok
    ad.control.clear(); settle()
    assert invoke(ad).ok


def test_08_lambda_latency_adds_configured_delay(ad):
    ad.control.activate({"type": "LambdaLatency", "function": "process-item", "delay_ms": 2000}, 60); settle()
    assert invoke(ad).latency_ms >= 2000


# ---------------- the baseline (the reported failure) ----------------
def test_09_healthy_baseline_meets_the_configured_steady_state(ad):
    """No faults, clean queue: the system must satisfy the configured steady state. If this fails, read the printed metrics:
    success ~100% but p95 too high => threshold unrealistic (sls-chaos calibrate); low success => sls-chaos diagnose."""
    topo, ss, runner = build_all(CFG, ad)
    m = summarize(run_for(runner.workload, 20, runner.concurrency, runner.pace_s))
    ok, why = ss.check(m)
    assert ok, f"unhealthy baseline: {why}\nmetrics={m}"


def test_10_full_experiment_detects_violation_and_recovers(ad):
    topo, ss, runner = build_all(CFG, ad)
    runner.baseline_s, runner.recovery_s, runner.fault_window_s = 15, 15, 12
    sc = Scenario.model_validate({"event": "DynamoDB throttling on the write path",
                                  "faults": [{"type": "DynamoDBFault", "table": "Items", "effect": "throttle", "duration_s": 12}]})
    out = runner.run(sc)
    assert out.baseline_ok, f"baseline unhealthy, experiment is invalid: {out.reasons}"
    assert out.impact == "violated" and out.during["errors"], out
    assert out.recovered, f"system did not recover after rollback: {out.recovery}"
    assert ad.control.active() == []
