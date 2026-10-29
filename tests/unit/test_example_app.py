import importlib.util
import io
import json
import os
import sys
import zipfile

import boto3
import pytest
from botocore.exceptions import ClientError

EX = os.path.join(os.path.dirname(__file__), "..", "..", "examples", "items_app")


def _load(name):
    spec = importlib.util.spec_from_file_location(name, os.path.join(EX, f"{name}.py"))
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    return m


def test_deploy_zip_contains_only_lambda_safe_packages():
    names = zipfile.ZipFile(io.BytesIO(_load("deploy").build_zip())).namelist()
    assert "handlers.py" in names and "serverless_chaos/runtime/engine.py" in names and "serverless_chaos/adapters/base.py" in names
    assert not any(n.startswith(("serverless_chaos/generator", "serverless_chaos/llm", "serverless_chaos/faults")) for n in names)


def test_handlers_work_and_obey_injected_faults(aws, monkeypatch):
    monkeypatch.setattr("serverless_chaos.runtime.lambda_hook._default", None)
    boto3.client("dynamodb", region_name="us-east-1").create_table(
        TableName="Items", AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}], BillingMode="PAY_PER_REQUEST")
    boto3.client("dynamodb", region_name="us-east-1").create_table(
        TableName="Processed", AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}], BillingMode="PAY_PER_REQUEST")
    boto3.client("sqs", region_name="us-east-1").create_queue(QueueName="events-queue")
    h = _load("handlers")
    out = h.process_item({"id": "a1"}, None)
    assert out["statusCode"] == 200
    msg = h.ad.queue.receive("events-queue")[0]
    h.consume_event({"Records": [{"body": msg.body}]}, None)
    assert h.ad.storage.get("Processed", {"id": "a1"}) == {"id": "a1", "status": "done"}
    # event is lost in transit -> nothing reaches the consumer
    h.ad.control.activate({"type": "QueueFault", "queue": "events-queue", "effect": "drop"}, 30)
    h.process_item({"id": "a2"}, None)
    assert h.ad.queue.receive("events-queue") == []
    # handler-side fault from the same control plane
    h.ad.control.clear()
    h.ad.control.activate({"type": "LambdaError", "function": "process-item", "error": "exception"}, 30)
    from serverless_chaos.runtime import InjectedFaultError
    with pytest.raises(InjectedFaultError):
        h.process_item({"id": "a3"}, None)
