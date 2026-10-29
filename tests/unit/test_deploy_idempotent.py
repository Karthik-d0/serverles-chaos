import importlib.util
import os

import boto3
import pytest

EX = os.path.join(os.path.dirname(__file__), "..", "..", "examples", "items_app", "deploy.py")


def _deploy_module():
    spec = importlib.util.spec_from_file_location("items_deploy", EX)
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    trust = ('{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},'
             '"Action":"sts:AssumeRole"}]}')
    # moto validates roles (LocalStack does not): create a real one in moto's account
    m.ROLE = boto3.client("iam", region_name="us-east-1").create_role(RoleName="lambda-role", AssumeRolePolicyDocument=trust)["Role"]["Arn"]
    return m


def clients():
    return (boto3.client(s, region_name="us-east-1") for s in ("dynamodb", "sqs", "lambda"))


def test_deploy_twice_reuses_everything_and_keeps_one_mapping(aws):
    d = _deploy_module()
    ddb, sqs, lam = clients()
    first = d.deploy_all(ddb, sqs, lam, "http://x:4566", log=lambda *_: None)
    second = d.deploy_all(ddb, sqs, lam, "http://x:4566", log=lambda *_: None)      # must not raise ResourceConflictException
    assert first["function process-item"] == "created" and second["function process-item"] == "updated"
    assert second["table Items"] == "reused"
    assert first["event source mapping events-queue -> consume-event"].startswith("created")
    assert second["event source mapping events-queue -> consume-event"].startswith("reused")
    arn = sqs.get_queue_attributes(QueueUrl=sqs.get_queue_url(QueueName="events-queue")["QueueUrl"], AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    assert len(lam.list_event_source_mappings(FunctionName="consume-event", EventSourceArn=arn)["EventSourceMappings"]) == 1


def test_mapping_conflict_race_is_tolerated(aws, monkeypatch):
    """LocalStack raises ResourceConflictException when the mapping already exists; we must reuse it, not fail."""
    from botocore.exceptions import ClientError
    d = _deploy_module()
    ddb, sqs, lam = clients()
    d.deploy_all(ddb, sqs, lam, "http://x:4566", log=lambda *_: None)
    _, arn = d.ensure_queue(sqs, "events-queue")
    real_list = d._mappings
    calls = {"list": 0, "create": 0}

    def stale_then_real(lam_, fn, qarn):          # first listing looks empty (stale), forcing a create attempt
        calls["list"] += 1
        return [] if calls["list"] == 1 else real_list(lam_, fn, qarn)

    class ConflictingLambda:                       # delegates to moto, but create behaves like LocalStack
        def __getattr__(self, n): return getattr(lam, n)
        def create_event_source_mapping(self, **kw):
            calls["create"] += 1
            raise ClientError({"Error": {"Code": "ResourceConflictException", "Message": "exists"}}, "CreateEventSourceMapping")

    monkeypatch.setattr(d, "_mappings", stale_then_real)
    uuid_, status = d.ensure_event_source_mapping(ConflictingLambda(), "consume-event", arn)
    assert calls["create"] == 1 and status == "reused" and uuid_


def test_disabled_mapping_is_re_enabled(aws):
    d = _deploy_module()
    ddb, sqs, lam = clients()
    d.deploy_all(ddb, sqs, lam, "http://x:4566", log=lambda *_: None)
    _, arn = d.ensure_queue(sqs, "events-queue")
    m = d._mappings(lam, "consume-event", arn)[0]
    lam.update_event_source_mapping(UUID=m["UUID"], Enabled=False)
    uuid_, status = d.ensure_event_source_mapping(lam, "consume-event", arn)
    assert status == "reused" and lam.get_event_source_mapping(UUID=uuid_)["State"] == "Enabled"
