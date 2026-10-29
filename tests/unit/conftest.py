"""UNIT tests: fast, offline, run against moto (an in-process AWS mock). They never touch LocalStack.
Integration tests against real LocalStack live in tests/integration and are selected with `pytest -m integration`."""
import boto3
import pytest
from moto import mock_aws


@pytest.fixture(autouse=True)
def isolated_env(monkeypatch):
    for k, v in {"AWS_ACCESS_KEY_ID": "t", "AWS_SECRET_ACCESS_KEY": "t", "AWS_DEFAULT_REGION": "us-east-1"}.items():
        monkeypatch.setenv(k, v)
    for k in ("SC_ENDPOINT_URL", "AWS_ENDPOINT_URL", "LOCALSTACK_HOSTNAME", "SC_CHAOS_DISABLED", "SC_LAMBDA_ENDPOINT_URL"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("SC_ENV_FILE", "none")     # a developer's real .env must never leak into unit tests
    monkeypatch.setenv("SC_REAL_AWS", "1")        # ...and neither must the LocalStack endpoint defaults


@pytest.fixture
def aws():
    """Offline AWS (moto) with tables T + chaos-control and queue q."""
    with mock_aws():
        ddb = boto3.client("dynamodb", region_name="us-east-1")
        for t in ("T", "chaos-control"):
            ddb.create_table(TableName=t, AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
                             KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}], BillingMode="PAY_PER_REQUEST")
        boto3.client("sqs", region_name="us-east-1").create_queue(QueueName="q")
        yield
