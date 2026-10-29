"""INTEGRATION tests: a real, running LocalStack with the sample app deployed. No moto anywhere in this directory.

    docker compose up -d
    pytest -m integration -v

If LocalStack is not reachable these tests FAIL (they are never skipped or redirected to a mock), because a green run
must mean the real thing works.
"""
import importlib.util
import json
import os
import time
import urllib.request
from pathlib import Path

import boto3
import pytest

from serverless_chaos.adapters import build_adapters
from serverless_chaos.env import setup_environment

ROOT = Path(__file__).resolve().parents[2]
setup_environment()   # project .env + LocalStack defaults; explicit env wins


def pytest_collection_modifyitems(items):
    for item in items:
        if "tests/integration" in str(item.fspath).replace(os.sep, "/"):
            item.add_marker(pytest.mark.integration)


def settle(seconds: float = 1.3):
    time.sleep(seconds)  # Lambdas cache the fault list for 0.5 s


@pytest.fixture(scope="session")
def localstack():
    url = os.environ["SC_ENDPOINT_URL"]
    try:
        health = json.loads(urllib.request.urlopen(url + "/_localstack/health", timeout=5).read())
    except Exception as e:
        pytest.fail(f"LocalStack is not reachable at {url} ({type(e).__name__}: {e}). Start it with `docker compose up -d` "
                    "(needs LOCALSTACK_AUTH_TOKEN in .env). Integration tests never fall back to moto.", pytrace=False)
    return {"url": url, "services": health.get("services", {})}


@pytest.fixture(scope="session")
def deploy_module():
    spec = importlib.util.spec_from_file_location("items_deploy", ROOT / "examples" / "items_app" / "deploy.py")
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
    return m


@pytest.fixture(scope="session")
def deployed(localstack, deploy_module):
    kw = dict(endpoint_url=localstack["url"], region_name=os.environ["AWS_DEFAULT_REGION"])
    clients = [boto3.client(s, **kw) for s in ("dynamodb", "sqs", "lambda")]
    lambda_ep = os.getenv("SC_LAMBDA_ENDPOINT_URL", "http://localhost.localstack.cloud:4566")
    return deploy_module.deploy_all(*clients, lambda_ep, log=lambda *_: None)


@pytest.fixture(scope="session")
def ad(deployed):
    return build_adapters()


@pytest.fixture(scope="session")
def probe(deployed):
    return build_adapters(control_table=None)      # measuring instrument: never fault-injected


@pytest.fixture(autouse=True)
def clean_state(ad):
    """Every test starts and ends with no faults and an empty queue, so tests cannot contaminate each other."""
    ad.control.clear(); settle()
    ad.queue.drain("events-queue", timeout_s=60)
    yield
    ad.control.clear(); settle()


def wait_for(cond, timeout, every=0.3):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(every)
    return False
