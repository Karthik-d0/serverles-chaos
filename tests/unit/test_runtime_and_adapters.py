import subprocess
import sys
import time

import pytest
from botocore.exceptions import ClientError

from serverless_chaos.adapters import build_adapters
from serverless_chaos.runtime import (DynamoControlPlane, FaultRuntime, InMemoryControlPlane, InjectedFaultError,
                                      NullRuntime, chaos_lambda)


def rt(*faults, ttl=60):
    c = InMemoryControlPlane()
    for f in faults:
        c.activate(f, ttl)
    return FaultRuntime(c), c


# ---------------- pure decision logic ----------------
def test_no_faults_no_effect():
    assert not FaultRuntime(InMemoryControlPlane()).decide("storage", "T", "write").active


def test_dynamodb_effects_and_operation_filter():
    r, _ = rt({"type": "DynamoDBFault", "table": "T", "operations": "write", "effect": "latency", "delay_ms": 500})
    assert r.decide("storage", "T", "write").delay_s == 0.5
    assert r.decide("storage", "T", "read").delay_s == 0          # reads unaffected
    assert r.decide("storage", "Other", "write").delay_s == 0     # other tables unaffected
    r, _ = rt({"type": "DynamoDBFault", "table": "T", "effect": "throttle"})
    assert r.decide("storage", "T", "read").error[0] == "ProvisionedThroughputExceededException"


def test_queue_effects():
    r, _ = rt({"type": "QueueFault", "queue": "q", "effect": "drop"})
    assert r.decide("queue", "q", "send").drop and not r.decide("queue", "q", "receive").active
    r, _ = rt({"type": "QueueFault", "queue": "q", "effect": "delay", "delay_ms": 2500})
    assert r.decide("queue", "q", "send").queue_delay_s == 3
    r, _ = rt({"type": "QueueFault", "queue": "q", "effect": "unavailable"})
    assert r.decide("queue", "q", "receive").error


def test_lambda_split_between_invoke_and_handler():
    r, _ = rt({"type": "LambdaError", "function": "f", "error": "throttle"})
    assert r.decide("compute", "f", "invoke").error[0] == "TooManyRequestsException"
    assert not r.decide("compute", "f", "handler").error
    r, _ = rt({"type": "LambdaError", "function": "f", "error": "exception"},
              {"type": "LambdaLatency", "function": "f", "delay_ms": 100})
    d = r.decide("compute", "f", "handler")
    assert d.error and abs(d.delay_s - 0.1) < 1e-6 and not r.decide("compute", "f", "invoke").active


def test_service_outage_hits_every_resource_of_the_service():
    r, _ = rt({"type": "ServiceOutage", "service": "dynamodb"})
    assert r.decide("storage", "A", "read").error and r.decide("storage", "B", "write").error
    assert not r.decide("queue", "q", "send").active


def test_probability_zero_and_expiry():
    r, _ = rt({"type": "ServiceOutage", "service": "sqs", "probability": 0.0})
    assert not r.decide("queue", "q", "send").active
    now = [1000.0]
    c = InMemoryControlPlane(clock=lambda: now[0])
    c.activate({"type": "ServiceOutage", "service": "sqs"}, ttl_s=10)
    r = FaultRuntime(c)
    assert r.decide("queue", "q", "send").error
    now[0] += 11
    assert not r.decide("queue", "q", "send").active


def test_lambda_decorator():
    r, _ = rt({"type": "LambdaError", "function": "f", "error": "exception"})

    @chaos_lambda("f", runtime=r)
    def handler(e, c): return "ok"

    with pytest.raises(InjectedFaultError):
        handler({}, None)
    assert chaos_lambda("g", runtime=r)(lambda e, c: "ok")({}, None) == "ok"
    assert chaos_lambda("f", runtime=NullRuntime())(lambda e, c: "ok")({}, None) == "ok"


# ---------------- adapters against (offline) AWS ----------------
def test_adapters_roundtrip_without_faults(aws):
    ad = build_adapters(control_table=None)
    ad.storage.put("T", {"id": "1", "n": 3, "x": 1.5})
    assert ad.storage.get("T", {"id": "1"}) == {"id": "1", "n": 3, "x": 1.5}
    assert ad.storage.get("T", {"id": "missing"}) is None
    ad.queue.send("q", {"a": 1})
    msgs = ad.queue.receive("q")
    assert msgs and '"a": 1' in msgs[0].body
    ad.queue.delete("q", msgs[0].receipt)


def test_storage_faults_raise_realistic_errors(aws):
    ad = build_adapters()
    ad.control.activate({"type": "DynamoDBFault", "table": "T", "effect": "throttle"}, 30)
    with pytest.raises(ClientError) as e:
        ad.storage.put("T", {"id": "1"})
    assert e.value.response["Error"]["Code"] == "ProvisionedThroughputExceededException"
    ad.control.clear()
    ad.storage.put("T", {"id": "1"})          # recovers immediately after rollback


def test_storage_latency_is_applied(aws):
    ad = build_adapters()
    ad.control.activate({"type": "DynamoDBFault", "table": "T", "effect": "latency", "delay_ms": 300}, 30)
    t0 = time.perf_counter()
    ad.storage.put("T", {"id": "1"})
    assert time.perf_counter() - t0 >= 0.3


def test_queue_drop_duplicate_delay(aws):
    ad = build_adapters()
    ad.control.activate({"type": "QueueFault", "queue": "q", "effect": "drop"}, 30)
    assert ad.queue.send("q", "x") == "dropped" and ad.queue.depth("q") == 0
    ad.control.clear()
    ad.control.activate({"type": "QueueFault", "queue": "q", "effect": "duplicate"}, 30)
    ad.queue.send("q", "x")
    assert ad.queue.depth("q") == 2
    ad.control.clear(); ad.queue.purge("q")
    ad.control.activate({"type": "QueueFault", "queue": "q", "effect": "delay", "delay_ms": 5000}, 30)
    ad.queue.send("q", "x")
    assert ad.queue.depth("q") == 0            # message is invisible for 5 s


def test_dynamo_control_plane_expiry_and_fail_open(aws):
    now = [1000.0]
    from serverless_chaos.runtime.control import DynamoControlPlane
    c = DynamoControlPlane("chaos-control", clock=lambda: now[0], cache_ttl_s=0)
    c.activate({"type": "ServiceOutage", "service": "sqs"}, ttl_s=10)
    assert len(c.active()) == 1
    now[0] += 11
    assert c.active() == []
    broken = DynamoControlPlane("does-not-exist", cache_ttl_s=0)
    assert broken.active() == []               # fails open


def test_runtime_and_adapters_need_only_boto3():
    """These packages are zipped into Lambdas, where pydantic/httpx are not installed."""
    code = ("import sys; sys.modules['pydantic']=None; sys.modules['httpx']=None; "
            "import serverless_chaos.runtime, serverless_chaos.adapters; print('ok')")
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert out.stdout.strip() == "ok", out.stderr
