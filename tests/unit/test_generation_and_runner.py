import io
import json
import zipfile

import boto3
import pytest

from serverless_chaos.adapters import build_adapters
from serverless_chaos.discover import apply_annotations, discover
from serverless_chaos.experiment.runner import ExperimentRunner
from serverless_chaos.experiment.steady_state import SteadyState
from serverless_chaos.experiment.workload import Sample, run_for, summarize
from serverless_chaos.faults import parse_fault
from serverless_chaos.generator import (CoverageGuidedGenerator, GenerationError, LLMGenerator, RandomGenerator,
                                        candidate_faults, signature_universe)
from serverless_chaos.history import History
from serverless_chaos.llm import FakeLLM
from serverless_chaos.runtime import FaultRuntime, InMemoryControlPlane
from serverless_chaos.scenario import Scenario
from serverless_chaos.topology import Edge, FunctionNode, QueueNode, TableNode, Topology

TOPO = Topology(name="t", functions=[FunctionNode(name="f")], tables=[TableNode(name="T")], queues=[QueueNode(name="q")],
                edges=[Edge(src="q", dst="f", kind="triggers")])
SS = SteadyState(success_rate_min=0.9, p95_ms_max=5000)


def j(**kw): return json.dumps(kw)
GOOD = j(event="spike", rationale="r", faults=[{"type": "DynamoDBFault", "table": "T", "effect": "throttle"}])
GOOD2 = j(event="dup", rationale="r", faults=[{"type": "QueueFault", "queue": "q", "effect": "duplicate"}])
HALLU = j(event="x", faults=[{"type": "DynamoDBFault", "table": "Ghost", "effect": "throttle"}])


# ---------------- faults ----------------
def test_fault_validation_and_caps():
    with pytest.raises(Exception): parse_fault({"type": "QueueFault", "queue": "q", "effect": "delay"})
    with pytest.raises(Exception): parse_fault({"type": "LambdaLatency", "function": "f", "delay_ms": 10**7})
    with pytest.raises(Exception): parse_fault({"type": "ServiceOutage", "service": "sqs", "duration_s": 10_000})
    f = parse_fault({"type": "DynamoDBFault", "table": "T", "effect": "throttle"})
    assert f.signature == ("DynamoDBFault", "T", "throttle") and f.to_runtime()["table"] == "T"


# ---------------- LLM generator ----------------
def test_llm_generator_retries_on_hallucinated_resource():
    llm = FakeLLM([HALLU, GOOD])
    g = LLMGenerator(llm)
    sc = g.generate(TOPO, SS, History(None, signature_universe(TOPO)))
    assert sc.faults[0].table == "T" and g.last_attempts == 2
    assert "does not exist" in llm.calls[1][-1]["content"]        # the error was fed back


def test_llm_generator_retries_on_bad_json_and_gives_up():
    g = LLMGenerator(FakeLLM(["not json", "still {not json"]), max_attempts=2)
    with pytest.raises(GenerationError):
        g.generate(TOPO, SS, History(None, signature_universe(TOPO)))


def test_closed_loop_rejects_repeats_and_shows_history():
    h = History(None, signature_universe(TOPO))
    h.add(Scenario.model_validate_json(GOOD), "x", {"impact": "violated", "during": {"n": 5, "success_rate": 0.2, "p95_ms": 900}})
    llm = FakeLLM([GOOD, GOOD2])                                     # first answer repeats an already-tried fault
    sc = LLMGenerator(llm).generate(TOPO, SS, h)
    assert sc.faults[0].type == "QueueFault"
    first_prompt = llm.calls[0][1]["content"]
    assert "VIOLATED" in first_prompt and "NOT YET TRIED" in first_prompt


def test_open_loop_hides_history_and_allows_repeats():
    h = History(None, signature_universe(TOPO))
    h.add(Scenario.model_validate_json(GOOD), "x")
    llm = FakeLLM([GOOD])
    sc = LLMGenerator(llm, closed_loop=False).generate(TOPO, SS, h)
    assert sc.faults[0].table == "T"
    assert "history" not in llm.calls[0][1]["content"].lower()


# ---------------- baselines ----------------
def test_coverage_guided_reaches_full_coverage_random_does_not():
    n = len(signature_universe(TOPO))
    h = History(None, signature_universe(TOPO)); g = CoverageGuidedGenerator(seed=1)
    for _ in range(n): h.add(g.generate(TOPO, SS, h), g.name, {"impact": "no_impact"})
    assert h.coverage() == 1.0
    hr = History(None, signature_universe(TOPO)); r = RandomGenerator(seed=1)
    for _ in range(n): hr.add(r.generate(TOPO, SS, hr), r.name, {"impact": "no_impact"})
    assert hr.coverage() < 1.0                                       # random repeats itself (seed-stable)


def test_history_persists_and_summarizes(tmp_path):
    p = str(tmp_path / "h.jsonl")
    h = History(p, signature_universe(TOPO))
    rec = h.add(Scenario.model_validate_json(GOOD), "llm-closed")
    h.set_outcome(rec.id, {"impact": "violated"})
    h2 = History(p, signature_universe(TOPO))
    assert len(h2.records) == 1 and len(h2.violating()) == 1 and 0 < h2.coverage() < 1


# ---------------- workload + runner ----------------
class DbWorkload:
    """Writes through the real adapters; fails when the injected fault makes the write fail."""
    def __init__(self, ad): self.ad, self.i = ad, 0
    def run_once(self):
        import time
        t0 = time.perf_counter()
        try:
            self.i += 1
            self.ad.storage.put("T", {"id": str(self.i)})
            return Sample(True, (time.perf_counter() - t0) * 1000)
        except Exception as e:
            return Sample(False, (time.perf_counter() - t0) * 1000, e.response["Error"]["Code"])


def test_summarize_percentiles():
    m = summarize([Sample(True, 10), Sample(True, 20), Sample(False, 30, "E")])
    assert m["n"] == 3 and abs(m["success_rate"] - 2 / 3) < 1e-9 and m["errors"] == {"E": 1}


def test_runner_detects_violation_then_recovery_end_to_end(aws):
    ad = build_adapters()
    runner = ExperimentRunner(ad.control, DbWorkload(ad), SS, baseline_s=0.4, recovery_s=0.4, concurrency=2, fault_window_s=0.6, propagation_s=0)
    out = runner.run(Scenario.model_validate_json(GOOD))
    assert out.baseline_ok and out.impact == "violated" and out.recovered
    assert out.during["errors"].get("ProvisionedThroughputExceededException", 0) > 0
    assert ad.control.active() == []                                 # faults rolled back


def test_runner_reports_no_impact_for_unrelated_fault(aws):
    ad = build_adapters()
    runner = ExperimentRunner(ad.control, DbWorkload(ad), SS, baseline_s=0.3, recovery_s=0.3, concurrency=2, fault_window_s=0.4, propagation_s=0)
    out = runner.run(Scenario.model_validate_json(GOOD2))            # queue fault; workload only uses the table
    assert out.impact == "no_impact"


def test_runner_aborts_on_unhealthy_baseline():
    class Down:
        def run_once(self): return Sample(False, 1.0, "down")
    runner = ExperimentRunner(InMemoryControlPlane(), Down(), SS, baseline_s=0.2, recovery_s=0.2, concurrency=1,
                              propagation_s=0, heal_timeout_s=0)
    out = runner.run(Scenario.model_validate_json(GOOD))
    assert out.impact == "aborted" and not out.baseline_ok


def test_runner_rolls_back_even_if_activation_fails():
    class Flaky(InMemoryControlPlane):
        def activate(self, fault, ttl_s):
            if len(self.active()) >= 1:
                raise RuntimeError("boom")
            return super().activate(fault, ttl_s)
    ctl = Flaky()
    sc = Scenario.model_validate_json(j(event="e", faults=[{"type": "ServiceOutage", "service": "sqs"}, {"type": "ServiceOutage", "service": "lambda"}]))
    ok = type("W", (), {"run_once": lambda self: Sample(True, 1.0)})()
    with pytest.raises(RuntimeError):
        ExperimentRunner(ctl, ok, SS, baseline_s=0.1, recovery_s=0.1, concurrency=1, fault_window_s=0.1, propagation_s=0).run(sc)
    assert ctl.active() == []


# ---------------- discovery ----------------
def test_discovery_finds_resources_and_trigger_edges(aws):
    sqs, lam, iam = (boto3.client(s, region_name="us-east-1") for s in ("sqs", "lambda", "iam"))
    role = iam.create_role(RoleName="r", AssumeRolePolicyDocument="{}")["Role"]["Arn"]
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z: z.writestr("h.py", "def h(e,c): return 1")
    lam.create_function(FunctionName="consumer", Runtime="python3.12", Role=role, Handler="h.h", Code={"ZipFile": buf.getvalue()})
    url = sqs.get_queue_url(QueueName="q")["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    lam.create_event_source_mapping(EventSourceArn=arn, FunctionName="consumer")
    topo = discover(lam, boto3.client("dynamodb", region_name="us-east-1"), sqs, "demo")
    assert topo.names("function") == ["consumer"] and topo.names("table") == ["T"]       # chaos-control excluded
    assert Edge(src="q", dst="consumer", kind="triggers") in topo.edges
    topo = apply_annotations(topo, {"edges": [{"src": "consumer", "dst": "T", "kind": "writes"}], "weaknesses": ["no DLQ"]})
    assert "consumer --writes--> T" in topo.describe() and "no DLQ" in topo.describe()
