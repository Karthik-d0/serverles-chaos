"""Regression tests for the 'unhealthy baseline' problems (state leakage between experiments, misleading metrics)."""
import json

import pytest
from botocore.exceptions import ClientError

from serverless_chaos.adapters import InvokeResult, build_adapters
from serverless_chaos.cli import main
from serverless_chaos.experiment.runner import ExperimentRunner
from serverless_chaos.experiment.steady_state import SteadyState
from serverless_chaos.experiment.workload import EventFlowWorkload, Sample, summarize
from serverless_chaos.history import History
from serverless_chaos.runtime import InMemoryControlPlane
from serverless_chaos.scenario import Scenario

SC = Scenario.model_validate_json(json.dumps({"event": "e", "faults": [{"type": "ServiceOutage", "service": "sqs"}]}))
SS = SteadyState(success_rate_min=0.9, p95_ms_max=1000, min_samples=3)
OK = type("W", (), {"run_once": lambda self: Sample(True, 5.0)})()


# ---------- queue drain ----------
def test_drain_empties_queue_including_delayed_messages(aws):
    ad = build_adapters(control_table=None)
    for i in range(5):
        ad.queue.send("q", f"m{i}")
    ad.queue.send("q", "late", delay_s=30)
    assert sum(ad.queue.stats("q").values()) > 0
    ad.queue.drain("q", timeout_s=5, poll_s=0.05)
    assert ad.queue.stats("q") == {"visible": 0, "in_flight": 0, "delayed": 0}


def test_drain_falls_back_when_purge_is_rate_limited(aws, monkeypatch):
    ad = build_adapters(control_table=None)
    for i in range(12):
        ad.queue.send("q", f"m{i}")

    def refuse(queue):
        raise ClientError({"Error": {"Code": "PurgeQueueInProgress", "Message": "once per 60s"}}, "PurgeQueue")

    monkeypatch.setattr(ad.queue, "_purge", refuse)
    ad.queue.drain("q", timeout_s=5, poll_s=0.05)
    assert sum(ad.queue.stats("q").values()) == 0


def test_drain_times_out_loudly_instead_of_continuing_dirty(aws, monkeypatch):
    ad = build_adapters(control_table=None)
    monkeypatch.setattr(ad.queue, "_purge", lambda q: None)
    monkeypatch.setattr(ad.queue, "_stats", lambda q: {"visible": 3, "in_flight": 0, "delayed": 0})
    with pytest.raises(TimeoutError, match="did not drain"):
        ad.queue.drain("q", timeout_s=0.3, poll_s=0.05)


def test_drain_is_not_fault_injected(aws):
    """A QueueFault must not be able to break the reset step itself."""
    ad = build_adapters()
    ad.queue.send("q", "x")
    ad.control.activate({"type": "ServiceOutage", "service": "sqs"}, 30)
    ad.queue.drain("q", timeout_s=5, poll_s=0.05)        # would raise if drain went through injection
    assert ad.queue.stats("q")["visible"] == 0


# ---------- runner ----------
def test_runner_resets_system_before_every_baseline():
    order = []

    class W:
        def run_once(self):
            order.append("work"); return Sample(True, 1.0)

    runner = ExperimentRunner(InMemoryControlPlane(), W(), SS, baseline_s=0.1, recovery_s=0.1, concurrency=1,
                              fault_window_s=0.1, propagation_s=0, prepare=lambda: order.append("prepare"))
    runner.run(SC)
    assert order[0] == "prepare" and "work" in order


def test_failed_reset_aborts_instead_of_measuring_a_dirty_system():
    def broken():
        raise TimeoutError("queue 'events-queue' did not drain")

    out = ExperimentRunner(InMemoryControlPlane(), OK, SS, baseline_s=0.1, recovery_s=0.1, concurrency=1,
                           propagation_s=0, prepare=broken).run(SC)
    assert out.impact == "aborted" and "could not reset" in out.reasons[0]


# ---------- probe isolation ----------
def test_sink_polling_is_immune_to_injected_faults(aws, monkeypatch):
    """With a DynamoDB fault on the sink table, the probe must keep reading it; only the real system may fail."""
    from types import SimpleNamespace
    monkeypatch.setattr("serverless_chaos.experiment.workload.uuid.uuid4", lambda: SimpleNamespace(hex="evt-1"))
    faulty = build_adapters()                                   # instrumented client (what the application sees)
    probe = build_adapters(control_table=None)                  # measuring instrument
    probe.storage.put("T", {"id": "evt-1"})                     # the pipeline "delivered" the event

    class Producer:
        def invoke(self, fn, payload): return InvokeResult(200, {"statusCode": 200}, None, 1.0)

    faulty.control.activate({"type": "DynamoDBFault", "table": "T", "effect": "unavailable"}, 30)
    good = EventFlowWorkload(Producer(), probe.storage, "p", "T", deadline_s=1).run_once()
    bad = EventFlowWorkload(Producer(), faulty.storage, "p", "T", deadline_s=1).run_once()   # the OLD, flawed wiring
    assert good.ok                                              # delivery is measured correctly
    assert not bad.ok and bad.error == "InternalServerError"    # the probe itself would have been broken by the fault


# ---------- metrics ----------
def test_p95_describes_successful_requests_and_failures_show_in_success_rate():
    all_failed = summarize([Sample(False, 5000, "event_not_delivered") for _ in range(10)])
    assert all_failed["success_rate"] == 0 and all_failed["p95_ms"] == 0 and all_failed["p95_all_ms"] == 5000
    ok, why = SS.check(all_failed)
    assert not ok and any("success_rate" in w for w in why) and not any("p95" in w for w in why)   # one clear reason, not two
    mixed = summarize([Sample(True, 100)] * 18 + [Sample(False, 5000, "x")] * 2)
    assert mixed["p95_ms"] == 100 and mixed["p95_all_ms"] == 5000


def test_slow_but_successful_requests_still_violate_latency():
    slow = summarize([Sample(True, 4000) for _ in range(10)])
    ok, why = SS.check(slow)
    assert not ok and any("p95" in w for w in why)


def test_too_few_samples_is_inconclusive_not_healthy():
    ok, why = SS.check(summarize([Sample(True, 1.0), Sample(True, 1.0)]))
    assert not ok and "samples" in why[0]


# ---------- history ----------
def test_aborted_experiments_do_not_count_as_coverage():
    universe = {("ServiceOutage", "sqs", "outage"), ("ServiceOutage", "lambda", "outage")}
    h = History(None, universe)
    h.add(SC, "g", {"impact": "aborted"})
    h.add(SC, "g")                                                  # crashed mid-run: no outcome at all
    assert h.coverage() == 0 and h.untried() == sorted(universe)
    h.add(SC, "g", {"impact": "no_impact"})
    assert h.coverage() == 0.5
    assert "ABORTED" in h.summarize() and "ignored" in h.summarize()


def test_loop_stops_after_repeated_aborts(aws, tmp_path, monkeypatch, capsys):
    """An unhealthy system must stop the loop loudly instead of burning rounds and inflating coverage."""
    topo = tmp_path / "t.json"
    topo.write_text(json.dumps({"name": "x", "functions": [{"name": "f"}], "queues": [{"name": "q"}]}))
    cfg = tmp_path / "e.json"
    cfg.write_text(json.dumps({"topology": str(topo), "workload": {"type": "invoke", "function": "f"},
                               "baseline_s": 0.1, "recovery_s": 0.1, "propagation_s": 0, "heal_timeout_s": 0,
                               "reset": {"drain_queues": ["q"]}}))
    from serverless_chaos import config as cfgmod

    class Down:
        def run_once(self): return Sample(False, 1.0, "down")

    monkeypatch.setattr(cfgmod, "build_workload", lambda cfg, adapters: Down())
    hist = tmp_path / "h.jsonl"
    main(["loop", "--config", str(cfg), "--history", str(hist), "--generator", "coverage", "--rounds", "8", "--max-aborts", "2"])
    out = capsys.readouterr().out
    h = History(str(hist))
    assert "STOPPING" in out and "diagnose" in out and len(h.records) == 2 and h.coverage() == 0


class _HealingWorkload:
    def __init__(self, bad_s):
        import time
        self.t0, self.bad_s = time.time(), bad_s

    def run_once(self):
        import time
        time.sleep(0.01)
        healthy = time.time() - self.t0 > self.bad_s
        return Sample(healthy, 5.0, None if healthy else "still_recovering")


def test_slow_recovery_from_previous_fault_is_waited_for_and_measured():
    runner = ExperimentRunner(InMemoryControlPlane(), _HealingWorkload(bad_s=0.8), SS, baseline_s=0.3,
                              recovery_s=0.2, concurrency=1, fault_window_s=0.2, propagation_s=0, heal_timeout_s=5)
    out = runner.run(SC)
    assert out.baseline_ok and out.impact != "aborted"
    assert out.heal_wait_s > 0.3


def test_system_that_never_heals_aborts_with_the_errors_in_the_reason():
    runner = ExperimentRunner(InMemoryControlPlane(), _HealingWorkload(bad_s=999), SS, baseline_s=0.2,
                              recovery_s=0.2, concurrency=1, propagation_s=0, heal_timeout_s=0.7)
    out = runner.run(SC)
    assert out.impact == "aborted" and "still_recovering" in out.reasons[0] and "after waiting" in out.reasons[0]
    assert out.heal_wait_s >= 0.7


def test_heal_time_is_attributed_to_the_previous_experiment_and_shown_to_the_llm():
    from serverless_chaos.cli import attribute_heal
    from serverless_chaos.experiment.runner import Outcome

    h = History(None, {("ServiceOutage", "sqs", "outage")})
    prev = h.add(SC, "g", {"impact": "violated"})
    attribute_heal(h, prev, Outcome(baseline={}, during={}, recovery={}, baseline_ok=True, held_during=True,
                                    recovered=True, impact="no_impact", heal_wait_s=42, started_at=0, duration_s=1))
    assert h.records[0].outcome["slow_to_heal_s"] == 42 and "needed 42s to heal" in h.summarize()
    attribute_heal(h, h.records[0], Outcome(baseline={}, during={}, recovery={}, baseline_ok=False, held_during=False,
                                            recovered=False, impact="aborted", heal_wait_s=60, started_at=0, duration_s=1))
    assert h.records[0].outcome["never_healed"] and "did NOT heal" in h.summarize()


def test_report_points_at_the_previous_experiment_for_each_abort(tmp_path, capsys):
    topo = tmp_path / "t.json"
    topo.write_text(json.dumps({"name": "x", "queues": [{"name": "q"}]}))
    cfg = tmp_path / "e.json"
    cfg.write_text(json.dumps({"topology": str(topo), "workload": {}}))
    h = History(str(tmp_path / "h.jsonl"))
    h.add(SC, "g", {"impact": "violated", "recovered": False})
    h.add(SC, "g", {"impact": "aborted", "reasons": ["unhealthy baseline: success_rate 0.00"],
                     "baseline": {"n": 6, "success_rate": 0.0, "p95_ms": 0,
                                  "errors": {"event_not_delivered": 6}}})
    main(["report", "--config", str(cfg), "--history", str(tmp_path / "h.jsonl")])
    out = capsys.readouterr().out
    assert "ABORTED #2" in out and "previous real experiment #1" in out and "recovered=False" in out
    assert "event_not_delivered" in out
