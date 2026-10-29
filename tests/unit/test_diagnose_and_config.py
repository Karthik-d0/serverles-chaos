import json
import os

import boto3

from serverless_chaos.adapters import build_adapters
from serverless_chaos.config import load_config
from serverless_chaos.diagnose import findings, leftover_faults, mapping_report, queue_report


def test_config_topology_path_resolves_from_any_cwd(tmp_path, monkeypatch):
    (tmp_path / "topology.json").write_text("{}")
    cfg = tmp_path / "exp.json"
    cfg.write_text(json.dumps({"topology": "topology.json", "workload": {}}))
    elsewhere = tmp_path / "far" / "away"; elsewhere.mkdir(parents=True)
    monkeypatch.chdir(elsewhere)
    assert load_config(str(cfg))["topology"] == str(tmp_path / "topology.json")        # found next to the config file


def test_diagnose_reports_leftover_faults_backlog_and_missing_mapping(aws):
    ad = build_adapters()
    ad.control.activate({"type": "DynamoDBFault", "table": "T", "effect": "throttle"}, 120)
    ad.queue.send("q", "stale-1"); ad.queue.send("q", "stale-2")
    left = leftover_faults(ad.storage.client)
    qs = queue_report(ad.queue, ["q"])
    maps = mapping_report(boto3.client("lambda", region_name="us-east-1"), [])
    out = findings(left, qs, maps, [], 5.0)
    text = "\n".join(out)
    assert "ACTIVE FAULTS" in text and "DynamoDBFault@T" in text
    assert "QUEUE BACKLOG on 'q'" in text
    assert "NO event source mapping" in text


def test_diagnose_flags_producer_failure_lost_events_and_tight_deadline():
    probes = [{"invoke_ms": 10, "invoke_ok": False, "function_error": "Unhandled", "payload": "boom", "e2e_ms": None},
              {"invoke_ms": 10, "invoke_ok": True, "function_error": None, "payload": None, "e2e_ms": None},
              {"invoke_ms": 10, "invoke_ok": True, "function_error": None, "payload": None, "e2e_ms": 4600}]
    text = "\n".join(findings([], {}, [{"function": "f", "source": "q", "state": "Enabled"}], probes, 5.0))
    assert "PRODUCER INVOCATION FAILS" in text and "EVENTS NEVER ARRIVED" in text and "close to the workload deadline" in text


def test_clean_system_reports_no_problem():
    out = findings([], {"q": {"visible": 0, "in_flight": 0, "delayed": 0}}, [{"function": "f", "source": "q", "state": "Enabled"}],
                   [{"invoke_ms": 5, "invoke_ok": True, "function_error": None, "payload": None, "e2e_ms": 900}], 5.0)
    assert len(out) == 1 and out[0].startswith("No problem found")
