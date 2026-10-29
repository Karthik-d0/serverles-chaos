import json

from serverless_chaos.cli import main
from serverless_chaos.history import History
from serverless_chaos.scenario import Scenario


def test_generate_and_report_offline(tmp_path, capsys):
    topo = tmp_path / "topology.json"
    topo.write_text(json.dumps({"name": "x", "functions": [{"name": "f"}], "tables": [{"name": "T"}], "queues": [{"name": "q"}]}))
    cfg = tmp_path / "exp.json"
    cfg.write_text(json.dumps({"topology": str(topo), "workload": {"type": "invoke", "function": "f"}}))
    hist = tmp_path / "h.jsonl"
    main(["generate", "--config", str(cfg), "--history", str(hist), "--generator", "coverage"])
    sc = Scenario.model_validate_json(capsys.readouterr().out)           # CLI prints a valid scenario
    h = History(str(hist)); h.add(sc, "coverage", {"impact": "violated"})
    main(["report", "--config", str(cfg), "--history", str(hist)])
    out = capsys.readouterr().out
    assert "violations: 1" in out and "coverage (valid experiments only)" in out and "ABORTED" in out

