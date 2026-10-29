"""Experiment configuration file (JSON) -> runner + workload."""
import json
from pathlib import Path
from typing import Tuple

from .adapters import AdapterSet, build_adapters
from .experiment.runner import ExperimentRunner
from .experiment.steady_state import SteadyState
from .experiment.workload import EventFlowWorkload, InvokeWorkload
from .topology import Topology


def _resolve(path: str, cfg_path: str) -> str:
    """Relative paths in the config work from any cwd: as given, next to the config file, or from the project root."""
    from .env import _package_root
    cands = [Path(path), Path(cfg_path).resolve().parent / path]
    if _package_root():
        cands.append(_package_root() / path)
    return str(next((c for c in cands if c.exists()), Path(path)))


def load_config(path: str) -> dict:
    with open(path) as f:
        cfg = json.load(f)
    if "topology" in cfg:
        cfg["topology"] = _resolve(cfg["topology"], path)
    return cfg


def build_workload(cfg: dict, adapters: AdapterSet):
    """Compute calls go through the instrumented adapters; event-sink polling uses an UNINSTRUMENTED reader."""
    w = cfg["workload"]
    if w["type"] == "invoke":
        payload = w.get("payload", {})
        return InvokeWorkload(adapters.compute, w["function"], lambda: dict(payload))
    if w["type"] == "eventflow":
        probe = build_adapters(control_table=None)           # measuring instrument: immune to injected faults
        return EventFlowWorkload(adapters.compute, probe.storage, w["producer"], w["sink_table"],
                                 key_name=w.get("key_name", "id"), deadline_s=w.get("deadline_s", 5.0),
                                 payload_extra=w.get("payload", {}))
    raise ValueError(f"unknown workload type {w['type']!r} (use 'invoke' or 'eventflow')")


def build_all(cfg_path: str, adapters: AdapterSet) -> Tuple[Topology, SteadyState, ExperimentRunner]:
    cfg = load_config(cfg_path)
    topo = Topology.load(cfg["topology"])
    ss = SteadyState(**cfg.get("steady_state", {}))
    reset = cfg.get("reset", {})
    queues = reset.get("drain_queues", topo.names("queue"))
    timeout = reset.get("timeout_s", 60)

    def prepare():                      # return the system to a clean state before every baseline
        for q in queues:
            adapters.queue.drain(q, timeout_s=timeout)

    runner = ExperimentRunner(adapters.control, build_workload(cfg, adapters), ss, prepare=prepare,
                              propagation_s=cfg.get("propagation_s", 1.0),
                              heal_timeout_s=cfg.get("heal_timeout_s", 60.0),
                              baseline_s=cfg.get("baseline_s", 5), recovery_s=cfg.get("recovery_s", 5),
                              concurrency=cfg.get("concurrency", 4), pace_s=cfg.get("pace_s", 0.0),
                              fault_window_s=cfg.get("fault_window_s"), settle_s=cfg.get("settle_s", 0.0))
    return topo, ss, runner
