"""sls-chaos command line interface."""
import argparse
import json
import os
import sys
import time

from .adapters import build_adapters
from .env import setup_environment
from .generator import CoverageGuidedGenerator, GenerationError, LLMGenerator, RandomGenerator, signature_universe
from .history import History
from .scenario import Scenario


def _make_generator(kind: str, seed: int):
    if kind in ("llm", "llm-open"):
        from .llm import llm_from_env
        return LLMGenerator(llm_from_env(), closed_loop=(kind == "llm"))
    return {"coverage": CoverageGuidedGenerator, "random": RandomGenerator}[kind](seed=seed)


def cmd_setup(a):
    ad = build_adapters(a.endpoint)
    ad.control.ensure_table()
    print(f"control table '{ad.control.table}' ready")


def cmd_discover(a):
    from .discover import apply_annotations, discover
    import boto3
    from .runtime.control import resolve_endpoint
    ep = a.endpoint or resolve_endpoint()
    kw = dict(endpoint_url=ep, region_name="us-east-1")
    topo = discover(boto3.client("lambda", **kw), boto3.client("dynamodb", **kw), boto3.client("sqs", **kw), a.name)
    if a.annotations:
        topo = apply_annotations(topo, json.load(open(a.annotations)))
    topo.save(a.out)
    print(topo.describe())
    print(f"\nsaved -> {a.out}")


def cmd_generate(a):
    from .config import load_config
    from .experiment.steady_state import SteadyState
    from .topology import Topology
    cfg = load_config(a.config)
    topo = Topology.load(cfg["topology"])
    hist = History(a.history, signature_universe(topo))
    try:
        sc = _make_generator(a.generator, a.seed).generate(topo, SteadyState(**cfg.get("steady_state", {})), hist)
    except GenerationError as e:
        sys.exit(f"generation failed: {e}")
    print(sc.model_dump_json(indent=2))


def _execute(runner, sc, hist, gen_name):
    rec = hist.add(sc, gen_name)
    out = runner.run(sc)
    hist.set_outcome(rec.id, out.model_dump())
    print(f"#{rec.id} {sc.short()} => {out.impact}"
          f"{' (' + '; '.join(out.reasons) + ')' if out.reasons else ''}  recovered={out.recovered}"
          f"{f'  (waited {out.heal_wait_s:.0f}s for the system to heal first)' if out.heal_wait_s and out.impact != 'aborted' else ''}")
    return rec, out


def attribute_heal(hist, prev_rec, out) -> None:
    if prev_rec is None or (prev_rec.outcome or {}).get("impact") not in History.VALID or not out.heal_wait_s:
        return
    if out.impact == "aborted":
        hist.annotate(prev_rec.id, never_healed=True)
    else:
        hist.annotate(prev_rec.id, slow_to_heal_s=round(out.heal_wait_s, 1))


def cmd_run(a):
    from .config import build_all
    ad = build_adapters(a.endpoint)
    topo, ss, runner = build_all(a.config, ad)
    hist = History(a.history, signature_universe(topo))
    sc = Scenario.model_validate_json(open(a.scenario).read())
    _execute(runner, sc, hist, "manual")


def cmd_clear(a):
    ad = build_adapters(a.endpoint)
    ad.control.clear()
    print("all faults cleared")


def cmd_calibrate(a):
    """Measure the HEALTHY system (no faults) and print what thresholds it can actually meet. Changes nothing."""
    from .config import build_all
    from .experiment.workload import run_for, summarize
    ad = build_adapters(a.endpoint)
    topo, ss, runner = build_all(a.config, ad)
    ad.control.clear(); time.sleep(runner.propagation_s); runner.prepare()
    m = summarize(run_for(runner.workload, a.seconds, runner.concurrency, runner.pace_s))
    print(json.dumps(m, indent=2))
    ok, why = ss.check(m)
    print(f"\nconfigured steady state ({ss.success_rate_min:.0%} success, p95 <= {ss.p95_ms_max:.0f} ms): {'HEALTHY' if ok else 'NOT met: ' + '; '.join(why)}")
    if m["n"]:
        print(f"measured: success {m['success_rate']:.1%}, p50 {m['p50_ms']:.0f} ms, p95 {m['p95_ms']:.0f} ms over {m['n']} samples")
        print("If success is ~100% and only the latency threshold fails, the threshold was unrealistic for this environment; "
              "set p95_ms_max above the measured p95 with a margin you can justify. If success is low, the system or the event flow is broken: run `sls-chaos diagnose`.")


def cmd_diagnose(a):
    import boto3
    from .config import build_workload, load_config
    from .diagnose import findings, leftover_faults, mapping_report, probe_events, queue_report, recent_logs
    from .runtime.control import resolve_endpoint
    from .topology import Topology
    cfg = load_config(a.config)
    topo = Topology.load(cfg["topology"])
    ad = build_adapters(a.endpoint)
    kw = dict(endpoint_url=a.endpoint or resolve_endpoint(), region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
    leftovers = leftover_faults(ad.storage.client)
    queues = queue_report(ad.queue, topo.names("queue"))
    mappings = mapping_report(ad.compute.client, topo.names("function"))
    print("== leftover faults in control table:", json.dumps(leftovers, indent=1) if leftovers else "none")
    print("== queues:", json.dumps(queues))
    print("== event source mappings:", json.dumps(mappings))
    w = cfg["workload"]
    probes = []
    if w["type"] == "eventflow":
        probe = build_adapters(control_table=None)
        print(f"== probing {a.probes} events one at a time (no concurrency) ...")
        probes = probe_events(ad.compute, probe.storage, w["producer"], w["sink_table"], a.probes, deadline_s=30)
        for p in probes:
            print("  ", json.dumps(p))
    logs = recent_logs(boto3.client("logs", **kw), topo.names("function"))
    print("== recent error log lines:", json.dumps(logs, indent=1))
    print("\n== FINDINGS")
    for f in findings(leftovers, queues, mappings, probes, w.get("deadline_s", 5.0)):
        print(" -", f)


def cmd_loop(a):
    from .config import build_all
    ad = build_adapters(a.endpoint)
    topo, ss, runner = build_all(a.config, ad)
    hist = History(a.history, signature_universe(topo))
    gen = _make_generator(a.generator, a.seed)
    streak, prev = 0, None
    for i in range(a.rounds):
        print(f"--- round {i + 1}/{a.rounds}  coverage {hist.coverage():.0%}")
        try:
            sc = gen.generate(topo, ss, hist)
        except GenerationError as e:
            print(f"generation failed, skipping round: {e}")
            continue
        rec, out = _execute(runner, sc, hist, gen.name)
        attribute_heal(hist, prev, out)
        prev = hist.records[-1] if out.impact in History.VALID else prev
        streak = streak + 1 if out.impact == "aborted" else 0
        if streak >= a.max_aborts:
            print(f"\nSTOPPING: {streak} experiments in a row aborted because the system was unhealthy BEFORE any fault was "
                  "injected. Results so far say nothing about resilience. Run: sls-chaos diagnose --config " + a.config)
            break
    cmd_report(argparse.Namespace(history=a.history, config=a.config))


def cmd_report(a):
    from .topology import Topology
    from .config import load_config
    universe = signature_universe(Topology.load(load_config(a.config)["topology"])) if a.config else set()
    h = History(a.history, universe)
    print(f"{'id':>3}  {'generator':<12} {'impact':<10} scenario")
    for r in h.records:
        print(f"{r.id:>3}  {r.generator:<12} {(r.outcome or {}).get('impact', '-'):<10} {r.scenario.short()}")
    for r in h.aborted():
        o = r.outcome
        before = next((x for x in reversed(h.records) if x.id < r.id and (x.outcome or {}).get("impact") in History.VALID), None)
        print(f"\nABORTED #{r.id} (its fault was NOT injected): {'; '.join(o['reasons'])}")
        print(f"   baseline: n={o['baseline'].get('n')} success={o['baseline'].get('success_rate', 0):.0%} "
              f"p95_ok={o['baseline'].get('p95_ms', 0):.0f}ms errors={o['baseline'].get('errors')}")
        if before:
            b = before.outcome
            print(f"   previous real experiment #{before.id} {before.scenario.short()} => {b.get('impact')}, "
                  f"recovered={b.get('recovered')}  <- first suspect")
    print(f"\nexperiments: {len(h.records)}   valid: {len(h.valid_records())}   ABORTED (not tested, not counted): {len(h.aborted())}   "
          f"violations: {len(h.violating())}\ndistinct combos tested: {len(h.tried())}"
          + (f"   coverage (valid experiments only): {h.coverage():.0%}" if universe else ""))


def main(argv=None):
    setup_environment()
    p = argparse.ArgumentParser(prog="sls-chaos", description=__doc__)
    p.add_argument("--endpoint", help="LocalStack URL (default: from SC_ENDPOINT_URL / AWS_ENDPOINT_URL)")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("setup", help="create the chaos control table"); s.set_defaults(fn=cmd_setup)
    s = sub.add_parser("discover", help="build topology.json from a running LocalStack")
    s.add_argument("--out", default="topology.json"); s.add_argument("--name", default="app"); s.add_argument("--annotations")
    s.set_defaults(fn=cmd_discover)
    s = sub.add_parser("clear", help="remove all active faults"); s.set_defaults(fn=cmd_clear)
    s = sub.add_parser("calibrate", help="measure the healthy system (no faults)"); s.set_defaults(fn=cmd_calibrate)
    s.add_argument("--config", required=True); s.add_argument("--seconds", type=float, default=30)
    s = sub.add_parser("diagnose", help="collect evidence about an unhealthy baseline"); s.set_defaults(fn=cmd_diagnose)
    s.add_argument("--config", required=True); s.add_argument("--probes", type=int, default=3)
    for name, fn in (("generate", cmd_generate), ("run", cmd_run), ("loop", cmd_loop), ("report", cmd_report)):
        s = sub.add_parser(name); s.set_defaults(fn=fn)
        s.add_argument("--config", required=(name != "report"), help="experiment.json")
        s.add_argument("--history", default="sandbox/history.jsonl")
        if name in ("generate", "loop"):
            s.add_argument("--generator", choices=["llm", "llm-open", "coverage", "random"], default="coverage")
            s.add_argument("--seed", type=int, default=0)
        if name == "run": s.add_argument("--scenario", required=True)
        if name == "loop":
            s.add_argument("--rounds", type=int, default=5)
            s.add_argument("--max-aborts", type=int, default=3, help="stop after this many consecutive aborted experiments")
    a = p.parse_args(argv)
    a.fn(a)


if __name__ == "__main__":
    main()
