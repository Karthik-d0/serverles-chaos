"""Evidence gathering for an unhealthy baseline. Every section reports facts; nothing is guessed.

    sls-chaos diagnose --config examples/items_app/experiment.json
"""
import json
import time
import uuid
from typing import Dict, List


def leftover_faults(ddb, table: str = "chaos-control") -> List[dict]:
    """Every item in the control table, expired or not. Anything here while idle is a leftover from a previous run."""
    now, out = time.time(), []
    try:
        for it in ddb.scan(TableName=table).get("Items", []):
            spec = json.loads(it["spec"]["S"])
            spec["_expires_in_s"] = round(float(it["expires_at"]["N"]) - now, 1)
            out.append(spec)
    except Exception as e:
        out.append({"error": f"cannot read control table: {e}"})
    return out


def queue_report(queue_adapter, queues: List[str]) -> Dict[str, dict]:
    return {q: queue_adapter.stats(q) for q in queues}


def mapping_report(lam, functions: List[str]) -> List[dict]:
    rows = []
    for fn in functions:
        for m in lam.list_event_source_mappings(FunctionName=fn)["EventSourceMappings"]:
            rows.append({"function": fn, "source": m["EventSourceArn"].rsplit(":", 1)[-1], "state": m.get("State"),
                         "batch_size": m.get("BatchSize"), "last_result": m.get("LastProcessingResult")})
    return rows


def probe_events(compute, sink_reader, producer: str, sink_table: str, n: int = 3, deadline_s: float = 30.0) -> List[dict]:
    """Send n events ONE AT A TIME (no concurrency) and time each stage. Shows cold start vs SQS->Lambda latency."""
    rows = []
    for _ in range(n):
        eid = uuid.uuid4().hex
        t0 = time.perf_counter()
        r = compute.invoke(producer, {"id": eid})
        t_invoke = (time.perf_counter() - t0) * 1000
        row = {"invoke_ms": round(t_invoke), "invoke_ok": r.ok, "function_error": r.function_error,
               "payload": str(r.payload)[:200] if not r.ok else None, "e2e_ms": None}
        if r.ok:
            while time.perf_counter() - t0 < deadline_s:
                if sink_reader.get(sink_table, {"id": eid}) is not None:
                    row["e2e_ms"] = round((time.perf_counter() - t0) * 1000)
                    break
                time.sleep(0.1)
        rows.append(row)
    return rows


def recent_logs(logs, functions: List[str], limit: int = 8) -> Dict[str, List[str]]:
    out = {}
    for fn in functions:
        try:
            ev = logs.filter_log_events(logGroupName=f"/aws/lambda/{fn}", limit=50).get("events", [])
            out[fn] = [e["message"].strip()[:160] for e in ev if "ERROR" in e["message"] or "Traceback" in e["message"]
                       or "injected" in e["message"] or "Task timed out" in e["message"]][-limit:]
        except Exception as e:
            out[fn] = [f"(no logs: {type(e).__name__})"]
    return out


def findings(leftovers, queues, mappings, probes, deadline_s) -> List[str]:
    f = []
    active = [x for x in leftovers if "error" not in x and x.get("_expires_in_s", 0) > 0]
    if active:
        f.append(f"ACTIVE FAULTS while idle: {[x['type'] + '@' + str(x.get('function') or x.get('table') or x.get('queue') or x.get('service')) for x in active]}"
                 " -> a previous run left faults behind (or another process is injecting). Run `sls-chaos clear`.")
    for q, s in queues.items():
        if s["visible"] + s["in_flight"] + s["delayed"] > 0:
            f.append(f"QUEUE BACKLOG on '{q}': {s}. Old events compete with new ones and inflate latency / lose events.")
    for m in mappings:
        if m["state"] != "Enabled":
            f.append(f"EVENT SOURCE MAPPING {m['source']} -> {m['function']} is '{m['state']}', not Enabled: events will not be consumed.")
    if not mappings:
        f.append("NO event source mapping found: nothing consumes the queue. Run deploy.py.")
    bad = [p for p in probes if not p["invoke_ok"]]
    if bad:
        f.append(f"PRODUCER INVOCATION FAILS ({len(bad)}/{len(probes)}): {bad[0]['function_error'] or ''} {bad[0]['payload'] or ''}")
    lost = [p for p in probes if p["invoke_ok"] and p["e2e_ms"] is None]
    if lost:
        f.append(f"EVENTS NEVER ARRIVED ({len(lost)}/{len(probes)} within probe deadline): consumer not running or failing; see log excerpts.")
    slow = [p["e2e_ms"] for p in probes if p["e2e_ms"]]
    if slow and max(slow) > deadline_s * 1000 * 0.8:
        f.append(f"END-TO-END latency {max(slow)} ms is close to the workload deadline ({deadline_s * 1000:.0f} ms): "
                 "the deadline/threshold is unrealistic for this environment, not the system broken. Run `sls-chaos calibrate`.")
    return f or ["No problem found by these checks. Run `sls-chaos calibrate` to measure the healthy baseline."]
