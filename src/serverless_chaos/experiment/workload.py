"""Workloads drive the system during an experiment and produce samples. A sample's `ok` already includes
end-to-end event delivery for EventFlowWorkload - the steady state of an event-driven system is that events arrive."""
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Protocol


@dataclass
class Sample:
    ok: bool
    latency_ms: float
    error: Optional[str] = None


class Workload(Protocol):
    def run_once(self) -> Sample: ...


def _err(e: Exception) -> str:
    code = getattr(e, "response", {}).get("Error", {}).get("Code") if hasattr(e, "response") else None
    return code or type(e).__name__


class InvokeWorkload:
    """Synchronously invokes a function (request/response path)."""

    def __init__(self, compute, function: str, payload_factory: Callable[[], Any] = lambda: {}):
        self.compute, self.function, self.payload_factory = compute, function, payload_factory

    def run_once(self) -> Sample:
        t0 = time.perf_counter()
        try:
            r = self.compute.invoke(self.function, self.payload_factory())
            return Sample(r.ok, (time.perf_counter() - t0) * 1000, None if r.ok else (r.function_error or f"status {r.status_code}"))
        except Exception as e:
            return Sample(False, (time.perf_counter() - t0) * 1000, _err(e))


class EventFlowWorkload:
    """Triggers an event (invokes a producer) and waits until its effect appears in a sink table.
    Latency = end-to-end propagation time. Lost, late or failed events count as failures."""

    def __init__(self, compute, sink_reader, producer: str, sink_table: str, key_name: str = "id",
                 deadline_s: float = 5.0, poll_s: float = 0.1, payload_extra: Optional[dict] = None):
        # `compute` is the instrumented client (callers DO see throttling / outages).
        # `sink_reader` is the measuring instrument and must be built WITHOUT fault injection
        # (build_adapters(control_table=None).storage); otherwise a DynamoDB fault would break the probe itself.
        self.compute, self.storage = compute, sink_reader
        self.producer, self.sink_table, self.key_name = producer, sink_table, key_name
        self.deadline_s, self.poll_s, self.payload_extra = deadline_s, poll_s, payload_extra or {}

    def run_once(self) -> Sample:
        t0 = time.perf_counter()
        eid = uuid.uuid4().hex
        try:
            r = self.compute.invoke(self.producer, {**self.payload_extra, self.key_name: eid})
            if not r.ok:
                return Sample(False, (time.perf_counter() - t0) * 1000, r.function_error or f"status {r.status_code}")
            while time.perf_counter() - t0 < self.deadline_s:
                if self.storage.get(self.sink_table, {self.key_name: eid}) is not None:
                    return Sample(True, (time.perf_counter() - t0) * 1000)
                time.sleep(self.poll_s)
            return Sample(False, self.deadline_s * 1000, "event_not_delivered")
        except Exception as e:
            return Sample(False, (time.perf_counter() - t0) * 1000, _err(e))


def run_for(workload: Workload, duration_s: float, concurrency: int = 4, pace_s: float = 0.0) -> List[Sample]:
    samples: List[Sample] = []
    lock = threading.Lock()
    end = time.perf_counter() + duration_s

    def worker():
        while time.perf_counter() < end:
            try:
                s = workload.run_once()
            except Exception as e:  # a workload must never crash the runner
                s = Sample(False, 0.0, type(e).__name__)
            with lock:
                samples.append(s)
            if pace_s:
                time.sleep(pace_s)

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(concurrency)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    return samples


def summarize(samples: List[Sample]) -> dict:
    n = len(samples)
    if n == 0:
        return {"n": 0, "success_rate": 0.0, "p50_ms": 0.0, "p95_ms": 0.0, "p95_all_ms": 0.0, "errors": {}}
    errors: dict = {}
    for s in samples:
        if not s.ok:
            errors[s.error or "unknown"] = errors.get(s.error or "unknown", 0) + 1

    def pct(values, p):
        values = sorted(values)
        return values[min(len(values) - 1, int(p * len(values)))] if values else 0.0

    ok_lat = [s.latency_ms for s in samples if s.ok]
    # p50/p95 describe the latency of requests that WORKED. Failures are measured by success_rate; their "latency" is just
    # the timeout and would otherwise make an all-failed phase read as "p95 = deadline". p95_all_ms keeps the raw view.
    return {"n": n, "success_rate": len(ok_lat) / n, "p50_ms": pct(ok_lat, 0.50), "p95_ms": pct(ok_lat, 0.95),
            "p95_all_ms": pct([s.latency_ms for s in samples], 0.95), "errors": errors}
