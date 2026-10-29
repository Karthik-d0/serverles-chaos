"""Runs one scenario: prepare -> baseline -> inject -> recovery, always rolling faults back."""
import time
from typing import Callable, Optional

from pydantic import BaseModel

from ..scenario import Scenario
from .steady_state import SteadyState
from .workload import Workload, run_for, summarize


class Outcome(BaseModel):
    baseline: dict
    during: dict
    recovery: dict
    baseline_ok: bool
    held_during: bool
    recovered: bool
    impact: str                 # "no_impact" | "violated" | "aborted"   (aborted = the fault was never actually tested)
    reasons: list = []
    heal_wait_s: float = 0.0
    started_at: float
    duration_s: float


class ExperimentRunner:
    def __init__(self, control, workload: Workload, steady_state: SteadyState,
                 baseline_s: float = 5, recovery_s: float = 5, concurrency: int = 4,
                 pace_s: float = 0.0, fault_window_s: Optional[float] = None, settle_s: float = 0.0,
                 prepare: Optional[Callable[[], None]] = None, propagation_s: float = 1.0,
                 heal_timeout_s: float = 60.0):
        """prepare: runs before every baseline to return the system to a clean state (e.g. drain queues).
        propagation_s: Lambdas cache the fault list for ~0.5 s, so wait this long after changing faults before measuring."""
        self.control, self.workload, self.ss = control, workload, steady_state
        self.baseline_s, self.recovery_s, self.concurrency = baseline_s, recovery_s, concurrency
        self.pace_s, self.fault_window_s, self.settle_s = pace_s, fault_window_s, settle_s
        self.prepare, self.propagation_s, self.heal_timeout_s = prepare, propagation_s, heal_timeout_s

    def _phase(self, seconds: float) -> dict:
        return summarize(run_for(self.workload, seconds, self.concurrency, self.pace_s))

    def _aborted(self, t0: float, baseline: dict, reason: str, heal_wait_s: float = 0.0) -> Outcome:
        empty = summarize([])
        return Outcome(baseline=baseline, during=empty, recovery=empty, baseline_ok=False, held_during=False,
                       recovered=False, impact="aborted", reasons=[reason], heal_wait_s=heal_wait_s,
                       started_at=t0, duration_s=time.time() - t0)

    def _healthy_baseline(self):
        start, windows = time.time(), 0
        while True:
            m = self._phase(self.baseline_s)
            windows += 1
            ok, why = self.ss.check(m)
            elapsed = time.time() - start
            if ok:
                return m, True, [], (elapsed - self.baseline_s if windows > 1 else 0.0)
            if elapsed >= self.heal_timeout_s:
                return m, False, why, elapsed
            if self.prepare:
                self.prepare()

    def run(self, scenario: Scenario) -> Outcome:
        t0 = time.time()
        self.control.clear()
        time.sleep(self.propagation_s)
        if self.prepare:
            try:
                self.prepare()
            except Exception as e:
                return self._aborted(t0, summarize([]), f"could not reset the system before the baseline: {type(e).__name__}: {e}")
        try:
            baseline, baseline_ok, why, heal_wait = self._healthy_baseline()
        except Exception as e:
            return self._aborted(t0, summarize([]),
                                 f"could not reset the system while waiting for it to heal: {type(e).__name__}: {e}")
        if not baseline_ok:  # never blame a fault for an already-unhealthy system
            errs = f"; errors={baseline['errors']}" if baseline.get("errors") else ""
            return self._aborted(
                t0, baseline,
                f"unhealthy baseline (still unhealthy after waiting {heal_wait:.0f}s): "
                + "; ".join(why) + errs,
                heal_wait_s=heal_wait,
            )
        window = self.fault_window_s if self.fault_window_s is not None else scenario.window_s
        try:
            for f in scenario.faults:
                self.control.activate(f.to_runtime(), ttl_s=window + self.propagation_s + 5)
            time.sleep(self.propagation_s)
            during = self._phase(window)
        finally:                      # rollback no matter what happens
            self.control.clear()
        time.sleep(self.propagation_s + self.settle_s)
        recovery = self._phase(self.recovery_s)
        held, reasons = self.ss.check(during)
        recovered, _ = self.ss.check(recovery)
        return Outcome(baseline=baseline, during=during, recovery=recovery, baseline_ok=True, held_during=held,
                       recovered=recovered, impact="no_impact" if held else "violated", reasons=reasons,
                       heal_wait_s=heal_wait, started_at=t0, duration_s=time.time() - t0)
