"""Decides what an injected fault does to one call. Pure logic: no AWS calls, easy to unit-test.

A *fault* is a plain dict (see serverless_chaos.faults for the validated pydantic models that produce it):
  {"id", "type", "probability", <target key>, ... type specific fields}
"""
import random
import time
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from .catalog import KIND_OF_SERVICE, KIND_OF_TYPE, TARGET_KEY

TIMEOUT_SIM_S = 3.0  # how long a simulated Lambda timeout "hangs" before failing


@dataclass
class Decision:
    delay_s: float = 0.0
    error: Optional[Tuple[str, str]] = None      # (error code, message)
    drop: bool = False                            # queue send: swallow the message
    duplicate: bool = False                       # queue send: deliver twice
    queue_delay_s: int = 0                        # queue send: DelaySeconds
    applied: List[str] = field(default_factory=list)  # ids of faults that fired

    @property
    def active(self) -> bool:
        return bool(self.applied)


class NullRuntime:
    """No faults. Used when chaos is disabled or no control plane is configured."""

    def decide(self, kind: str, target: str, op: str) -> Decision:
        return Decision()


class FaultRuntime:
    def __init__(self, control, rng: Optional[random.Random] = None):
        self.control = control
        self.rng = rng or random.Random()

    # ---- matching ----------------------------------------------------------------
    @staticmethod
    def _matches(fault: dict, kind: str, target: str) -> bool:
        ftype = fault.get("type")
        if ftype == "ServiceOutage":
            return KIND_OF_SERVICE.get(fault.get("service")) == kind
        if KIND_OF_TYPE.get(ftype) != kind:
            return False
        return fault.get(TARGET_KEY[ftype]) == target

    # ---- decision ------------------------------------------------------------------
    def decide(self, kind: str, target: str, op: str) -> Decision:
        """kind: storage|queue|compute. op: read|write (storage), send|receive (queue), invoke|handler (compute)."""
        d = Decision()
        for f in self.control.active():
            if not self._matches(f, kind, target):
                continue
            if self.rng.random() > float(f.get("probability", 1.0)):
                continue
            if self._apply(f, kind, op, d):
                d.applied.append(f.get("id", f.get("type", "?")))
        return d

    def _apply(self, f: dict, kind: str, op: str, d: Decision) -> bool:
        t = f["type"]
        if t == "ServiceOutage":
            code = {"storage": "InternalServerError", "queue": "ServiceUnavailable", "compute": "ServiceException"}[kind]
            return self._set_error(d, code, f"{f['service']} outage")
        if t == "LambdaLatency" and op == "handler":
            jitter = f.get("jitter_ms", 0) or 0
            d.delay_s += max(0.0, (f["delay_ms"] + self.rng.uniform(-jitter, jitter)) / 1000.0)
            return True
        if t == "LambdaError":
            err = f["error"]
            if err == "throttle" and op == "invoke":
                return self._set_error(d, "TooManyRequestsException", "Rate exceeded")
            if op == "handler" and err == "exception":
                return self._set_error(d, "InjectedException", "Unhandled exception")
            if op == "handler" and err == "timeout":
                d.delay_s += TIMEOUT_SIM_S
                return self._set_error(d, "TaskTimedOut", "Task timed out")
            return False
        if t == "DynamoDBFault":
            if f.get("operations", "all") not in ("all", op):
                return False
            eff = f["effect"]
            if eff == "latency":
                d.delay_s += f["delay_ms"] / 1000.0
                return True
            if eff == "throttle":
                return self._set_error(d, "ProvisionedThroughputExceededException", "Throughput exceeded")
            return self._set_error(d, "InternalServerError", "DynamoDB unavailable")
        if t == "QueueFault":
            eff = f["effect"]
            if eff == "unavailable":
                return self._set_error(d, "ServiceUnavailable", "SQS unavailable")
            if op != "send":
                return False
            if eff == "drop":
                d.drop = True
            elif eff == "duplicate":
                d.duplicate = True
            elif eff == "delay":
                d.queue_delay_s = max(d.queue_delay_s, min(900, int(-(-f["delay_ms"] // 1000))))
            return True
        return False

    @staticmethod
    def _set_error(d: Decision, code: str, msg: str) -> bool:
        if d.error is None:
            d.error = (code, msg)
        return True
