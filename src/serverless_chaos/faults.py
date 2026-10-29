"""Validated fault specifications (control side). `model_dump(mode="json")` yields the plain dict the runtime consumes.

Every fault has hard safety caps; the LLM can never request an unbounded fault.
"""
from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, Field, TypeAdapter, model_validator

MAX_DELAY_MS = 30_000
MAX_QUEUE_DELAY_MS = 900_000  # SQS DelaySeconds limit
MAX_DURATION_S = 300


class _Fault(BaseModel):
    probability: float = Field(1.0, ge=0.0, le=1.0, description="Chance each call is affected.")
    duration_s: int = Field(30, ge=1, le=MAX_DURATION_S, description="How long the fault stays active.")

    # --- identity used for novelty / coverage: (type, target, variant) ---
    @property
    def target(self) -> str: raise NotImplementedError
    @property
    def variant(self) -> str: return "-"
    @property
    def signature(self) -> tuple: return (self.type, self.target, self.variant)  # type: ignore[attr-defined]
    def short(self) -> str: return f"{self.type}@{self.target}({self.variant})"  # type: ignore[attr-defined]
    def to_runtime(self) -> dict: return self.model_dump(mode="json")


class LambdaLatency(_Fault):
    type: Literal["LambdaLatency"] = "LambdaLatency"
    function: str
    delay_ms: int = Field(2000, ge=1, le=MAX_DELAY_MS)
    jitter_ms: int = Field(0, ge=0, le=MAX_DELAY_MS)
    @property
    def target(self): return self.function
    @property
    def variant(self): return "latency"


class LambdaError(_Fault):
    type: Literal["LambdaError"] = "LambdaError"
    function: str
    error: Literal["exception", "timeout", "throttle"]
    @property
    def target(self): return self.function
    @property
    def variant(self): return self.error


class DynamoDBFault(_Fault):
    type: Literal["DynamoDBFault"] = "DynamoDBFault"
    table: str
    operations: Literal["read", "write", "all"] = "all"
    effect: Literal["latency", "throttle", "unavailable"]
    delay_ms: Optional[int] = Field(None, ge=1, le=MAX_DELAY_MS)
    @model_validator(mode="after")
    def _need_delay(self):
        if self.effect == "latency" and self.delay_ms is None:
            raise ValueError("delay_ms is required when effect is 'latency'")
        return self
    @property
    def target(self): return self.table
    @property
    def variant(self): return self.effect


class QueueFault(_Fault):
    type: Literal["QueueFault"] = "QueueFault"
    queue: str
    effect: Literal["delay", "drop", "duplicate", "unavailable"]
    delay_ms: Optional[int] = Field(None, ge=1, le=MAX_QUEUE_DELAY_MS)
    @model_validator(mode="after")
    def _need_delay(self):
        if self.effect == "delay" and self.delay_ms is None:
            raise ValueError("delay_ms is required when effect is 'delay'")
        return self
    @property
    def target(self): return self.queue
    @property
    def variant(self): return self.effect


class ServiceOutage(_Fault):
    type: Literal["ServiceOutage"] = "ServiceOutage"
    service: Literal["dynamodb", "sqs", "lambda"]
    @property
    def target(self): return self.service
    @property
    def variant(self): return "outage"


Fault = Annotated[Union[LambdaLatency, LambdaError, DynamoDBFault, QueueFault, ServiceOutage], Field(discriminator="type")]
_adapter = TypeAdapter(Fault)


def parse_fault(data: dict) -> _Fault:
    return _adapter.validate_python(data)


FAULT_CATALOG = """\
- LambdaLatency {function, delay_ms 1-30000, jitter_ms}: slows a Lambda handler (also affects event-triggered invocations).
- LambdaError {function, error: exception|timeout|throttle}: handler raises / hangs then fails / invoke is throttled (429).
- DynamoDBFault {table, operations: read|write|all, effect: latency|throttle|unavailable, delay_ms (required for latency)}: degraded table.
- QueueFault {queue, effect: delay|drop|duplicate|unavailable, delay_ms (required for delay)}: SQS message delayed, lost, delivered twice, or queue down.
- ServiceOutage {service: dynamodb|sqs|lambda}: every call to that service fails.
Every fault also takes probability (0-1) and duration_s (1-300)."""
