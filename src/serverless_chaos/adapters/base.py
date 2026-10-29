"""Abstract adapters. Public methods are *template methods*: they run fault injection, then call the
backend-specific `_xxx` method. Backends (AWS/LocalStack today, anything else later) only implement `_xxx`.
"""
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from botocore.exceptions import ClientError

from ..runtime.engine import Decision, NullRuntime
from ..runtime.errors import make_client_error


@dataclass
class Message:
    id: str
    body: str
    receipt: str = ""
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class InvokeResult:
    status_code: int
    payload: Any
    function_error: Optional[str]
    latency_ms: float

    @property
    def ok(self) -> bool:
        if self.function_error or self.status_code >= 400:
            return False
        if isinstance(self.payload, dict) and isinstance(self.payload.get("statusCode"), int):
            return self.payload["statusCode"] < 400
        return True


class _Faultable:
    kind = ""

    def __init__(self, runtime=None):
        self.runtime = runtime or NullRuntime()

    def _inject(self, target: str, op: str, api_name: str) -> Decision:
        d = self.runtime.decide(self.kind, target, op)
        if d.delay_s:
            time.sleep(d.delay_s)
        if d.error:
            raise make_client_error(d.error[0], d.error[1], api_name)
        return d


class StorageAdapter(_Faultable, ABC):
    kind = "storage"

    def put(self, table: str, item: dict) -> None:
        self._inject(table, "write", "PutItem")
        self._put(table, item)

    def get(self, table: str, key: dict) -> Optional[dict]:
        self._inject(table, "read", "GetItem")
        return self._get(table, key)

    def delete(self, table: str, key: dict) -> None:
        self._inject(table, "write", "DeleteItem")
        self._delete(table, key)

    def scan(self, table: str, limit: int = 100) -> List[dict]:
        self._inject(table, "read", "Scan")
        return self._scan(table, limit)

    @abstractmethod
    def _put(self, table: str, item: dict) -> None: ...
    @abstractmethod
    def _get(self, table: str, key: dict) -> Optional[dict]: ...
    @abstractmethod
    def _delete(self, table: str, key: dict) -> None: ...
    @abstractmethod
    def _scan(self, table: str, limit: int) -> List[dict]: ...


class QueueAdapter(_Faultable, ABC):
    kind = "queue"

    def send(self, queue: str, body: Any, delay_s: int = 0) -> str:
        d = self._inject(queue, "send", "SendMessage")
        if d.drop:                                   # message silently lost
            return "dropped"
        delay_s = max(delay_s, d.queue_delay_s)
        msg_id = self._send(queue, body, delay_s)
        if d.duplicate:                              # delivered twice
            self._send(queue, body, delay_s)
        return msg_id

    def receive(self, queue: str, max_messages: int = 1, wait_s: int = 0) -> List[Message]:
        self._inject(queue, "receive", "ReceiveMessage")
        return self._receive(queue, max_messages, wait_s)

    def delete(self, queue: str, receipt: str) -> None:
        self._inject(queue, "receive", "DeleteMessage")
        self._delete(queue, receipt)

    def depth(self, queue: str) -> int:
        return self._depth(queue)

    def purge(self, queue: str) -> None:
        self._purge(queue)

    def stats(self, queue: str) -> Dict[str, int]:
        """{'visible', 'in_flight', 'delayed'}: everything that could still be delivered or is being processed."""
        return self._stats(queue)

    def drain(self, queue: str, timeout_s: float = 60.0, poll_s: float = 0.5) -> None:
        """Empty the queue and wait until nothing is visible, in flight (being processed) or delayed.

        Leftover messages from a previous experiment (dropped/delayed/duplicated/failed-and-redelivered events) would
        otherwise leak into the next baseline and make a healthy system look broken.
        """
        try:
            self._purge(queue)
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") != "PurgeQueueInProgress":   # AWS allows 1 purge / 60 s
                raise
            while True:                                  # fall back to receive+delete
                batch = self._receive(queue, 10, 0)
                if not batch:
                    break
                for m in batch:
                    self._delete(queue, m.receipt)
        end, quiet = time.time() + timeout_s, 0
        while True:
            quiet = quiet + 1 if sum(self._stats(queue).values()) == 0 else 0
            if quiet >= 2:                               # two consecutive empty readings
                return
            if time.time() > end:
                raise TimeoutError(f"queue '{queue}' did not drain within {timeout_s}s: {self._stats(queue)}")
            time.sleep(poll_s)

    @abstractmethod
    def _send(self, queue: str, body: Any, delay_s: int) -> str: ...
    @abstractmethod
    def _receive(self, queue: str, max_messages: int, wait_s: int) -> List[Message]: ...
    @abstractmethod
    def _delete(self, queue: str, receipt: str) -> None: ...
    @abstractmethod
    def _depth(self, queue: str) -> int: ...
    @abstractmethod
    def _purge(self, queue: str) -> None: ...
    @abstractmethod
    def _stats(self, queue: str) -> Dict[str, int]: ...


class ComputeAdapter(_Faultable, ABC):
    """Invoke-side only injects throttling/outage; latency/exceptions are injected handler-side (chaos_lambda)."""
    kind = "compute"

    def invoke(self, function: str, payload: Any = None) -> InvokeResult:
        t0 = time.perf_counter()
        self._inject(function, "invoke", "Invoke")
        res = self._invoke(function, payload)
        res.latency_ms = (time.perf_counter() - t0) * 1000.0  # includes any injected delay
        return res

    @abstractmethod
    def _invoke(self, function: str, payload: Any) -> InvokeResult: ...
