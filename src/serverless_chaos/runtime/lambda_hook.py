"""Decorator that makes a Lambda handler fault-injectable (handler-side: latency, exception, timeout)."""
import functools
import os
import time

from .control import DynamoControlPlane
from .engine import FaultRuntime, NullRuntime
from .errors import InjectedFaultError

_default = None


def default_runtime():
    """Runtime configured from env: SC_CHAOS_DISABLED=1 turns chaos off; SC_CONTROL_TABLE names the control table."""
    global _default
    if _default is None:
        if os.getenv("SC_CHAOS_DISABLED") == "1":
            _default = NullRuntime()
        else:
            _default = FaultRuntime(DynamoControlPlane(os.getenv("SC_CONTROL_TABLE", "chaos-control")))
    return _default


def chaos_lambda(function_name: str, runtime=None):
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(event, context):
            d = (runtime or default_runtime()).decide("compute", function_name, "handler")
            if d.delay_s:
                time.sleep(d.delay_s)
            if d.error:
                raise InjectedFaultError(*d.error)
            return fn(event, context)
        return wrapper
    return deco
