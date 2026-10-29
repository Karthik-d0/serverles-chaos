"""Dependency-light fault runtime (stdlib + boto3 only) - safe to bundle into Lambda packages."""
from .catalog import KIND_OF_SERVICE, KIND_OF_TYPE, TARGET_KEY
from .engine import Decision, FaultRuntime, NullRuntime
from .control import ControlPlane, DynamoControlPlane, InMemoryControlPlane, resolve_endpoint
from .errors import InjectedFaultError, make_client_error
from .lambda_hook import chaos_lambda, default_runtime

__all__ = [
    "KIND_OF_SERVICE", "KIND_OF_TYPE", "TARGET_KEY", "Decision", "FaultRuntime", "NullRuntime",
    "ControlPlane", "DynamoControlPlane", "InMemoryControlPlane", "resolve_endpoint",
    "InjectedFaultError", "make_client_error", "chaos_lambda", "default_runtime",
]
