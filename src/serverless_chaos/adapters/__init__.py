"""Unified adapter layer (stdlib + boto3 only). Fault injection lives in the base classes, so any backend inherits it."""
from .base import ComputeAdapter, InvokeResult, Message, QueueAdapter, StorageAdapter
from .aws import AwsComputeAdapter, AwsQueueAdapter, AwsStorageAdapter
from .factory import AdapterSet, build_adapters

__all__ = [
    "StorageAdapter", "QueueAdapter", "ComputeAdapter", "Message", "InvokeResult",
    "AwsStorageAdapter", "AwsQueueAdapter", "AwsComputeAdapter", "AdapterSet", "build_adapters",
]
