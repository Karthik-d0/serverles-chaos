import os
from dataclasses import dataclass
from typing import Optional

import boto3

from ..runtime.control import DynamoControlPlane, resolve_endpoint
from ..runtime.engine import FaultRuntime, NullRuntime
from .aws import AwsComputeAdapter, AwsQueueAdapter, AwsStorageAdapter


@dataclass
class AdapterSet:
    storage: AwsStorageAdapter
    queue: AwsQueueAdapter
    compute: AwsComputeAdapter
    runtime: object
    control: Optional[DynamoControlPlane] = None


def build_adapters(endpoint_url: Optional[str] = None, region: Optional[str] = None,
                   control_table: Optional[str] = "chaos-control", runtime=None, session=None) -> AdapterSet:
    """One call to get all three adapters wired to the same fault runtime.

    endpoint_url=None resolves from env (SC_ENDPOINT_URL / AWS_ENDPOINT_URL / LOCALSTACK_HOSTNAME).
    Pass control_table=None (or set SC_CHAOS_DISABLED=1) to run without fault injection.
    """
    endpoint_url = endpoint_url or resolve_endpoint()
    region = region or os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    s = session or boto3
    kw = dict(endpoint_url=endpoint_url, region_name=region)
    control = None
    if runtime is None:
        if control_table is None or os.getenv("SC_CHAOS_DISABLED") == "1":
            runtime = NullRuntime()
        else:
            control = DynamoControlPlane(control_table, client=s.client("dynamodb", **kw))
            runtime = FaultRuntime(control)
    return AdapterSet(
        storage=AwsStorageAdapter(s.client("dynamodb", **kw), runtime),
        queue=AwsQueueAdapter(s.client("sqs", **kw), runtime),
        compute=AwsComputeAdapter(s.client("lambda", **kw), runtime),
        runtime=runtime, control=control)
