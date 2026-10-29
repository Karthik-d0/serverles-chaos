"""Control plane: where active faults live. Apps (and Lambdas) read it; the experiment runner writes it.

DynamoControlPlane keeps one item per active fault in a DynamoDB table, so deployed Lambdas see
the same faults as the runner. It FAILS OPEN: if the table is unreachable, no faults are applied
and the application is never broken by the chaos tooling itself.
"""
import json
import logging
import os
import time
import uuid
from typing import List, Optional

import boto3

log = logging.getLogger("serverless_chaos")


def resolve_endpoint() -> Optional[str]:
    """Endpoint for LocalStack, from env. None means real AWS."""
    ep = os.getenv("SC_ENDPOINT_URL") or os.getenv("AWS_ENDPOINT_URL")
    if ep:
        return ep
    host = os.getenv("LOCALSTACK_HOSTNAME")
    if host:
        return f"http://{host}:{os.getenv('EDGE_PORT', '4566')}"
    return None


class ControlPlane:
    def activate(self, fault: dict, ttl_s: float) -> str: raise NotImplementedError
    def active(self) -> List[dict]: raise NotImplementedError
    def clear(self) -> None: raise NotImplementedError


class InMemoryControlPlane(ControlPlane):
    def __init__(self, clock=time.time):
        self._faults = {}
        self._clock = clock

    def activate(self, fault: dict, ttl_s: float) -> str:
        fid = fault.get("id") or uuid.uuid4().hex[:8]
        self._faults[fid] = ({**fault, "id": fid}, self._clock() + ttl_s)
        return fid

    def active(self) -> List[dict]:
        now = self._clock()
        return [f for f, exp in self._faults.values() if exp > now]

    def clear(self) -> None:
        self._faults.clear()


class DynamoControlPlane(ControlPlane):
    def __init__(self, table: str = "chaos-control", client=None, cache_ttl_s: float = 0.5,
                 clock=time.time, endpoint_url: Optional[str] = None, region: Optional[str] = None):
        self.table = table
        self.cache_ttl_s = cache_ttl_s
        self._clock = clock
        self._cache: List[dict] = []
        self._cache_at = 0.0
        self.client = client or boto3.client(
            "dynamodb", endpoint_url=endpoint_url or resolve_endpoint(),
            region_name=region or os.getenv("AWS_DEFAULT_REGION", "us-east-1"))

    def ensure_table(self) -> None:
        try:
            self.client.describe_table(TableName=self.table)
        except self.client.exceptions.ResourceNotFoundException:
            self.client.create_table(
                TableName=self.table,
                AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST")

    def activate(self, fault: dict, ttl_s: float) -> str:
        fid = fault.get("id") or uuid.uuid4().hex[:8]
        spec = {**fault, "id": fid}
        self.client.put_item(TableName=self.table, Item={
            "id": {"S": fid}, "spec": {"S": json.dumps(spec)},
            "expires_at": {"N": repr(self._clock() + ttl_s)}})
        self._cache_at = 0.0  # invalidate
        return fid

    def active(self) -> List[dict]:
        now = self._clock()
        if now - self._cache_at < self.cache_ttl_s:
            return self._cache
        try:
            items, kwargs = [], {}
            while True:
                resp = self.client.scan(TableName=self.table, **kwargs)
                items += resp.get("Items", [])
                if "LastEvaluatedKey" not in resp:
                    break
                kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
            self._cache = [json.loads(i["spec"]["S"]) for i in items if float(i["expires_at"]["N"]) > now]
        except Exception as e:  # fail open
            log.warning("chaos control plane unavailable, running without faults: %s", e)
            self._cache = []
        self._cache_at = now
        return self._cache

    def clear(self) -> None:
        resp = self.client.scan(TableName=self.table, ProjectionExpression="id")
        for it in resp.get("Items", []):
            self.client.delete_item(TableName=self.table, Key={"id": it["id"]})
        self._cache, self._cache_at = [], 0.0
