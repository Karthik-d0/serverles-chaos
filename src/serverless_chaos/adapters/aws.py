"""AWS / LocalStack backend (boto3 clients; only the endpoint URL differs between the two)."""
import json
import time
from decimal import Decimal
from typing import Any, List, Optional

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

from .base import ComputeAdapter, InvokeResult, Message, QueueAdapter, StorageAdapter

_ser, _de = TypeSerializer(), TypeDeserializer()


def _to_ddb(d: dict) -> dict:
    d = json.loads(json.dumps(d), parse_float=Decimal)  # DynamoDB has no float type
    return {k: _ser.serialize(v) for k, v in d.items()}


def _from_ddb(item: dict) -> dict:
    out = {k: _de.deserialize(v) for k, v in item.items()}
    return json.loads(json.dumps(out, default=lambda o: float(o) if o % 1 else int(o)))


class AwsStorageAdapter(StorageAdapter):
    def __init__(self, client, runtime=None):
        super().__init__(runtime)
        self.client = client

    def _put(self, table, item):
        self.client.put_item(TableName=table, Item=_to_ddb(item))

    def _get(self, table, key):
        r = self.client.get_item(TableName=table, Key=_to_ddb(key), ConsistentRead=True)
        return _from_ddb(r["Item"]) if "Item" in r else None

    def _delete(self, table, key):
        self.client.delete_item(TableName=table, Key=_to_ddb(key))

    def _scan(self, table, limit):
        return [_from_ddb(i) for i in self.client.scan(TableName=table, Limit=limit).get("Items", [])]


class AwsQueueAdapter(QueueAdapter):
    def __init__(self, client, runtime=None):
        super().__init__(runtime)
        self.client = client
        self._urls = {}

    def _url(self, queue: str) -> str:
        if queue not in self._urls:
            self._urls[queue] = self.client.get_queue_url(QueueName=queue)["QueueUrl"]
        return self._urls[queue]

    def _send(self, queue, body, delay_s):
        if not isinstance(body, str):
            body = json.dumps(body)
        r = self.client.send_message(QueueUrl=self._url(queue), MessageBody=body, DelaySeconds=int(delay_s))
        return r["MessageId"]

    def _receive(self, queue, max_messages, wait_s) -> List[Message]:
        r = self.client.receive_message(QueueUrl=self._url(queue), MaxNumberOfMessages=max_messages,
                                        WaitTimeSeconds=wait_s)
        return [Message(m["MessageId"], m["Body"], m["ReceiptHandle"]) for m in r.get("Messages", [])]

    def _delete(self, queue, receipt):
        self.client.delete_message(QueueUrl=self._url(queue), ReceiptHandle=receipt)

    def _depth(self, queue):
        a = self.client.get_queue_attributes(QueueUrl=self._url(queue), AttributeNames=["ApproximateNumberOfMessages"])
        return int(a["Attributes"]["ApproximateNumberOfMessages"])

    def _purge(self, queue):
        self.client.purge_queue(QueueUrl=self._url(queue))

    def _stats(self, queue):
        a = self.client.get_queue_attributes(QueueUrl=self._url(queue), AttributeNames=[
            "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed"])["Attributes"]
        return {"visible": int(a["ApproximateNumberOfMessages"]), "in_flight": int(a["ApproximateNumberOfMessagesNotVisible"]),
                "delayed": int(a["ApproximateNumberOfMessagesDelayed"])}


class AwsComputeAdapter(ComputeAdapter):
    def __init__(self, client, runtime=None):
        super().__init__(runtime)
        self.client = client

    def _invoke(self, function, payload) -> InvokeResult:
        t0 = time.perf_counter()
        r = self.client.invoke(FunctionName=function, InvocationType="RequestResponse",
                               Payload=json.dumps(payload if payload is not None else {}).encode())
        raw = r["Payload"].read()
        try:
            body: Any = json.loads(raw) if raw else None
        except ValueError:
            body = raw.decode(errors="replace")
        return InvokeResult(r["StatusCode"], body, r.get("FunctionError"), (time.perf_counter() - t0) * 1000.0)
