"""Build a Topology from a live LocalStack/AWS account. Event-source mappings give the trigger edges;
everything the APIs cannot reveal (which function writes which table) comes from an annotations file."""
import json
from typing import Optional

from .topology import Edge, FunctionNode, QueueNode, TableNode, Topology

INTERNAL_PREFIX = "chaos-"  # our own control table etc. is never a fault target


def discover(lambda_client, dynamodb_client, sqs_client, name: str = "app") -> Topology:
    topo = Topology(name=name)
    for page in lambda_client.get_paginator("list_functions").paginate():
        for f in page["Functions"]:
            topo.functions.append(FunctionNode(name=f["FunctionName"], timeout_s=f.get("Timeout"), memory_mb=f.get("MemorySize"),
                                               has_dlq=bool(f.get("DeadLetterConfig", {}).get("TargetArn"))))
    for page in dynamodb_client.get_paginator("list_tables").paginate():
        topo.tables += [TableNode(name=t) for t in page["TableNames"] if not t.startswith(INTERNAL_PREFIX)]
    for url in sqs_client.list_queues().get("QueueUrls", []):
        qname = url.rsplit("/", 1)[-1]
        attrs = sqs_client.get_queue_attributes(QueueUrl=url, AttributeNames=["RedrivePolicy"]).get("Attributes", {})
        topo.queues.append(QueueNode(name=qname, has_dlq="RedrivePolicy" in attrs))
    for fn in topo.functions:
        for page in lambda_client.get_paginator("list_event_source_mappings").paginate(FunctionName=fn.name):
            for m in page["EventSourceMappings"]:
                src = m["EventSourceArn"].rsplit(":", 1)[-1] if ":table/" not in m["EventSourceArn"] else m["EventSourceArn"].split("/")[1]
                topo.edges.append(Edge(src=src, dst=fn.name, kind="triggers"))
    return topo


def apply_annotations(topo: Topology, annotations: dict) -> Topology:
    """annotations = {"edges": [{"src","dst","kind"}], "weaknesses": [...], "notes": {"resource": "text"}}"""
    for e in annotations.get("edges", []):
        edge = Edge(**e)
        if edge not in topo.edges:
            topo.edges.append(edge)
    topo.weaknesses += [w for w in annotations.get("weaknesses", []) if w not in topo.weaknesses]
    for res, text in annotations.get("notes", {}).items():
        for group in (topo.functions, topo.tables, topo.queues):
            for n in group:
                if n.name == res:
                    n.notes = text
    return topo
