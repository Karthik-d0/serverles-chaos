"""Event-driven application topology: resources plus the *event flow* between them."""
import json
from typing import List, Literal, Optional

from pydantic import BaseModel, Field

EdgeKind = Literal["triggers", "invokes", "writes", "reads", "sends"]


class FunctionNode(BaseModel):
    name: str
    timeout_s: Optional[int] = None
    memory_mb: Optional[int] = None
    has_dlq: Optional[bool] = None
    notes: str = ""


class TableNode(BaseModel):
    name: str
    notes: str = ""


class QueueNode(BaseModel):
    name: str
    has_dlq: Optional[bool] = None
    notes: str = ""


class Edge(BaseModel):
    src: str
    dst: str
    kind: EdgeKind  # e.g. queue --triggers--> function, function --writes--> table, function --sends--> queue


class Topology(BaseModel):
    name: str = "app"
    functions: List[FunctionNode] = Field(default_factory=list)
    tables: List[TableNode] = Field(default_factory=list)
    queues: List[QueueNode] = Field(default_factory=list)
    edges: List[Edge] = Field(default_factory=list)
    weaknesses: List[str] = Field(default_factory=list, description="Known resilience gaps (no DLQ, no retries...). Optional hints for the generator.")

    # ---- lookups ----
    def names(self, kind: str) -> List[str]:
        return [n.name for n in {"function": self.functions, "table": self.tables, "queue": self.queues}[kind]]

    def has(self, kind: str, name: str) -> bool:
        return name in self.names(kind)

    def services(self) -> List[str]:
        out = []
        if self.tables: out.append("dynamodb")
        if self.queues: out.append("sqs")
        if self.functions: out.append("lambda")
        return out

    # ---- io ----
    def save(self, path: str) -> None:
        with open(path, "w") as f:
            f.write(self.model_dump_json(indent=2))

    @classmethod
    def load(cls, path: str) -> "Topology":
        with open(path) as f:
            return cls.model_validate(json.load(f))

    # ---- prompt text ----
    def describe(self) -> str:
        lines = [f"Application: {self.name}", "Event-driven serverless system on AWS (LocalStack)."]
        for f in self.functions:
            extra = ", ".join(x for x in [f"timeout {f.timeout_s}s" if f.timeout_s else "", f"{f.memory_mb}MB" if f.memory_mb else "",
                                          "DLQ" if f.has_dlq else ("no DLQ" if f.has_dlq is False else "")] if x)
            lines.append(f"- Lambda `{f.name}`" + (f" ({extra})" if extra else "") + (f": {f.notes}" if f.notes else ""))
        for t in self.tables:
            lines.append(f"- DynamoDB table `{t.name}`" + (f": {t.notes}" if t.notes else ""))
        for q in self.queues:
            lines.append(f"- SQS queue `{q.name}`" + (", no DLQ" if q.has_dlq is False else "") + (f": {q.notes}" if q.notes else ""))
        if self.edges:
            lines.append("Event flow:")
            lines += [f"  {e.src} --{e.kind}--> {e.dst}" for e in self.edges]
        if self.weaknesses:
            lines.append("Known weaknesses: " + "; ".join(self.weaknesses))
        return "\n".join(lines)
