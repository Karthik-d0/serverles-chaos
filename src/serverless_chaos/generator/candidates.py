"""All single faults that make sense for a topology, with sensible default magnitudes."""
from typing import List, Set, Tuple

from ..faults import DynamoDBFault, Fault, LambdaError, LambdaLatency, QueueFault, ServiceOutage
from ..topology import Topology


def candidate_faults(topo: Topology, duration_s: int = 30) -> List[Fault]:
    out: List[Fault] = []
    d = dict(duration_s=duration_s)
    for fn in topo.names("function"):
        out.append(LambdaLatency(function=fn, delay_ms=2000, **d))
        out += [LambdaError(function=fn, error=e, **d) for e in ("exception", "timeout", "throttle")]
    for t in topo.names("table"):
        out.append(DynamoDBFault(table=t, effect="latency", delay_ms=1500, **d))
        out += [DynamoDBFault(table=t, effect=e, **d) for e in ("throttle", "unavailable")]
    for q in topo.names("queue"):
        out.append(QueueFault(queue=q, effect="delay", delay_ms=5000, **d))
        out += [QueueFault(queue=q, effect=e, **d) for e in ("drop", "duplicate", "unavailable")]
    out += [ServiceOutage(service=s, **d) for s in topo.services()]
    return out


def signature_universe(topo: Topology) -> Set[Tuple[str, str, str]]:
    return {f.signature for f in candidate_faults(topo)}
