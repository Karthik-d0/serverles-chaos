from typing import List, Tuple

from pydantic import BaseModel, Field


class SteadyState(BaseModel):
    """What 'healthy' means for the workload. Success includes end-to-end event delivery for event-flow workloads."""
    description: str = "Requests succeed and finish quickly."
    success_rate_min: float = Field(0.99, ge=0, le=1)
    p95_ms_max: float = Field(1000.0, gt=0)
    min_samples: int = Field(5, ge=1, description="Fewer samples than this is inconclusive, which counts as NOT healthy.")

    def check(self, metrics: dict) -> Tuple[bool, List[str]]:
        reasons = []
        if metrics["n"] < self.min_samples:
            return False, [f"only {metrics['n']} samples collected (need >= {self.min_samples})"]
        if metrics["success_rate"] < self.success_rate_min:
            reasons.append(f"success_rate {metrics['success_rate']:.2f} < {self.success_rate_min}")
        if metrics["success_rate"] > 0 and metrics["p95_ms"] > self.p95_ms_max:   # latency of successful requests
            reasons.append(f"p95 {metrics['p95_ms']:.0f}ms > {self.p95_ms_max:.0f}ms")
        return (not reasons), reasons
