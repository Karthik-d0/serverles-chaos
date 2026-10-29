"""Persistent experiment history: the memory of the closed loop."""
import json
import os
import time
from collections import Counter
from typing import Any, Dict, List, Optional, Set, Tuple

from pydantic import BaseModel, Field

from .scenario import Scenario

Sig = Tuple[str, str, str]


class Record(BaseModel):
    id: int
    ts: float = Field(default_factory=time.time)
    generator: str = ""
    scenario: Scenario
    outcome: Optional[Dict[str, Any]] = None


class History:
    def __init__(self, path: Optional[str] = None, universe: Optional[Set[Sig]] = None):
        self.path = path
        self.universe: Set[Sig] = set(universe or [])
        self.records: List[Record] = []
        if path and os.path.exists(path):
            with open(path) as f:
                self.records = [Record.model_validate_json(l) for l in f if l.strip()]

    # ---- writing ----
    def add(self, scenario: Scenario, generator: str = "", outcome: Optional[dict] = None) -> Record:
        rec = Record(id=len(self.records) + 1, generator=generator, scenario=scenario, outcome=outcome)
        self.records.append(rec)
        self._save()
        return rec

    def set_outcome(self, record_id: int, outcome: dict) -> None:
        next(r for r in self.records if r.id == record_id).outcome = outcome
        self._save()

    def annotate(self, record_id: int, **fields) -> None:
        rec = next(r for r in self.records if r.id == record_id)
        rec.outcome = {**(rec.outcome or {}), **fields}
        self._save()

    def _save(self) -> None:
        if not self.path:
            return
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        with open(self.path, "w") as f:
            f.writelines(r.model_dump_json() + "\n" for r in self.records)

    # ---- analysis ----
    VALID = ("no_impact", "violated")

    def valid_records(self) -> List[Record]:
        """Experiments that actually tested the fault. Aborted (unhealthy baseline) or unfinished ones do not count."""
        return [r for r in self.records if (r.outcome or {}).get("impact") in self.VALID]

    def aborted(self) -> List[Record]:
        return [r for r in self.records if (r.outcome or {}).get("impact") == "aborted"]

    def tried(self) -> Counter:
        return Counter(s for r in self.valid_records() for s in r.scenario.signatures)

    def untried(self) -> List[Sig]:
        return sorted(self.universe - set(self.tried()))

    def coverage(self) -> float:
        return len(self.universe & set(self.tried())) / len(self.universe) if self.universe else 0.0

    def violating(self) -> List[Record]:
        return [r for r in self.records if (r.outcome or {}).get("impact") == "violated"]

    def violating_signatures(self) -> Set[Sig]:
        """Signatures of single-fault scenarios that violated the steady state (unambiguous attribution)."""
        return {r.scenario.signatures[0] for r in self.violating() if len(r.scenario.faults) == 1}

    # ---- prompt context ----
    def summarize(self, last_k: int = 10, max_untried: int = 20) -> str:
        if not self.records:
            return "No experiments have been run yet."
        out = [f"{len(self.valid_records())} valid experiment(s) so far ({len(self.aborted())} aborted before testing and ignored). Most recent:"]
        for r in self.records[-last_k:]:
            o = r.outcome or {}
            res = {"violated": "steady state VIOLATED", "no_impact": "no impact", "aborted": "ABORTED: system was unhealthy before injection, fault NOT tested"}.get(o.get("impact"), "outcome unknown")
            d = o.get("during") or {}
            if d.get("n"):
                res += f" (success {d['success_rate']:.0%}, p95 {d['p95_ms']:.0f}ms)"
            if o.get("never_healed"):
                res += " [afterwards the system did NOT heal on its own]"
            elif o.get("slow_to_heal_s"):
                res += f" [afterwards the system needed {o['slow_to_heal_s']:.0f}s to heal]"
            out.append(f"- #{r.id}: {r.scenario.short()} => {res}")
        if self.universe:
            out.append(f"Coverage: {self.coverage():.0%} of (fault, target, variant) combinations tried.")
            un = self.untried()
            if un:
                out.append("NOT YET TRIED: " + ", ".join(f"{t}@{g}({v})" for t, g, v in un[:max_untried])
                           + (f" (+{len(un) - max_untried} more)" if len(un) > max_untried else ""))
        return "\n".join(out)
