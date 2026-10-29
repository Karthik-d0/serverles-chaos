"""Closed-loop, history-conditioned scenario generator with grounding and validation.

Pipeline per scenario (design inspired by ChaosEater's schema-constrained agents with validate-and-retry):
  1. prompt = topology + steady state + fault catalog + HISTORY of past experiments and untried combinations
  2. parse JSON -> pydantic Scenario (schema + safety caps)
  3. grounding: every target must exist in the topology (rejects hallucinated resources)
  4. novelty (closed loop only): scenario must contain at least one untried (fault, target, variant)
  On any failure the error is fed back to the model and it retries (max_attempts).
"""
import json
import re
from typing import Dict, List

from pydantic import ValidationError

from ..experiment.steady_state import SteadyState
from ..faults import FAULT_CATALOG
from ..history import History
from ..scenario import Scenario
from ..topology import Topology
from .base import GenerationError

SYSTEM = f"""You are a chaos-engineering assistant for event-driven serverless applications (AWS Lambda, SQS, DynamoDB).
Design ONE fault-injection scenario that is most likely to reveal a real resilience weakness.
Rules:
- First imagine a realistic event (traffic spike, partial regional degradation, poison/duplicate messages, noisy dependency).
- Pick ONE fault by default. Use two only if they are on DIFFERENT parts of the event flow and you can say why the combination matters. Never more than two. Use ONLY the fault types below.
- Prefer PARTIAL faults: probability 0.2-0.6, bounded delays (500-4000 ms). Avoid ServiceOutage, probability 1.0, and LambdaError 'exception' on a synchronous hard dependency unless nothing else is untried. A total outage of something the system must have is a guaranteed failure, not a finding.
- Aim for faults that the system's retries, DLQs or idempotency should be able to absorb. Escalate severity only after a milder variant of the same fault was tolerated.
- Use ONLY resource names that appear in the application description. Never invent resources.
- Target weak points of the event flow (missing DLQ, no retry, no idempotency, synchronous dependencies).
- If an experiment history is given: do NOT repeat a (fault, target, variant) already tried; prefer NOT YET TRIED combinations. If a fault violated, try a MILDER variant of it (lower probability or delay) to find the tolerance threshold.
- Keep magnitudes realistic and bounded.
Fault types:
{FAULT_CATALOG}
Answer with ONLY a JSON object: {{"event": str, "rationale": str, "faults": [{{"type": ..., ...}}]}}"""


def _extract_json(text: str) -> dict:
    text = re.sub(r"^```(?:json)?|```$", "", text.strip(), flags=re.M).strip()
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end < 0:
        raise ValueError("no JSON object found in the answer")
    return json.loads(text[start:end + 1])


class LLMGenerator:
    def __init__(self, llm, closed_loop: bool = True, max_attempts: int = 3):
        self.llm, self.closed_loop, self.max_attempts = llm, closed_loop, max_attempts
        self.name = "llm-closed" if closed_loop else "llm-open"
        self.last_attempts = 0

    # ---- prompt ----
    def _user(self, topo: Topology, steady: SteadyState, history: History) -> str:
        parts = [topo.describe(),
                 f"Steady state to protect: {steady.description} (success rate >= {steady.success_rate_min:.0%}, p95 <= {steady.p95_ms_max:.0f} ms)."]
        if self.closed_loop:
            parts.append("Experiment history:\n" + history.summarize())
        parts.append("Design the next scenario.")
        return "\n\n".join(parts)

    # ---- checks ----
    @staticmethod
    def _ground(sc: Scenario, topo: Topology) -> List[str]:
        errs = []
        for f in sc.faults:
            ok = (topo.has("function", f.target) if f.type in ("LambdaLatency", "LambdaError") else
                  topo.has("table", f.target) if f.type == "DynamoDBFault" else
                  topo.has("queue", f.target) if f.type == "QueueFault" else f.target in topo.services())
            if not ok:
                errs.append(f"{f.type} targets '{f.target}', which does not exist in the application. "
                            f"Existing: functions={topo.names('function')}, tables={topo.names('table')}, queues={topo.names('queue')}.")
        return errs

    def _novelty(self, sc: Scenario, history: History) -> List[str]:
        untried = history.untried()
        if not self.closed_loop or not untried or any(s in set(untried) for s in sc.signatures):
            return []
        return ["Every fault in this scenario was already tried. Use at least one NOT YET TRIED combination, e.g. "
                + ", ".join(f"{t}@{g}({v})" for t, g, v in untried[:8]) + "."]

    # ---- main ----
    def generate(self, topo: Topology, steady: SteadyState, history: History) -> Scenario:
        msgs: List[Dict[str, str]] = [{"role": "system", "content": SYSTEM},
                                      {"role": "user", "content": self._user(topo, steady, history)}]
        last = "no attempt made"
        for attempt in range(1, self.max_attempts + 1):
            self.last_attempts = attempt
            answer = self.llm.chat(msgs)
            try:
                sc = Scenario.model_validate(_extract_json(answer))
                problems = self._ground(sc, topo) + self._novelty(sc, history)
            except (ValueError, ValidationError) as e:
                problems = [f"Invalid output: {e}"]
            if not problems:
                return sc
            last = "; ".join(problems)
            msgs += [{"role": "assistant", "content": answer},
                     {"role": "user", "content": "Your scenario was rejected:\n- " + "\n- ".join(problems) +
                      "\nFix these problems without repeating them. Answer with ONLY the JSON object."}]
        raise GenerationError(f"no valid scenario after {self.max_attempts} attempts; last problems: {last}")
