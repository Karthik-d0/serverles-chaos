"""Non-LLM generators: baselines for the evaluation, and an offline fallback."""
import random

from ..experiment.steady_state import SteadyState
from ..history import History
from ..scenario import Scenario
from ..topology import Topology
from .candidates import candidate_faults


class RandomGenerator:
    """Uniform random fault, with replacement, ignoring history. The 'no intelligence' baseline."""
    name = "random"

    def __init__(self, seed: int = 0, duration_s: int = 30):
        self.rng, self.duration_s = random.Random(seed), duration_s

    def generate(self, topo: Topology, steady: SteadyState, history: History) -> Scenario:
        f = self.rng.choice(candidate_faults(topo, self.duration_s))
        return Scenario(event="Random fault", rationale="uniform random baseline", faults=[f])


class CoverageGuidedGenerator:
    """Closed loop without an LLM: always pick an untried (fault, target, variant); prefer neighbours of past violations."""
    name = "coverage"

    def __init__(self, seed: int = 0, duration_s: int = 30):
        self.rng, self.duration_s = random.Random(seed), duration_s

    def generate(self, topo: Topology, steady: SteadyState, history: History) -> Scenario:
        cands = candidate_faults(topo, self.duration_s)
        tried = history.tried()
        bad_targets = {s[1] for s in history.violating_signatures()}

        def score(f) -> float:
            s = 0.0 if f.signature in tried else 10.0
            s += 2.0 if f.target in bad_targets and f.signature not in tried else 0.0   # explore around known weak spots
            s -= tried.get(f.signature, 0)                                               # least-tried when all explored
            return s + self.rng.random()                                                 # random tie-break

        f = max(cands, key=score)
        why = "untried combination" if f.signature not in tried else "least-tried combination"
        if f.target in bad_targets:
            why += "; target previously violated the steady state"
        return Scenario(event="Coverage-guided probe", rationale=why, faults=[f])
