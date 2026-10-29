from typing import Protocol

from ..experiment.steady_state import SteadyState
from ..history import History
from ..scenario import Scenario
from ..topology import Topology


class GenerationError(Exception):
    pass


class Generator(Protocol):
    name: str

    def generate(self, topo: Topology, steady: SteadyState, history: History) -> Scenario: ...
