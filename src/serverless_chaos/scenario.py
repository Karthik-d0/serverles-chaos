from typing import List

from pydantic import BaseModel, Field

from .faults import Fault


class Scenario(BaseModel):
    """One experiment: a set of faults injected simultaneously for one window."""
    event: str = Field(description="The real-world situation being simulated (traffic spike, regional degradation...).")
    rationale: str = Field("", description="Why these faults expose a weakness.")
    faults: List[Fault] = Field(min_length=1, max_length=2)

    @property
    def signatures(self) -> list:
        return [f.signature for f in self.faults]

    @property
    def window_s(self) -> int:
        return max(f.duration_s for f in self.faults)

    def short(self) -> str:
        return " + ".join(f.short() for f in self.faults)
