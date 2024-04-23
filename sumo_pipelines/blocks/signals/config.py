from dataclasses import dataclass, field
from typing import List, Union


@dataclass
class PhaseUpdateConfig:
    phase: int
    key: str
    val: Union[int, float, str]


@dataclass
class SplitDict:
    phase: int
    split: float


@dataclass
class NEMAConfig:
    in_file: str
    out_file: str
    id: str
    program_id: str
    offset: Union[int, None] = None
    splits: List[SplitDict] = field(default_factory=list)
    phase_updates: List[PhaseUpdateConfig] = field(default_factory=list)


@dataclass
class NEMAUpdateConfig:
    tls: List[NEMAConfig]
