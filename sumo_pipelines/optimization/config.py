from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class SearchSpace:
    param1: float
    param2: float
    param3: float

@dataclass
class SearchAlgorithm:
    name: str
    parameters: Dict[str, Any]
    import_path: str

@dataclass
class ObjectiveFunction:
    path: str


@dataclass
class CalibrationConfig:
    search_space: SearchSpace
    search_algorithm: SearchAlgorithm
