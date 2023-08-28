from pathlib import Path
from dataclasses import MISSING, dataclass, field
from typing import Any, List

from omegaconf import DictConfig, ListConfig


@dataclass
class ReadConfig:
    """
    This class is custom to our use case. It is used to sample parameters from a table
    """

    regex: str
    root_dir: str


@dataclass
class SeedConfig:
    """
    This class is custom to our use case. It is used to sample parameters from a table
    """
    num_samples: int
    seed: int = 42
    range: List = field(default_factory=list)



@dataclass
class IteratorConfig:
    name: str
    val: Any = MISSING
    choices: List = field(default_factory=list)


