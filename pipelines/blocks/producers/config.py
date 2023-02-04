from pathlib import Path
from dataclasses import dataclass
from typing import Any

from omegaconf import DictConfig


@dataclass
class ReadConfig(DictConfig):
    """
    This class is custom to our use case. It is used to sample parameters from a table
    """

    regex: str
    root_dir: str


@dataclass
class SeedConfig(DictConfig):
    """
    This class is custom to our use case. It is used to sample parameters from a table
    """

    num_samples: int
    range: tuple
    copy_func: Any
    seed: int = 42
