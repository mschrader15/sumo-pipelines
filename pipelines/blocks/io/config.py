from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, List

from omegaconf import DictConfig, ListConfig


@dataclass
class SaveConfig(DictConfig):
    """
    This class is custom to our use case. It is used to sample parameters from a table
    """

    save_path: str


@dataclass
class _MvFilePair(DictConfig):
    """
    This class is custom to our use case. It is used to sample parameters from a table
    """

    source: str = field(default="")
    target: str = field(default="")


@dataclass
class MvFileConfig(DictConfig):
    """
    This class is custom to our use case. It is used to sample parameters from a table
    """

    mv_files: List[_MvFilePair]
