from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, List

from omegaconf import DictConfig, ListConfig


@dataclass
class SaveConfig:
    """
    This class is used to save the configuration of the simulation
    """

    save_path: str


@dataclass
class _MvFilePair:
    """
    This is a helper class for MvFileConfig
    """

    source: str = field(default="")
    target: str = field(default="")


@dataclass
class MvFileConfig:
    """
    This class is used to copy files from one location to another
    """

    mv_files: List[_MvFilePair]


@dataclass
class RemoveFileConfig:
    """
    This class is used to remove files
    """

    rm_files: List[str]

