from copy import deepcopy
from dataclasses import dataclass, field, MISSING, make_dataclass
from datetime import datetime
import importlib
import inspect
from pathlib import Path
from typing import Any, Callable, Dict, Generator, List, Tuple, Union, Optional

from omegaconf import DictConfig, OmegaConf

from sumo_pipelines.blocks import *


try:
    import ray

    ray_exists = True
except ImportError:
    ray_exists = False


@dataclass
class PipePiece:
    function: Any  # this is really a callable
    config: Dict


@dataclass
class PipeBlock:
    block: str
    producers: List[PipePiece] = field(default_factory=list)
    consumers: List[PipePiece] = field(default_factory=list)
    parallel: bool = field(default=False)
    number_of_workers: Union[str, int] = field(default="auto")


@dataclass
class Pipeline:
    pipeline: List[PipeBlock] = field(default=MISSING)
    executor: str = field(default="ray")
    parallel_proc: Union[str, int] = field(default="auto")


@dataclass
class MetaData:
    name: str
    author: str
    output: str
    run_id: Any
    cwd: str
    simulation_root: str
    random_seed: int = field(default=42)

    def __str__(self):
        return f"{self.name} by {self.author} (run_id: {self.run_id})"

    def __repr__(self):
        return self.__str__()


def get_blocks():
    blocks = {}
    for block_dir in (Path(__file__).parent / "blocks").iterdir():
        if block_dir.is_dir() and not block_dir.name.startswith("_"):
            search_path = f"sumo_pipelines.blocks.{block_dir.name}.config"
            for _, c in inspect.getmembers(
                importlib.import_module(search_path), inspect.isclass
            ):
                # if hasattr(config, "__name__"):
                # asserts that the class is in the same module as the predicate
                if c.__module__ == search_path:
                    blocks[c.__name__] = c
    return blocks


# doing funny stuff for the blocks
@dataclass
class Blocks:
    pass

Blocks = make_dataclass(
    "Blocks",
    [(config.__name__, Optional[config], None) for config in get_blocks().values()],
)


@dataclass
class PipelineConfig:
    """This is the config file for the pipeline"""

    Metadata: MetaData
    Blocks: Blocks
    Pipeline: Pipeline

