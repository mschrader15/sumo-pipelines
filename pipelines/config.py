from copy import deepcopy
from dataclasses import dataclass, field, MISSING, make_dataclass
from datetime import datetime
import importlib
import inspect
from pathlib import Path
from typing import Any, Callable, Generator, List, Tuple, Union, Optional

from omegaconf import DictConfig, OmegaConf

from pipelines.blocks import *


try:
    import ray

    ray_exists = True
except ImportError:
    ray_exists = False


@dataclass
class PipePiece:
    function: str
    type: str
    config: dict


@dataclass
class PipeBlock:
    block: str
    producers: List[PipePiece]
    consumers: List[PipePiece]
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


# @dataclass
# class Blocks:
#     """This just stored the blocks for the pipeline, so that they can be imported in the pipeline config file"""

#     # TODO: this should be createed dynamically with dataclasses.make_dataclass

#     SeedConfig: Optional[SeedConfig]
#     ReadConfig: Optional[ReadConfig]
#     CFTableConfig: Optional[CFTableConfig]
#     SimulationConfig: Optional[SimulationConfig]
#     SaveConfig: Optional[SaveConfig]
#     MvFileConfig: Optional[MvFileConfig]


def get_blocks():
    
    blocks = {}
    for block_dir in (Path(__file__).parent / "blocks").iterdir():
        if block_dir.is_dir() and not block_dir.name.startswith("_"):
            search_path = f"pipelines.blocks.{block_dir.name}.config"
            for _, c in inspect.getmembers(importlib.import_module(search_path), inspect.isclass):
                # if hasattr(config, "__name__"):
                # asserts that the class is in the same module as the predicate
                if c.__module__ == search_path:
                    blocks[c.__name__] = c
    return blocks




Blocks = make_dataclass(
    "Blocks",
    [
        (config.__name__, Optional[config])
        for config in get_blocks().values()
    ],
)

@dataclass
class PipelineConfig:
    """This is the config file for the pipeline"""

    Metadata: MetaData
    Blocks: Blocks
    Pipeline: Pipeline


def to_yaml(path: Path, config: DictConfig, resolve):
    """Save a config file"""
    with open(path, "w") as f:
        f.write(OmegaConf.to_yaml(config, resolve=resolve))


def open_config(path: Path) -> PipelineConfig:
    """Open a config file and return a DictConfig object"""

    time_ = datetime.now().strftime("%m.%d.%Y_%H.%M.%S")

    s = OmegaConf.structured(PipelineConfig)
    c = OmegaConf.load(
        path,
    )

    merged = OmegaConf.merge(s, c)

    # handle the output directory
    merged.Metadata.output = str(Path(merged.Metadata.output).joinpath(time_))
    # create the output directory
    Path(merged.Metadata.output).mkdir(parents=True, exist_ok=True)
    # save the config file at the top level. Don't resolve the config file
    to_yaml(Path(merged.Metadata.output).joinpath("config.yaml"), merged, False)

    return merged

