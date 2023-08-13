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
    function: str
    config: Dict


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


Blocks = make_dataclass(
    "Blocks",
    [(config.__name__, Optional[config]) for config in get_blocks().values()],
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


def open_config(path: Path, structured: OmegaConf = None,) -> PipelineConfig:
    """Open a config file and return a DictConfig object"""

    time_ = datetime.now().strftime("%m.%d.%Y_%H.%M.%S")

    s = OmegaConf.structured(PipelineConfig) if structured is None else structured
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

    print("Config file saved at: ", merged.Metadata.output)

    return merged


def open_completed_config(path: Path, validate: bool = True) -> PipelineConfig:
    """Open a config file and return a DictConfig object"""

    with open(path, "r") as f:
        c = OmegaConf.load(
            f,
        )
    if not validate:
        return c

    s = OmegaConf.structured(PipelineConfig)
    return OmegaConf.merge(s, c)


def load_function(function: str) -> Callable:
    """Load a function from a string"""
    # allow already loaded functions
    if callable(function):
        return function
    # load the function
    if function.startswith("external"):
        return importlib.import_module(
            ".".join((function.lstrip("external.")).split(".")[:-1])
        ).__dict__[function.split(".")[-1]]
    
    return importlib.import_module(
        f"sumo_pipelines.blocks.{function.split('.')[0]}.functions"
    ).__dict__[function.split(".")[-1]]


def recursive_producer(producers: List[Tuple[str, List[str]]]) -> None:
    producers = [(load_function(function), dotpath) for function, dotpath in producers]
    i = 0

    def _recursive_producer(
        main_config, producers: List[Tuple[Callable, List[str]]] = producers
    ):
        nonlocal i
        for f in producers[0][0](
            OmegaConf.select(main_config, producers[0][1]), main_config, producers[0][1]
        ):
            if len(producers) > 1:
                yield from _recursive_producer(f, producers[1:])
            else:
                if f.Metadata.get("run_id", None) is None:
                    f.Metadata.run_id = f"{i}"
                    # make the output directory
                    Path(f.Metadata.cwd).mkdir(parents=True, exist_ok=True)
                    i += 1
                yield f

    return _recursive_producer


def create_consumers(
    function_n_configs: List[Tuple[str, List[str]]], parallel: bool = False
) -> Union[Callable, object]:
    """Load a consumer from a config file"""
    func = [
        (load_function(function), dotpath) for function, dotpath in function_n_configs
    ]

    def consumer(main_config):
        for f, dotpath in func:
            f(OmegaConf.select(main_config, dotpath), main_config)

    return ray.remote(consumer) if parallel else consumer
