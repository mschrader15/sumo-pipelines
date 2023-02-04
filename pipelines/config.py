from copy import deepcopy
from dataclasses import dataclass, field, MISSING, make_dataclass
from datetime import datetime
import importlib
from pathlib import Path
from typing import Any, Callable, Generator, List, Tuple, Union, Optional

from omegaconf import DictConfig, OmegaConf

from blocks import *


try:
    import ray

    ray_exists = True
except ImportError:
    ray_exists = False


@dataclass
class PipePiece(DictConfig):
    function: str
    type: str
    config: dict


@dataclass
class PipeBlock(DictConfig):
    block: str
    producers: List[PipePiece]
    consumers: List[PipePiece]
    parallel: bool = field(default=False)
    number_of_workers: Union[str, int] = field(default="auto")


@dataclass
class Pipeline(DictConfig):
    pipeline: List[PipeBlock] = field(default=MISSING)
    executor: str = field(default="ray")
    parallel_proc: Union[str, int] = field(default="auto")


@dataclass
class MetaData(DictConfig):
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


@dataclass
class Blocks(DictConfig):
    """This just stored the blocks for the pipeline, so that they can be imported in the pipeline config file"""

    # TODO: this should be createed dynamically with dataclasses.make_dataclass

    SeedConfig: Optional[SeedConfig]
    ReadConfig: Optional[ReadConfig]
    CFTableConfig: Optional[CFTableConfig]
    SimulationConfig: Optional[SimulationConfig]
    SaveConfig: Optional[SaveConfig]
    MvFileConfig: Optional[MvFileConfig]


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
    merged.Metadata.output = str(Path(merged.Metadata.output).joinpath(time_))

    # save the config file at the top level. Don't resolve the config file
    to_yaml(Path(merged.Metadata.output).joinpath("config.yaml"), merged, False)

    return merged


def load_function(function: str) -> Callable:
    """Load a function from a string"""
    return importlib.import_module(
        f"blocks.{function.split('.')[0]}.functions"
    ).__dict__[function.split(".")[-1]]


def create_producer(function: str) -> Generator[PipelineConfig, None, None]:
    """Load a generator from a config file"""
    func = load_function(function)

    def producer(config, main_config):
        i = 0
        for f in func(config, main_config):
            if f.Metadata.get("run_id", None) is None:
                f.Metadata.run_id = f"{i}"
                i += 1
            yield f

    return producer


def create_consumers(
    function_n_configs: List[Tuple[str, List[str]]], parallel: bool = False
) -> Union[Callable, object]:
    """Load a consumer from a config file"""
    func = [
        (load_function(function), dotpath) for function, dotpath in function_n_configs
    ]

    def consumer(main_config):
        for f, dotpath in func:
            f(recursive_get(main_config, dotpath), main_config)

    return ray.remote(consumer) if parallel else consumer


def recursive_get(d: DictConfig, keys: List[str]) -> Any:
    """Get a value from a nested dict"""
    return d[keys[0]] if len(keys) == 1 else recursive_get(d[keys[0]], keys[1:])


if __name__ == "__main__":
    c = open_config(
        "/Users/max/Development/sumo-pipelines/configurations/cf_sampling.yaml"
    )

    # c.Pipeline.build_pipeline()

    # create the consumer functions
    for k, pipeline in enumerate(c.Pipeline.pipeline):
        assert (
            len(pipeline.producers) == 1
        ), "Only one producer per block is allowed (until I figure out how to chain them)"

        if pipeline.parallel:
            # initialize ray
            if not ray_exists:
                raise ImportError(
                    "Ray is not installed, but is required for parallel processing"
                )
            ray.init()

            consumer = create_consumers(
                [
                    (
                        consumer.function,
                        ["Pipeline", "pipeline", k, "consumers", i, "config"],
                    )
                    for i, consumer in enumerate(pipeline.consumers)
                ],
                parallel=True,
            )
            procs = []
            for producer in pipeline.producers:
                l = 0
                for f in create_producer(producer.function)(producer.config, c):
                    procs.append(consumer.remote(f))
                    l += 1
                    if l > 2:
                        break
            ray.get(procs)

        else:
            consumer = create_consumers(
                [
                    (
                        consumer.function,
                        ["Pipeline", "pipeline", k, "consumers", i, "config"],
                    )
                    for i, consumer in enumerate(pipeline.consumers)
                ],
                parallel=False,
            )
            for producer in pipeline.producers:
                for f in create_producer(producer.function)(producer.config, c):
                    consumer(f)
