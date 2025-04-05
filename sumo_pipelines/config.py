from dataclasses import MISSING, dataclass, field
from typing import Any, Dict, List, Optional, Union

from sumo_pipelines.blocks import *  # noqa: F403


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
    queue_based: bool = field(default=False)
    result_handler: Optional[PipePiece] = field(default=None)


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


# extend the dataclass with


@dataclass
class PipelineConfig:
    """This is the config file for the pipeline"""

    Metadata: MetaData
    Blocks: Dict[str, dict]
    Pipeline: Pipeline
