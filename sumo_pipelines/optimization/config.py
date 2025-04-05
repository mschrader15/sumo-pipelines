from dataclasses import MISSING, dataclass, field
from typing import Any, Dict, List

from omegaconf import DictConfig

from sumo_pipelines.config import MetaData, Pipeline


@dataclass
class SearchSpaceConstructor:
    function: Any
    args: List[Any] = field(default_factory=list)


@dataclass
class SearchSpaceParameter:
    ss: SearchSpaceConstructor
    val: Any = MISSING


@dataclass
class SearchSpaceConfig:
    variables: Dict[str, SearchSpaceParameter] = field(default_factory=dict)
    build_function: Any = "${import:optimization.utils.build_search_space}"
    update_function: Any = "${import:optimization.utils.update_search_space}"

    @property
    def search_space(self):
        return {k: v.ss.ss_ray for k, v in self.variables.items() if v.ss is not None}


@dataclass
class TunerConfig:
    name: str
    gen_function: Any
    gen_function_kwargs: Dict[str, Any] = field(default_factory=dict)
    tuner_kwargs: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ObjectiveConfig:
    function: Any
    n_iterations: int = 1
    return_intermediate: bool = field(default=False)
    report_config: bool = field(default=False)
    fail_safe: Any = field(default=False)
    config: Dict[str, Any] = field(default_factory=dict)
    additional_returns: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ObjectiveWrapperConfig:
    function: Any
    config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OutputConfig:
    save_path: str
    save_results_table: bool = field(default=True)
    save_best_config: bool = field(default=True)


@dataclass
class CalibrationConfig:
    SearchSpace: SearchSpaceConfig
    Tuner: TunerConfig
    ObjectiveFn: ObjectiveConfig
    ObjectiveWrapper: ObjectiveWrapperConfig
    Output: OutputConfig


@dataclass
class OptimizationConfig:
    Metadata: MetaData
    Pipeline: Pipeline
    Blocks: DictConfig
    Optimization: CalibrationConfig
