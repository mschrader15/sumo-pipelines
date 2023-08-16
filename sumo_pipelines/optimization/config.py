from typing import Any, Dict, TYPE_CHECKING, List
from dataclasses import dataclass, field
import importlib

from sumo_pipelines.config import MetaData, Pipeline, Blocks

# import ray.tune only for type checking
if TYPE_CHECKING:
    from ray.tune.search import sample as tune_sample


@dataclass
class SearchSpaceConstructor:
    type: str
    args: List[Any] = field(default_factory=list)

    def __post_init__(self, ) -> None:
        self.type = self.type.lower()
        # check that ray tune has the type in its registry
        try:
            # try to get the method from tune sample
            self.ss_ray = getattr(tune_sample, self.type)(*self.args)
        except AttributeError:
            raise ValueError(f"Search space type {self.type} not found in ray tune.")
        
    def __getstate__(self):
        state = self.__dict__.copy()
        del state['ss_ray']
        return state
    
@dataclass
class SearchSpaceParameter:
    val: Any = field(default=None)
    ss: SearchSpaceConstructor = field(default=None)


@dataclass
class SearchSpaceConfig:
    parameters: Dict[str, SearchSpaceParameter] = field(default_factory=dict)

    def __getattr__(self, name):
        if name in self.parameters:
            return self.parameters[name]
        else:
            raise AttributeError(f"No such attribute: {name}")
        
    @property
    def search_space(self):
        return {k: v.ss.ss_ray for k, v in self.parameters.items() if v.ss is not None}
    
    def update_parameters(self, new_parameters: Dict[str, Any]):
        for k, v in new_parameters.items():
            if k not in self.parameters:
                raise ValueError(f"Parameter {k} not found in search space.")
            self.parameters[k].val = v


@dataclass
class TunerConfig:
    name: str
    gen_function: str = field(default=None)
    gen_function_kwargs: Dict[str, Any] = field(default_factory=dict)
    tuner_kwargs: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self, ) -> None:
        # try to import the create function
        try:
            self._create_function = getattr(importlib.import_module(".".join(self.gen_function.split(".")[:-1])), self.gen_function.split(".")[-1])
        except AttributeError:
            raise ValueError(f"Create function {self.create_function} not found in module {self.create_function.split('.')[0]}.")

    def create(self, **kwargs):
        return self._create_function(**self.gen_function_kwargs, **kwargs)

    # make the class pickleable (aka drop the create function)
    def __getstate__(self):
        state = self.__dict__.copy()
        del state['_create_function']
        return state


@dataclass
class ObjectiveConfig:
    function_path: str

    def __post_init__(self, ) -> None:
        # try to import the create function so that error gets thrown early
        try:
            self.function
        except AttributeError:
            raise ValueError(f"Objective function {self.function} not found: {self.function_path}")
    
    @property
    def function(self):
        return getattr(importlib.import_module(".".join(self.function_path.split(".")[:-1])), self.function_path.split(".")[-1])

    def __call__(self, *args, **kwargs):
        return self._function(*args, **kwargs)
    

@dataclass
class ObjectiveWrapperConfig:
    function_path: str
    config: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self, ) -> None:
        # try to import the create function so that error gets thrown early
        try:
            self.function
        except AttributeError:
            raise ValueError(f"Objective function {self.function} not found: {self.function_path}")
        
    @property
    def function(self):
        return getattr(importlib.import_module(".".join(self.function_path.split(".")[:-1])), self.function_path.split(".")[-1])
    
    def __call__(self, *args, **kwargs):
        return self._function(*args, **kwargs)


@dataclass
class CalibrationConfig:
    SearchSpace: SearchSpaceConfig
    Tuner: TunerConfig
    Objective: ObjectiveConfig
    ObjectiveWrapper: ObjectiveWrapperConfig = field(default=None)


@dataclass
class OptimizationConfig:
    MetaData: MetaData
    Pipeline: Pipeline
    Blocks: Blocks
    Optimization: CalibrationConfig

