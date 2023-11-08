from pathlib import Path
from dataclasses import MISSING, dataclass, field
from typing import Any, List, Dict

from omegaconf import DictConfig, ListConfig

@dataclass
class ReadConfig:
    """
    This class is custom to our use case. It is used to sample parameters from a table
    """

    regex: str
    root_dir: str


@dataclass
class SeedConfig:
    """
    This class is custom to our use case. It is used to sample parameters from a table
    """
    num_samples: int
    seed: int = 42
    range: List = field(default_factory=list)



@dataclass
class IteratorConfig:
    name: str
    val: Any = MISSING
    choices: List = field(default_factory=list)



@dataclass
class SobolItem:
    bounds: List[float]
    val: Any = MISSING


@dataclass
class SobolSequenceConfig:
    save_path: Path
    params: Dict[str, SobolItem]
    N: int
    # sequence_type: str = "sobol"
    calc_second_order: bool = False

    @staticmethod
    def build_sobol_dict(
        cls_obj,
    ) -> dict:
        sobol_dict = {
            "num_vars": len(cls_obj.params),
            "names": [name for name in cls_obj.params.keys()],
            "bounds": [item.bounds for item in cls_obj.params.values()],
        }
        return sobol_dict