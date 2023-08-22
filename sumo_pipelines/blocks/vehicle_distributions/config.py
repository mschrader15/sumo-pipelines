from typing import Any, List, Dict
from dataclasses import dataclass, field



@dataclass
class CFTableConfig:
    """
    This class is custom to our use case. It is used to sample parameters from a table
    """
    table: str
    save_path: str
    vehicle_distribution_name: str
    seed: int = field(default=42)
    cf_params: Dict = field(default_factory=lambda: {})
    additional_params: Dict = field(default_factory=lambda: {})
    sample_mode: str = field(default="row-wise")
    num_samples: int = field(default=1000)

    # legacy
    parameters: List = field(default_factory=list)



@dataclass
class SimpleCFConfig:
    """
    This class is custom to our use case. It is used to sample parameters from a table
    """
    save_path: str
    vehicle_distribution_name: str
    cf_params: Dict[str, Any] = field(default_factory=lambda: {})



@dataclass
class ParamConfig:
    """
    This emulates on of the parameters in the SimpleCFConfig class
    """
    name: str
    distribution: str = field(default="uniform")
    params: Dict = field(default_factory=lambda: {})
    bounds: List = field(default_factory=list)



@dataclass
class SampledSimpleCFConfig(SimpleCFConfig):
    """
    This class is custom to our use case. It is used to sample parameters from a table
    """
    cf_params: Dict[str, ParamConfig] = field(default_factory=lambda: {})
    
    
# @dataclass
# class MultiLineConfig:
