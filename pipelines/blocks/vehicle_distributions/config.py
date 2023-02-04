from typing import List
from pathlib import Path
from dataclasses import dataclass, field

import pandas as pd
import numpy as np
from omegaconf import DictConfig


@dataclass
class CFTableConfig(DictConfig):
    """
    This class is custom to our use case. It is used to sample parameters from a table
    """

    parameters: List[str]
    table: str
    save_path: str 
    seed: int = field(default=42)
    cf_model: str = field(default="idm")
    cf_params: dict = field(default_factory=lambda: {})
    vehicle_distribution_name: str = field(default="vehDist")
    sample_mode: str = field(default="row-wise")
    num_samples: int = field(default=1000)
