from typing import Any
from dataclasses import dataclass

from omegaconf import MISSING


@dataclass
class PostProcessingGEHConfig:
    rw_count_file: str
    sim_count_file: str
    agg_interval: int
    output_file: Any = None
    time_low_filter: int = 0
    time_high_filter: int = 3600
    geh_threshold: float = 5
    val: Any = MISSING
    geh_ok: float = MISSING


