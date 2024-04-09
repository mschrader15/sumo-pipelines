from pathlib import Path
from typing import Any, List
from dataclasses import dataclass, field

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


@dataclass
class USDOTCalibrationConfig:
    sim_file: Path
    calibrate_file: Path
    start_time: str
    sim_file_functions: Any = (None,)
    join_on_cols: List[str] = field(
        default_factory=lambda: list(
            [
                "tl",
                "detector_id",
            ]
        )
    )
    agg_interval: int = 900
    target_col: str = "volume"
    calibration_passed: bool = MISSING
    warmup: Any = "${Blocks.SimulationConfig.warmup_time}"
    sql_expression: str = ""
    output_file: str = ""
