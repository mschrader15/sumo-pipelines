from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class TurnFileConfig:
    to_from_file: Path
    start_time: str
    agg_interval: float
    output_file: Path
    time_column: str = "Timestamp"
    sql_filter: str = ""
    end_time: Any = None
    seed: int = 42


@dataclass
class RouteSamplerConfig:
    turn_file: str
    output_file: str
    random_route_file: str
    additional_args: list
    seed: Any = field(default=42)


@dataclass
class RandomTripsConfig:
    net_file: Path
    output_file: Path
    seed: Any
    additional_args: list
