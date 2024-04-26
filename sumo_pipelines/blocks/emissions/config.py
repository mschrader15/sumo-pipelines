from dataclasses import dataclass, field
from typing import Any

from omegaconf import MISSING


@dataclass
class FuelTotalConfig:
    emissions_xml: str
    sim_step: float
    total_energy: float = MISSING
    total_vehicles: int = MISSING
    delete_xml: bool = field(default=True)
    output_time_filter_lower: Any = field(default=None)
    output_time_filter_upper: Any = field(default=None)
    polygon_file: Any = field(default=None)


@dataclass
class EmissionsTableFuelTotalConfig:
    input_file: str
    save_file_path: Any = field(default=None)
    sim_step: Any = field(default="${Blocks.SimulationConfig.step_length}")
    total_fuel: float = MISSING
    total_distance: float = MISSING
    num_vehicles: int = MISSING
    total_timeloss: float = MISSING
    average_delay: float = MISSING
    average_fc: float = MISSING
    average_fc_normed: float = MISSING
    average_fc_l: float = MISSING
    time_low_filter: float = field(default=0)
    time_high_filter: float = field(default=1e9)
    filter_polygon: Any = field(default=None)
    average_speed: Any = field(default=None)
    average_travel_time: Any = field(default=None)


@dataclass
class TripInfoTotalFuelConfig:
    input_file: str
    val: float
    time_low_filter: float = field(default=0)
    time_high_filter: float = field(default=1e9)
