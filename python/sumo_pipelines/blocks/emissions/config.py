from dataclasses import dataclass, field
from typing import Any

from omegaconf import MISSING


@dataclass
class FuelTotalConfig:
    emissions_xml: str
    output_path: str
    sim_step: float
    delete_xml: bool = field(default=True)
    vehicle_average: bool = field(default=False)
    output_time_filter_lower: Any = field(default=None)
    output_time_filter_upper: Any = field(default=None)
    gasoline_filter: Any = field(default=None)
    diesel_filter: Any = field(default=None)
    x_filter: Any = field(default=None)
    y_filter: Any = field(default=None)


@dataclass
class EmissionsTableFuelTotalConfig:
    input_file: str
    sim_step: Any = field(default="${Blocks.SimulationConfig.step_length}")
    total_fuel: float = MISSING
    total_distance: float = MISSING
    num_vehicles: int = MISSING
    time_low_filter: float = field(default=0)
    time_high_filter: float = field(default=1e9)
    

@dataclass
class TripInfoTotalFuelConfig:
    input_file: str
    val: float
    time_low_filter: float = field(default=0)
    time_high_filter: float = field(default=1e9)
    