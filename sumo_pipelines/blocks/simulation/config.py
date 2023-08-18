from pathlib import Path
from typing import Any, List
from dataclasses import dataclass, field

import sumolib


@dataclass
class SimulationConfig:
    start_time: int
    end_time: int
    net_file: str
    gui: bool = field(default=False)
    route_files: List[str] = field(default_factory=list)
    additional_files: List[str] = field(default_factory=list)
    step_length: float = field(default=0.1)
    seed: int = field(default=42)
    additional_sim_params: List[str] = field(default_factory=list)
    simulation_output: str = ""
    warmup_time: int = field(default=1800)
    make_cmd: Any = "${import:simulation.make_cmd}"



