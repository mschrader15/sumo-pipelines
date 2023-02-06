from pathlib import Path
from typing import List
from dataclasses import dataclass, field

from omegaconf import DictConfig


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
    simulation_output: str = ''
