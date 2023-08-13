from pathlib import Path
from typing import List
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

    def make_cmd(
        self,
    ):
        sumo = sumolib.checkBinary("sumo-gui" if self.gui else "sumo")

        return list(
            map(
                str,
                [
                    sumo,
                    "-n",
                    self.net_file,
                    "-r",
                    ",".join(self.route_files),
                    "-a",
                    ",".join(self.additional_files),
                    "--begin",
                    str(self.start_time),
                    "--end",
                    str(self.end_time),
                    "--step-length",
                    str(self.step_length),
                    *self.additional_sim_params,
                ],
            )
        )
