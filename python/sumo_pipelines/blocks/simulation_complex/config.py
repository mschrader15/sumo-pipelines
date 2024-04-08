from dataclasses import dataclass
from sumo_pipelines.blocks.simulation.config import SimulationConfig



@dataclass
class ComplexSimulationConfig(SimulationConfig):
    vehicle_state_output: str = "${Metadata.cwd}/traj.out.parquet"
