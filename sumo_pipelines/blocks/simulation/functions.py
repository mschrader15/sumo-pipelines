import os
import subprocess
import sumolib

from omegaconf import DictConfig

from .config import SimulationConfig


def run_sumo(config: SimulationConfig, parent_config: DictConfig) -> None:
    """
    This is a standalone function that runs sumo and returns nothing.

    It is multi-process safe. You must make sure that files do not conflict if multi-processing.

    Args:
        config (SimulationConfig): The configuration for the simulation.
    """

    sumo_cmd = config.make_cmd()

    if config.simulation_output:
        with open(config.simulation_output, "w") as f:
            s = subprocess.run(sumo_cmd, check=True, stdout=f, stderr=f)
    else:
        s = subprocess.run(sumo_cmd, )
    
    if s.returncode != 0:
        raise RuntimeError("Sumo failed to run")



def online_traci(
        config: SimulationConfig,
        parent_config: DictConfig,
) -> None:
    
    # TODO: Implement this function
    step_function = parent_config.step_function