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

    sumo = sumolib.checkBinary("sumo-gui" if config.gui else "sumo")

    sumo_cmd = [
        sumo,
        "-n",
        config.net_file,
        "-r",
        ",".join(config["route_files"]),
        "-a",
        ",".join(config["additional_files"]),
        "--begin",
        str(config.start_time),
        "--end",
        str(config.end_time),
        "--step-length",
        str(config.step_length),
        *config.additional_sim_params,
    ]

    subprocess.run(sumo_cmd, check=True)


