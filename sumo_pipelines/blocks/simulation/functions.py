import os
import subprocess
import sumolib

from omegaconf import DictConfig

from .config import SimulationConfig


def make_cmd(
    config: SimulationConfig,
):
    sumo = sumolib.checkBinary("sumo-gui" if config.gui else "sumo")

    return list(
        map(
            str,
            [
                sumo,
                "-n",
                config.net_file,
                "-r",
                ",".join(config.route_files),
                "-a",
                ",".join(list(map(str, config.additional_files))),
                "--begin",
                str(config.start_time),
                "--end",
                str(config.end_time),
                "--step-length",
                str(config.step_length),
                *config.additional_sim_params,
            ],
        )
    )


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
        s = subprocess.run(
            sumo_cmd,
        )

    if s.returncode != 0:
        raise RuntimeError("Sumo failed to run")


def run_sumo_function(
    config: SimulationConfig,
    parent_config: DictConfig,
) -> None:
    # call the runner function
    config.runner_function(
        parent_config
    )    
