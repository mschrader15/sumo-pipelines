import subprocess
import sumolib

from omegaconf import DictConfig

from .config import SimulationConfig

import socket
from contextlib import closing


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


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
                *(
                    (
                        "-a",
                        ",".join(list(map(str, config.additional_files))),
                    )
                    if config.additional_files
                    else []
                ),
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

    sumo_cmd = config.make_cmd(config)

    if config.simulation_output:
        with open(config.simulation_output, "w") as f:
            s = subprocess.run(sumo_cmd, check=True, stdout=f, stderr=f)
    else:
        s = subprocess.run(
            sumo_cmd,
        )

    if s.returncode != 0:
        raise RuntimeError("Sumo failed to run")


def run_sumo_socket_listeners(
    config: SimulationConfig, parent_config: DictConfig
) -> None:
    """
    This is a standalone function that runs sumo and returns nothing.

    It is multi-process safe. You must make sure that files do not conflict if multi-processing.

    Args:
        config (SimulationConfig): The configuration for the simulation.
    """
    assert (
        len(config.socket_listeners) == 1
    ), "Only one socket listener is supported at this time"

    config.socket_listeners[0].config.port = find_free_port()

    sumo_cmd = config.make_cmd(config)

    if config.simulation_output:
        with open(config.simulation_output, "w") as f:
            s = subprocess.Popen(sumo_cmd, stdout=f, stderr=f)
    else:
        s = subprocess.Popen(
            sumo_cmd,
        )

    # test a socket listener
    config.socket_listeners[0].function(
        *config.socket_listeners[0].config.args,
    )

    # cleanup the process
    s.kill()
    r = s.wait()
    print(r)


def run_sumo_function(
    config: SimulationConfig,
    parent_config: DictConfig,
) -> None:
    # call the runner function
    config.runner_function(parent_config)
