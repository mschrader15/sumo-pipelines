import functools
import socket
import subprocess
from contextlib import closing, redirect_stdout

import sumolib
from omegaconf import DictConfig

from sumo_pipelines.utils.config_helpers import config_wrapper

from .config import SimulationConfig


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


@config_wrapper
def run_sumo(
    config: SimulationConfig, parent_config: DictConfig, *args, **kwargs
) -> None:
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


@config_wrapper
def run_sumo_fast_fcd(
    config: SimulationConfig, parent_config: DictConfig, *args, **kwargs
) -> None:
    """
    This is a standalone function that runs sumo and returns nothing.

    It is multi-process safe. You must make sure that files do not conflict if multi-processing.

    Args:
        config (SimulationConfig): The configuration for the simulation.
    """
    from sumo_pipelines.cpp import traci_vehicle_state_runner

    # find the following commands in the command line list
    # "--fcd-output" and the output file

    sumo_cmd = config.make_cmd(config)

    ind = sumo_cmd.index("--fcd-output") if "--fcd-output" in sumo_cmd else -1
    if ind == -1:
        raise ValueError("No fcd-output flag found in sumo command")

    # get the output file
    sumo_cmd.pop(ind)
    output_file = sumo_cmd.pop(ind)

    # check if collisions are desired
    ind = (
        sumo_cmd.index("--fcd-output.collisions")
        if "--fcd-output.collisions" in sumo_cmd
        else -1
    )
    collisions = False
    if ind != -1:
        sumo_cmd.pop(ind)
        collisions = True

    # check if leader is desired
    ind = (
        sumo_cmd.index("--fcd-output.leader")
        if "--fcd-output.leader" in sumo_cmd
        else -1
    )
    leader = False
    if ind != -1:
        sumo_cmd.pop(ind)
        leader = True

    if not (output_file.endswith(".parquet") or output_file.endswith(".prq")):
        raise ValueError("Output file must be a parquet file")

    func = functools.partial(
        traci_vehicle_state_runner,
        sumo_cmd,
        config.warmup_time,
        output_file,
        include_collision=collisions,
        include_leader=leader,
    )

    if config.simulation_output:
        with open(config.simulation_output, "w") as f:
            f.write(" ".join(sumo_cmd))
            with redirect_stdout(f):
                func()
    else:
        func()


@config_wrapper
def run_sumo_socket_listeners(
    config: SimulationConfig, parent_config: DictConfig, *args, **kwargs
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


@config_wrapper
def run_sumo_function(
    config: SimulationConfig, parent_config: DictConfig, *args, **kwargs
) -> None:
    # call the runner function
    config.runner_function(parent_config)
