import os
from pathlib import Path
import sys
from omegaconf import DictConfig
from .config import RouteSamplerConfig, RandomTripsConfig


def call_route_sampler(
    route_sampler_config: RouteSamplerConfig,
    config: DictConfig,
) -> None:
    
    try:
        sys.path.append(os.path.join(os.environ.get("SUMO_HOME"), "tools"))
    except TypeError:
        raise TypeError("SUMO_HOME is not set")

    try:
        import routeSampler
    except ImportError:
        raise ImportError("$SUMO_HOME must be in your path")


    routeSampler.main(
        routeSampler.get_options(
            [
                "-r",
                str(route_sampler_config.random_route_file),
                "-t",
                str(route_sampler_config.turn_file),
                "-o",
                str(route_sampler_config.output_file),
                "--seed",
                str(int(route_sampler_config.seed)),
                *route_sampler_config.additional_args,
            ]
        )
    )



def call_random_trips(
    config: RandomTripsConfig,
    *args,
    **kwargs,
) -> Path:
    
    try:
        sys.path.append(os.path.join(os.environ.get("SUMO_HOME"), "tools"))
    except TypeError:
        raise TypeError("SUMO_HOME is not set")

    try:
        import randomTrips
    except ImportError:
        raise ImportError("$SUMO_HOME must be in your path")

    net_file = config.net_file
    output_file = Path(config.output_file)
    seed = int(config.seed)

    randomTrips.main(
        randomTrips.get_options(
            [
                "-n",
                str(net_file),
                "-r",
                str(output_file),
                "-o",
                str(output_file.parent / "routes.add.xml"),
                "--validate",
                "--seed",
                str(seed),
                *config.additional_args
            ]
        )
    )

    return output_file