import os
import sys
from omegaconf import DictConfig
from .config import RouteSamplerConfig

sys.path.append(os.path.join(os.environ.get("SUMO_HOME"), "tools"))

import routeSampler

def call_route_sampler(
    route_sampler_config: RouteSamplerConfig,
    config: DictConfig,
) -> None:

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
                str(route_sampler_config.seed),
                *route_sampler_config.additional_args,
            ]
        )
    )
