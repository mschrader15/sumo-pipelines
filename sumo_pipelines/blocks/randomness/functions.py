import random

import numpy as np

from sumo_pipelines.utils.config_helpers import config_wrapper

from .config import RandomSeedConfig


@config_wrapper
def set_random_seed(
    config: RandomSeedConfig,
    _global_config: dict,
    random_state: np.random.RandomState = None,
    *args,
    **kwargs,
) -> None:
    if random_state is not None:
        config.seed = random_state.randint(*config.range)
    else:
        config.seed = random.randint(*config.range)
