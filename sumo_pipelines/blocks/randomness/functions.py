import random

from .config import RandomSeedConfig


def set_random_seed(config: RandomSeedConfig, *args, **kwargs) -> None:
    config.seed = random.randint(*config.range)
