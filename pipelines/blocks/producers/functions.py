"""
This module contains the functions that are used by the generators.

They should be pure functions, and should not have any side effects.

They should return a generator, to be consumed by the caller.

The generators should return a top-level config file, which is then passed to the next block.
"""
from copy import deepcopy
from pathlib import Path
import random
from typing import Generator
from omegaconf import OmegaConf, DictConfig

from config import PipelineConfig
from .config import ReadConfig, SeedConfig


def read_configs(
    config: ReadConfig, parent_config: DictConfig
) -> Generator[DictConfig, None, None]:
    """
    This function reads config files using regex.

    It returns a generator, to be consumed by the caller.

    A similar approach could be used to read a large number of files on the cloud.

    Args:
        config (SaveConfig): The config file to read.

    Returns:
        DictConfig: The config file.
    """
    config_files = list(Path(config.root_dir).glob(config.regex))
    config_files = sorted(config_files)
    for config_file in config_files:
        with open(config_file, "r") as f:
            yield OmegaConf.load(f)


def generate_random_seed(
    config: SeedConfig, parent_config: DictConfig
) -> Generator[DictConfig, None, None]:
    # this function returns the a sequence of random seeds, to be consumed by the caller.
    # this is a simple example, but it could be used to generate a sequence of random seeds
    random.seed(parent_config.Metadata.random_seed)
    for _ in range(config.num_samples):
        randint = random.randint(*config.range)
        new_conf = deepcopy(parent_config)
        new_conf.Blocks.SeedConfig.seed = randint
        yield new_conf
