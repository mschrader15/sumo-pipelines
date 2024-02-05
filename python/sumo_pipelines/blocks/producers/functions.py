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

# from config import PipelineConfig
from .config import ReadConfig, SeedConfig, IteratorConfig, SobolSequenceConfig


def read_configs(
    config: ReadConfig,
    parent_config: DictConfig,
    *args,
    **kwargs,
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
    config: SeedConfig,
    parent_config: DictConfig,
    *args,
    **kwargs,
) -> Generator[DictConfig, None, None]:
    # this function returns the a sequence of random seeds, to be consumed by the caller.
    # this is a simple example, but it could be used to generate a sequence of random seeds
    random.seed(parent_config.Metadata.random_seed)
    for _ in range(config.num_samples):
        randint = random.randint(*config.range)
        new_conf = deepcopy(parent_config)
        new_conf.Blocks.SeedConfig.seed = randint
        yield new_conf


def generate_iterator(
    config: IteratorConfig,
    parent_config: DictConfig,
    dotpath: str,
    **kwargs,
) -> Generator[DictConfig, None, None]:
    # this function returns the a sequence of random seeds, to be consumed by the caller.
    # this is a simple example, but it could be used to generate a sequence of random seeds
    for choice in config.choices:
        new_conf = deepcopy(parent_config)
        OmegaConf.update(new_conf, f"{dotpath}.val", choice)
        yield new_conf


def generate_sobol_sequence(
    config: SobolSequenceConfig,
    parent_config: DictConfig,
    dotpath: str,
    *args,
    **kwargs,
) -> Generator[DictConfig, None, None]:
    try:
        from SALib.sample import sobol
    except ImportError:
        raise ImportError("Must have SALib installed to use this function")

    try:
        import polars as pl
    except ImportError:
        raise ImportError("Must have polars installed to use this function")

    sobol_dict = SobolSequenceConfig.build_sobol_dict(config)

    problem = sobol.sample(
        sobol_dict,
        N=config.N,
        calc_second_order=config.calc_second_order,
        seed=parent_config.Metadata.random_seed,
    )

    # generate new configs to interate
    for row in range(problem.shape[0]):
        new_conf = deepcopy(parent_config)
        for col in range(problem.shape[1]):    
            OmegaConf.update(
                new_conf,
                f"{dotpath}.params.{sobol_dict['names'][col]}.val",
                float(problem[row, col]),
            )
        yield new_conf

    if not config.save_path.parent.exists():
        config.save_path.parent.mkdir(parents=True)
    
    (pl.DataFrame(problem, schema=sobol_dict["names"]).write_parquet(config.save_path))