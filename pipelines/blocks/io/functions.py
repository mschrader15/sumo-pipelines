from pathlib import Path
from typing import Generator
from omegaconf import OmegaConf, DictConfig

from config import PipelineConfig
from .config import SaveConfig, MvFileConfig


def save_config(config: SaveConfig, parent_config: PipelineConfig) -> None:
    """
    This function saves the config file to a json file.

    Args:
        config (SaveConfig): The config file to save.
    """
    Path(config.save_path).parent.mkdir(parents=True, exist_ok=True)
    with open(config.save_path, "w") as f:
        f.write(OmegaConf.to_yaml(parent_config, resolve=True))


def mv_file(config: SaveConfig, parent_config: PipelineConfig) -> None:
    """
    This function moves the config file to a json file.

    Args:
        config (SaveConfig): The config file to save.
    """
    for f in config.mv_files:
        Path(f.target).parent.mkdir(parents=True, exist_ok=True)
        Path(f.source).rename(f.target)
