from pathlib import Path
from typing import Generator
from omegaconf import OmegaConf, DictConfig

from .config import SaveConfig, MvFileConfig


def save_config(config: SaveConfig, parent_config: OmegaConf) -> None:
    """
    This function saves the config file to a json file.

    Args:
        config (SaveConfig): The config file to save.
    """
    from sumo_pipelines.config import MetaData

    Path(config.save_path).parent.mkdir(parents=True, exist_ok=True)
    with open(config.save_path, "w") as f:
        # resolve only the metadata
        parent_config.Metadata = MetaData(
            **OmegaConf.to_container(parent_config.Metadata, resolve=True)
        )
        f.write(OmegaConf.to_yaml(parent_config, resolve=False))


def mv_file(config: SaveConfig, parent_config: OmegaConf) -> None:
    """
    This function moves a list of target files to their proper destination

    Args:
        config (SaveConfig): The config file to save.
    """
    for f in config.mv_files:
        Path(f.target).parent.mkdir(parents=True, exist_ok=True)
        Path(f.source).rename(f.target)
