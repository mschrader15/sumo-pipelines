from pathlib import Path
from omegaconf import OmegaConf
from copy import deepcopy

from .config import SaveConfig, RemoveFileConfig


def save_config(config: SaveConfig, parent_config: OmegaConf) -> None:
    """
    This function saves the config file to a json file.

    Args:
        config (SaveConfig): The config file to save.
    """
    from sumo_pipelines.config import MetaData
    
    local_config = deepcopy(parent_config)
    Path(config.save_path).parent.mkdir(parents=True, exist_ok=True)
    with open(config.save_path, "w") as f:
        # resolve only the metadata
        local_config.Metadata = MetaData(
            **OmegaConf.to_container(local_config.Metadata, resolve=True)
        )
        d = OmegaConf.to_container(local_config, resolve=False)
        # pop blocks that we don't want to save
        keys = list(d['Blocks'].keys())
        for b in keys:
            if isinstance(d['Blocks'][b], str) or (d['Blocks'][b] is None):
                d['Blocks'].pop(b)
        
        
        f.write(OmegaConf.to_yaml(OmegaConf.create(d), resolve=False))


def mv_file(config: SaveConfig, parent_config: OmegaConf) -> None:
    """
    This function moves a list of target files to their proper destination

    Args:
        config (SaveConfig): The config file to save.
    """
    for f in config.mv_files:
        Path(f.target).parent.mkdir(parents=True, exist_ok=True)
        Path(f.source).rename(f.target)

def rm_file(config: RemoveFileConfig, parent_config: OmegaConf) -> None:
    """
    This function removes a list of target files

    Args:
        config (RemoveFileConfig): The config file to save.
    """
    import os
    for f in config.rm_files:
        if os.path.isfile(f):
            os.remove(f)
            print(f"Removed file: {f}")
        else:
            print(f"No such file: {f}")