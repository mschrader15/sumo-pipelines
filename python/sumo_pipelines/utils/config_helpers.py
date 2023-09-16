from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import List, Union

from omegaconf import OmegaConf, SCMode


from sumo_pipelines.config import PipelineConfig
from sumo_pipelines.optimization.config import OptimizationConfig
from sumo_pipelines.pipe_handlers import load_function


def create_custom_resolvers():
    
    def update_parent_from_yaml(p, *, __parent__):
        c = OmegaConf.load(p)
        __parent__.update(c)
        
    
    try:
        OmegaConf.register_new_resolver(
            "import", lambda x: load_function(x), use_cache=True
        )
        OmegaConf.register_new_resolver(
            "datetime.now",
            lambda x: datetime.now().strftime(x or "%m.%d.%Y_%H.%M.%S"),
            use_cache=True,
        )
        OmegaConf.register_new_resolver(
            "datetime.parse", lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S%z")
        )
        OmegaConf.register_new_resolver(
            "yaml.update", update_parent_from_yaml, use_cache=True
        )
    except Exception as e:
        if "already registered" in str(e):
            pass
        else:
            raise e


def to_yaml(
    path: Path, config: Union[PipelineConfig, OptimizationConfig], resolve: bool
) -> None:
    """Save a config file"""
    write_config = OmegaConf.to_container(deepcopy(config), resolve=resolve)
    with open(path, "w") as f:
        # check if the dataclass field are None
        keys = list(write_config["Blocks"].keys())
        for k in keys:
            if write_config["Blocks"].get(k, None) is None:
                del write_config["Blocks"][k]

        f.write(OmegaConf.to_yaml(OmegaConf.create(write_config), resolve=resolve))


def open_config(
    path: Path,
    structured: OmegaConf = None,
) -> Union[PipelineConfig, OptimizationConfig]:
    """Open a config file and return a DictConfig object"""
    create_custom_resolvers()

    # time_ = datetime.now().strftime("%m.%d.%Y_%H.%M.%S")

    s = OmegaConf.structured(PipelineConfig) if structured is None else structured
    c = OmegaConf.load(
        path,
    )

    merged = OmegaConf.merge(s, c)

    # handle the output directory
    # merged.Metadata.output = str(Path(merged.Metadata.output).joinpath(time_))
    # create the output directory
    Path(merged.Metadata.output).mkdir(parents=True, exist_ok=True)
    # save the config file at the top level. Don't resolve the config file
    to_yaml(Path(merged.Metadata.output).joinpath("config.yaml"), merged, False)

    print("Config file saved at: ", merged.Metadata.output)

    return merged


def open_completed_config(
    path: Path, validate: bool = True
) -> Union[PipelineConfig, OptimizationConfig]:
    """Open a config file and return a DictConfig object"""
    try:
        create_custom_resolvers()
    except Exception as e:
        pass

    with open(path, "r") as f:
        c = OmegaConf.load(
            f,
        )
    if not validate:
        return c

    s = OmegaConf.structured(PipelineConfig)
    return OmegaConf.merge(s, c)


def open_config_structured(
    path: Union[Path, List[Path]],
    resolve_output: bool = False,
) -> Union[PipelineConfig, OptimizationConfig]:
    """
    Open a config file and return the underlying dataclass representation.

    This allows for the config to contain functions
    """
    create_custom_resolvers()

    if isinstance(path, list):
        all_confs = [OmegaConf.load(p) for p in path]
        # special merge behavior for Blocks.SimulationConfig.addiational_files & Blocks.SimulationConfig.additional_sim_params
        all_additional_files = []
        additional_sim_params = []
        for conf in all_confs:
            if conf.Blocks.get("SimulationConfig", None) is not None:
                # this is some tom fuckery to get the additional files
                if conf.Blocks.SimulationConfig.get('additional_files', None) is not None:
                    all_additional_files.extend(
                        list(
                            map(
                                str,
                                conf.Blocks.SimulationConfig._get_node(
                                    "additional_files", validate_access=False
                                )._content,
                            )
                        )
                    )
                if conf.Blocks.SimulationConfig.get('additional_sim_params', None) is not None:
                    additional_sim_params.extend(
                        list(
                            map(
                                str,
                                conf.Blocks.SimulationConfig._get_node(
                                    "additional_sim_params", validate_access=False
                                )._content,
                            )
                        )
                    )
        # reduce but keep the order
        additional_sim_params = list(dict.fromkeys(additional_sim_params))
        all_additional_files = list(dict.fromkeys(all_additional_files))

        # merge the configs
        c = OmegaConf.merge(*all_confs)

        if len(all_additional_files) > 0:
            c.Blocks.SimulationConfig.additional_files = all_additional_files
        if len(additional_sim_params) > 0:
            c.Blocks.SimulationConfig.additional_sim_params = additional_sim_params
    else:
        c = OmegaConf.load(
            path,
        )

    if c.get("Optimization", None) is not None:
        s = OmegaConf.structured(OptimizationConfig)
    else:
        s = OmegaConf.structured(
            PipelineConfig,
        )
    # merge the structured config with the loaded config
    c = OmegaConf.merge(s, c)

    # if resolve metadata
    if resolve_output:
        c.Metadata.output = str(c.Metadata.output)

    return c
