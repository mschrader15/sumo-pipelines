from typing import TYPE_CHECKING
from typing import Any, Dict

if TYPE_CHECKING:
    from sumo_pipelines.optimization.config import SearchSpaceConfig
else:
    SearchSpaceConfig = None

def build_search_space(config: SearchSpaceConfig, *args, **kwargs) -> dict:
    """
    Builds a search space from a config

    Args:
        config (SearchSpaceConfig): The config to build the search space from
        
    Returns:
        dict: A dictionary of search space variables in the Ray Tune format
    """
    return {
        k: v.ss.function(*v.ss.args, )
        for k, v in config.variables.items()
    }


def update_search_space(config: SearchSpaceConfig, new_parameters: Dict[str, Any]):
    for k, v in new_parameters.items():
        if k not in config.variables:
            raise ValueError(f"Parameter {k} not found in search space.")
        config.variables[k].val = v