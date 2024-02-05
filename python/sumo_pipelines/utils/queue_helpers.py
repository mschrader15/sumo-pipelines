from copy import deepcopy
from typing import Generator
from sumo_pipelines.config import PipelineConfig
from ray.util.queue import Queue
from omegaconf import OmegaConf


def unpack_queue(
    q: Queue,
    config: PipelineConfig,
) -> Generator[PipelineConfig, None, None]:
    while q.actor is not None:
        item = q.get()
        if item is None:
            break

        # copy the config
        new_config = deepcopy(config)

        for dp, val in item[1:]:
            # set the value
            OmegaConf.update(new_config, dp, val)

        yield new_config

    return
