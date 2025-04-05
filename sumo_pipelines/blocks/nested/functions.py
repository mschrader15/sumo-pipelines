from omegaconf import OmegaConf
from ray.util.queue import Queue

from sumo_pipelines.blocks.nested.config import NestedQueueFunction
from sumo_pipelines.config import PipelineConfig
from sumo_pipelines.utils.config_helpers import config_wrapper


@config_wrapper
def nested_queue_consumer(
    config: NestedQueueFunction,
    pipeline_config: PipelineConfig,
    queue: Queue,
    *args,
    **kwargs,
) -> None:
    # this function takes the queue and updates the results
    while True:
        item = queue.get()
        if (item[0] == "stop") or (item is None):
            break

        # update the pipeline config with the dotpaths in the config
        for dp, val in item[1:]:
            pipeline_config = OmegaConf.update(pipeline_config, dp, val)

        for function in config.functions:
            function(pipeline_config, *args, **kwargs)

        # # execute the function
        # config.function(item, pipeline_config, *args, **kwargs)
        # queue.put(item)
