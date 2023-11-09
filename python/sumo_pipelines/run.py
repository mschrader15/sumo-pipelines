from pathlib import Path
from typing import List, Union

from sumo_pipelines.config import PipelineConfig

from .utils.config_helpers import open_config_structured
from .pipe_handlers import create_consumers, recursive_producer

try:
    import ray

    # from ray.air import
    ray_exists = True
except ImportError:
    ray_exists = False


def run_pipeline(config: Union[Path, List[Path], PipelineConfig], debug: bool) -> PipelineConfig:
    """Run the pipeline"""
    #
    c = (
        open_config_structured(
            config,
            resolve_output=True,
        )
        if isinstance(config, (Path, str, list))
        else config
    )

    # create the consumer functions
    for k, pipeline in enumerate(c.Pipeline.pipeline):
        if pipeline.parallel:
            # initialize ray
            if not ray_exists:
                raise ImportError(
                    "Ray is not installed, but is required for parallel processing"
                )
            if ray.is_initialized():
                ray.init(address="auto", )
            else:
                ray.init(local_mode=debug, num_cpus=pipeline.number_of_workers)

            consumer = create_consumers(
                [
                    (
                        consumer.function,
                        f"Pipeline.pipeline[{k}].consumers[{i}].config",
                    )
                    for i, consumer in enumerate(pipeline.consumers)
                ],
                parallel=True,
            )
            procs = [
                consumer.remote(f)
                for f in recursive_producer(
                    [
                        (
                            consumer.function,
                            f"Pipeline.pipeline[{k}].producers[{i}].config",
                        )
                        for i, consumer in enumerate(pipeline.producers)
                    ],
                )(c)
            ]
            ray.get(procs)

        else:
            consumer = create_consumers(
                [
                    (
                        consumer.function,
                        f"Pipeline.pipeline[{k}].consumers[{i}].config",
                    )
                    for i, consumer in enumerate(pipeline.consumers)
                ],
                parallel=False,
            )
            for f in recursive_producer(
                [
                    (
                        consumer.function,
                        f"Pipeline.pipeline[{k}].producers[{i}].config",
                    )
                    for i, consumer in enumerate(pipeline.producers)
                ],
            )(c):
                consumer(f)

    return c