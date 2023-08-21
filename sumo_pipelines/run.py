from pathlib import Path

from .utils.config_helpers import open_config
from .pipe_handlers import create_consumers, recursive_producer

try:
    import ray
    # from ray.air import 
    ray_exists = True
except ImportError:
    ray_exists = False

def run_pipeline(config: Path, debug: bool) -> None:
    """Run the pipeline"""
    #
    c = open_config(
        config,
    )

    # c.Pipeline.build_pipeline()

    # create the consumer functions
    for k, pipeline in enumerate(c.Pipeline.pipeline):
        if pipeline.parallel:
            # initialize ray
            if not ray_exists:
                raise ImportError(
                    "Ray is not installed, but is required for parallel processing"
                )
            if ray.is_initialized():
                ray.init(address="auto")
            else:
                ray.init(local_mode=debug)

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
