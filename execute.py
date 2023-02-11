import importlib
from pathlib import Path
from typing import Any, Callable, Generator, List, Tuple, Union

from omegaconf import OmegaConf
import click

from sumo_pipelines.config import PipelineConfig, open_config, recursive_producer, create_consumers


try:
    import ray
    ray_exists = True
except ImportError:
    ray_exists = False


@click.command()
@click.argument("config", type=click.Path(exists=True, resolve_path=True))
@click.option("--debug", is_flag=True, default=False)
def main(config: Path, debug: bool) -> None:
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


if __name__ == "__main__":
    main()
