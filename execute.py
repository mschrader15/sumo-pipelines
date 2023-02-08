import importlib
from pathlib import Path
from typing import Any, Callable, Generator, List, Tuple, Union

from omegaconf import OmegaConf
import click

from sumo_pipelines.config import PipelineConfig, open_config


try:
    import ray

    ray_exists = True
except ImportError:
    ray_exists = False


def load_function(function: str) -> Callable:
    """Load a function from a string"""
    return importlib.import_module(
        f"sumo_pipelines.blocks.{function.split('.')[0]}.functions"
    ).__dict__[function.split(".")[-1]]


def recursive_producer(producers: List[Tuple[str, List[str]]]) -> None:
    producers = [(load_function(function), dotpath) for function, dotpath in producers]
    i = 0

    def _recursive_producer(main_config, producers: List[Tuple[Callable, List[str]]] = producers):
        nonlocal i
        for f in producers[0][0](OmegaConf.select(main_config, producers[0][1]), main_config):
            if len(producers) > 1:
                yield from _recursive_producer(f, producers[1:])
            else:
                if f.Metadata.get("run_id", None) is None:
                    f.Metadata.run_id = f"{i}"
                    # make the output directory
                    Path(f.Metadata.cwd).mkdir(parents=True, exist_ok=True)
                    i += 1
                yield f
        
    return _recursive_producer



def create_consumers(
    function_n_configs: List[Tuple[str, List[str]]], parallel: bool = False
) -> Union[Callable, object]:
    """Load a consumer from a config file"""
    func = [
        (load_function(function), dotpath) for function, dotpath in function_n_configs
    ]

    def consumer(main_config):
        for f, dotpath in func:
            f(OmegaConf.select(main_config, dotpath), main_config)

    return ray.remote(consumer) if parallel else consumer



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
