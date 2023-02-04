import importlib
from pathlib import Path
from typing import Any, Callable, Generator, List, Tuple, Union

from omegaconf import DictConfig
import click

from pipelines.config import PipelineConfig, open_config


try:
    import ray

    ray_exists = True
except ImportError:
    ray_exists = False


def load_function(function: str) -> Callable:
    """Load a function from a string"""
    return importlib.import_module(
        f"pipelines.blocks.{function.split('.')[0]}.functions"
    ).__dict__[function.split(".")[-1]]


def create_producer(function: str) -> Generator[PipelineConfig, None, None]:
    """Load a generator from a config file"""
    func = load_function(function)

    def producer(config, main_config):
        i = 0
        for f in func(config, main_config):
            if f.Metadata.get("run_id", None) is None:
                f.Metadata.run_id = f"{i}"
                # make the output directory
                Path(f.Metadata.cwd).mkdir(parents=True, exist_ok=True)
                i += 1
            yield f

    return producer


def create_consumers(
    function_n_configs: List[Tuple[str, List[str]]], parallel: bool = False
) -> Union[Callable, object]:
    """Load a consumer from a config file"""
    func = [
        (load_function(function), dotpath) for function, dotpath in function_n_configs
    ]

    def consumer(main_config):
        for f, dotpath in func:
            f(recursive_get(main_config, dotpath), main_config)

    return ray.remote(consumer) if parallel else consumer


def recursive_get(d: DictConfig, keys: List[str]) -> Any:
    """Get a value from a nested dict"""
    return d[keys[0]] if len(keys) == 1 else recursive_get(d[keys[0]], keys[1:])


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
        assert (
            len(pipeline.producers) == 1
        ), "Only one producer per block is allowed (until I figure out how to chain them)"

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
                        ["Pipeline", "pipeline", k, "consumers", i, "config"],
                    )
                    for i, consumer in enumerate(pipeline.consumers)
                ],
                parallel=True,
            )
            procs = []
            for producer in pipeline.producers:
                procs.extend(
                    consumer.remote(f)
                    for f in create_producer(producer.function)(producer.config, c)
                )
            ray.get(procs)

        else:
            consumer = create_consumers(
                [
                    (
                        consumer.function,
                        ["Pipeline", "pipeline", k, "consumers", i, "config"],
                    )
                    for i, consumer in enumerate(pipeline.consumers)
                ],
                parallel=False,
            )
            for producer in pipeline.producers:
                for f in create_producer(producer.function)(producer.config, c):
                    consumer(f)


if __name__ == "__main__":
    main()