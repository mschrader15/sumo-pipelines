import importlib
import os
from pathlib import Path
from typing import TYPE_CHECKING, Callable, List, Tuple, Union

import ray
from omegaconf import OmegaConf

if TYPE_CHECKING:
    # prevent circular imports
    from sumo_pipelines.config import PipeBlock, Pipeline
else:
    Pipeline = None
    PipeBlock = None


def load_function(function: str) -> Callable:
    """Load a function from a string"""
    # allow already loaded functions
    if callable(function):
        return function
    # load the function
    if function.startswith("external"):
        return importlib.import_module(
            ".".join((function.replace("external", "").lstrip(".")).split(".")[:-1])
        ).__dict__[function.split(".")[-1]]
    elif function.startswith("optimization"):
        return importlib.import_module(
            ".".join(("sumo_pipelines", *((function).split(".")[:-1])))
        ).__dict__[function.split(".")[-1]]
    try:
        return importlib.import_module(
            f"sumo_pipelines.blocks.{function.split('.')[0]}.functions"
        ).__dict__[function.split(".")[-1]]
    except ModuleNotFoundError:
        return importlib.import_module(
            ".".join(("sumo_pipelines", *((function).split(".")[:-1])))
        ).__dict__[function.split(".")[-1]]


def recursive_producer(producers: List[Tuple[str, List[str]]]) -> callable:
    """
    Create a recursive producer from a list of producers

    Args:
        producers (List[Tuple[str, List[str]]]): A list of producers

    Returns:
        callable: A recursive producer
    """
    producers = [(load_function(function), dotpath) for function, dotpath in producers]
    i = 0

    def _recursive_producer(
        main_config, producers: List[Tuple[Callable, List[str]]] = producers
    ):
        nonlocal i
        for f in producers[0][0](
            OmegaConf.select(main_config, producers[0][1]), main_config, producers[0][1]
        ):
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


def persistent_producer(
    producers: List[Tuple[str, List[str]]],
) -> Union[Callable, object]:
    producers = [(load_function(function), dotpath) for function, dotpath in producers]

    assert len(producers) == 1, "Only one producer is supported for persistent_producer"

    @ray.remote(
        num_cpus=1,
    )
    def _persistent_producer(
        main_config,
        *args,
        **kwargs,
    ):
        return producers[0][0](
            OmegaConf.select(main_config, producers[0][1]),
            main_config,
            producers[0][1],
            *args,
            **kwargs,
        )

    return _persistent_producer


def create_consumers(
    function_n_configs: List[Tuple[str, List[str]]],
    parallel: bool = False,
    # queue: bool = False,
) -> Union[Callable, object]:
    """
    Create a consumer from a list of functions and dotpaths

    Args:
        function_n_configs (List[Tuple[str, List[str]]]): A list of functions and dotpaths
        parallel (bool, optional): Whether to create a ray actor. Defaults to False.

    Returns:
        Union[Callable, object]: A consumer
    """
    from sumo_pipelines.utils.config_helpers import create_custom_resolvers

    func = [
        (load_function(function), dotpath) for function, dotpath in function_n_configs
    ]

    if parallel:

        @ray.remote(
            num_cpus=1,
            runtime_env={
                "env_vars": {
                    "PYTHONPATH": os.environ.get("PYTHONPATH", ""),
                }
            },
        )
        def consumer(main_config, *args, **kwargs):
            create_custom_resolvers()

            for f, dotpath in func:
                res = f(
                    OmegaConf.select(main_config, dotpath), main_config, *args, **kwargs
                )
            return res
    else:

        def consumer(main_config, *args, **kwargs):
            create_custom_resolvers()

            for f, dotpath in func:
                f(OmegaConf.select(main_config, dotpath), main_config, *args, **kwargs)

    return consumer


def get_pipeline_by_name(config: Pipeline, name: str) -> Tuple[PipeBlock, str]:
    """
    Get a pipeline by name.

    Args:
        config (Pipeline): The pipeline config
        name (str): The name of the pipeline
    """
    for _k, pipe in enumerate(config.Pipeline.pipeline):
        if pipe.block == name:
            # build the pipeline
            return pipe


def execute_pipe_block(
    block: PipeBlock,
    main_config: Pipeline,
    *args,
    **kwargs,
) -> None:
    assert (
        len(block.producers) == 0
    ), "Producers are not supported when executing a pipeline with this function"
    assert (
        block.parallel is False
    ), "Parallel pipelines are not supported when executing a pipeline with this function"

    # load the functions
    functions = [
        (block_piece.function, block_piece.config) for block_piece in block.consumers
    ]

    # execute the functions
    for f, c in functions:
        f(c, main_config, *args, **kwargs)
