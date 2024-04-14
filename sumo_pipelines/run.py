import contextlib
from copy import deepcopy
from pathlib import Path
from typing import List, Union

from sumo_pipelines.config import PipeBlock, PipelineConfig
from sumo_pipelines.pipe_handlers import create_consumers, recursive_producer
from sumo_pipelines.pipe_handlers.create import load_function, persistent_producer

try:
    import ray
    from ray.util.queue import Queue

    # from ray.air import
    ray_exists = True

except ImportError:
    ray_exists = False


def _config_handler(
    config: Union[Path, List[Path], PipelineConfig],
    gui: bool,
    replay: bool,
    skip_pipe_blocks: List[str],
) -> PipelineConfig:
    from sumo_pipelines.utils.config_helpers import open_config_structured

    c = (
        open_config_structured(
            config,
            resolve_output=True,
        )
        if isinstance(config, (Path, str, list))
        else config
    )

    with contextlib.suppress(AttributeError):
        c.Blocks.SimulationConfig.gui = gui

    if replay:
        return replay_pipeline(c, skip_pipe_blocks)

    return c


def _launch_ray(c: PipelineConfig, debug: bool):
    if any(p.parallel or p.queue_based for p in c.Pipeline.pipeline):
        if not ray_exists:
            raise ImportError(
                "Ray is not installed, but is required for parallel processing"
            )

        if ray.is_initialized():
            ray.init(
                address="auto",
            )
        else:
            ray.init(
                local_mode=debug,
            )


def replay_pipeline(
    config: PipelineConfig,
    skip_pipe_blocks: List[str],
) -> PipelineConfig:
    if not Path(config.Metadata.cwd).exists():
        Path(config.Metadata.cwd).mkdir(parents=True, exist_ok=True)

    for k, pipeline in enumerate(config.Pipeline.pipeline):
        if pipeline.block in skip_pipe_blocks:
            continue

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

        consumer(config)


def run_pipeline(
    config: Union[Path, List[Path], PipelineConfig],
    debug: bool,
    gui: bool,
    replay: bool,
    skip_pipe_blocks: List[str],
) -> PipelineConfig:
    """Run the pipeline"""
    c = _config_handler(config, gui, replay, skip_pipe_blocks)

    if replay:
        return

    _launch_ray(c, debug)

    # create the consumer functions
    for k, pipeline in enumerate(c.Pipeline.pipeline):
        if pipeline.queue_based:
            run_queue_pipeline(c, pipeline, k, debug=debug)
            continue

        if pipeline.parallel:
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
            res = ray.get(procs)

            if pipeline.result_handler is not None:
                # run the result handler
                load_function(pipeline.result_handler.function)(
                    pipeline.result_handler.config, c, res
                )

            # pl.DataFrame

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


def run_queue_pipeline(
    config: PipelineConfig,
    pipeline: List[PipeBlock],
    pipe_index: int,
    debug: bool,
) -> None:
    # # create the consumer functions
    # for k, pipeline in enumerate(c.Pipeline.pipeline):
    # make the queue the size of the number of possible parallel processes
    pipeline.number_of_workers - 1
    # check if ray is in local mode
    # if debug:
    # use a threading queue
    # queue = DebugQueue(maxsize=q_size, )
    # else:

    queue = Queue(maxsize=-1, actor_options={"num_cpus": 1, "num_gpus": 0})

    # create the producer functions
    producer = persistent_producer(
        [
            (
                producer.function,
                f"Pipeline.pipeline[{pipe_index}].producers[{i}].config",
            )
            for i, producer in enumerate(pipeline.producers)
        ]
    )

    prod_ref = producer.remote(config, queue)
    ray.get(prod_ref)

    consumer = create_consumers(
        [
            (
                consumer.function,
                f"Pipeline.pipeline[{pipe_index}].consumers[{i}].config",
            )
            for i, consumer in enumerate(pipeline.consumers)
        ],
        parallel=True,
    )

    procs = [
        consumer.remote(deepcopy(config), queue)
        # consumer(deepcopy(config), queue)
        for _ in range(pipeline.number_of_workers - 1)
    ]

    ray.get([*procs])

    print("Queue is empty")

    queue.shutdown()
