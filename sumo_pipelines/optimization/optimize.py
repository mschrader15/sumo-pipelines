import os
import random
from pathlib import Path

import numpy as np
import ray

# from functions.config import Root, parse_config_object
# from functions.sumo import Runner, WandbRunner
from ray import tune

from sumo_pipelines.optimization.config import OptimizationConfig
from sumo_pipelines.optimization.core import (
    handle_results,
    target_wrapper,
    with_parameter_wrapper,
)
from sumo_pipelines.utils.config_helpers import (
    create_custom_resolvers,
    open_config_structured,
)

try:
    from ray.air.integrations.wandb import WandbLoggerCallback
except ImportError:

    def WandbLoggerCallback(*args, **kwargs):
        pass


os.environ["PYTHONWARNINGS"] = "ignore::DeprecationWarning"

create_custom_resolvers()


def seed_everything(config: OptimizationConfig):
    np.random.seed(config.Metadata.random_seed)
    random.seed(config.Metadata.random_seed)


def initalize_ray(smoke_test: bool):
    # override the resources
    # https://docs.ray.io/en/latest/tune/api_docs/tune.html#tune.run
    # try:
    ray.init(num_cpus=1 if smoke_test else None, local_mode=smoke_test)


def run_optimization_core(config_obj: OptimizationConfig, smoke_test: bool):
    # check that GUI is off if we aren't in smoke test mode
    if not smoke_test and config_obj.Blocks.SimulationConfig.gui:
        print("Turning off GUI for calibration.")
        config_obj.Blocks.SimulationConfig.gui = False

    def trial_name_creator(trial):
        return f"{trial.trial_id}"

    def trial_dirname_creator(trial):
        return trial_name_creator(trial)

    # set the seed
    seed_everything(config_obj)

    # initalize ray
    # check first if it is not already initalized
    if not ray.is_initialized():
        initalize_ray(smoke_test)

    # build the runner
    runner = with_parameter_wrapper(
        trainable=target_wrapper(config_obj),
        config=config_obj,
    )

    # run the optimization
    tuner: tune.Tuner = config_obj.Optimization.Tuner.gen_function(
        **config_obj.Optimization.Tuner.gen_function_kwargs,
        trial_name_creator=trial_name_creator,
        trial_dirname_creator=trial_dirname_creator,
        # trial_dirname_creator=lambda trial: f"{trial.trainable_name}_{trial.trial_id}",
    )(
        trainable=runner,
        param_space=config_obj.Optimization.SearchSpace.build_function(
            config_obj.Optimization.SearchSpace
        ),
        **config_obj.Optimization.Tuner.tuner_kwargs,
    )

    analysis: tune.ResultGrid = tuner.fit()

    handle_results(analysis, config_obj)


def run_optimization(config_path: Path, smoke_test: bool, gui: bool = False):
    # parse the config
    config_obj = open_config_structured(config_path)

    if gui:
        try:
            config_obj.Blocks.SimulationConfig.gui = True
        except AttributeError:
            print("GUI is not supported for this config.")

    # run the optimization
    run_optimization_core(config_obj, smoke_test)
