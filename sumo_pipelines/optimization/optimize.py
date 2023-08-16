import os
import click
from pathlib import Path

import ray

# from functions.config import Root, parse_config_object
# from functions.sumo import Runner, WandbRunner

from ray import tune, air
from ray.tune.search import SearchAlgorithm
from ray.tune.schedulers import AsyncHyperBandScheduler, HyperBandScheduler

import numpy as np
import random


from .config import OptimizationConfig


try:
    from ray.air.integrations.wandb import WandbLoggerCallback
except ImportError:

    def WandbLoggerCallback(*args, **kwargs):
        pass


os.environ["PYTHONWARNINGS"] = "ignore::DeprecationWarning"


def seed_everything(config: OptimizationConfig):
    np.random.seed(config.MetaData.random_seed)
    random.seed(config.MetaData.random_seed)

def initalize_ray(smoke_test: bool):
    # override the resources
    # https://docs.ray.io/en/latest/tune/api_docs/tune.html#tune.run
    # try:
    ray.init(num_cpus=1 if smoke_test else None, local_mode=smoke_test)


def run_optimization(config_obj: OptimizationConfig, smoke_test: bool):
    # check that GUI is off if we aren't in smoke test mode
    if not smoke_test and config_obj.Blocks.SimulationConfig.gui:
        print("Turning off GUI for calibration.")
        config_obj.Blocks.SimulationConfig.gui = False

    # set the seed
    seed_everything(config_obj)

    # build the runner
    runner = config_obj.Optimization.Objective.function

    # check if there is wrapping to do
    if config_obj.Optimization.ObjectiveWrapper is not None:
        runner = config_obj.Optimization.ObjectiveWrapper.function(
            runner,
            config_obj
        )
    
    # initalize ray
    # check first if it is not already initalized
    if not ray.is_initialized():
        initalize_ray(smoke_test)

    # run the optimization
    tuner = config_obj.Optimization.Tuner.gen_function()(
        **config_obj.Optimization.Tuner.tuner_kwargs,
    )
    analysis = tuner.fit()

    print("Best config: ", analysis.get_best_result().config)
    print("Path: ", analysis.get_best_result().path)

    analysis.get_dataframe().to_csv(Path(config_obj.Config.output_path) / "results.csv")