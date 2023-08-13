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


try:
    from ray.air.integrations.wandb import WandbLoggerCallback
except ImportError:

    def WandbLoggerCallback(*args, **kwargs):
        pass


ROOT = Path(__file__).parent
os.environ["ROOT"] = str(ROOT)
os.environ["PYTHONWARNINGS"] = "ignore::DeprecationWarning"


def baysian_optimization(config: Root) -> SearchAlgorithm:
    from ray.tune.search import bayesopt
    from ray.tune.search import ConcurrencyLimiter

    algo = bayesopt.BayesOptSearch(
        utility_kwargs={"kind": "ucb", "kappa": 2.5, "xi": 0.0}
    )
    algo = ConcurrencyLimiter(algo, max_concurrent=4)

    return algo


def hebo_optimizer(config: Root) -> SearchAlgorithm:
    from ray.tune.search.hebo.hebo_search import pipHEBOSearch as HEBOSearch
    from ray.tune.search import ConcurrencyLimiter

    algo = HEBOSearch(
        metric="mean_loss",
        mode="min",
        max_concurrent=4,
    )
    return ConcurrencyLimiter(algo, max_concurrent=4)


def nevergrad_optimizer(config: Root) -> SearchAlgorithm:
    from ray.tune.search.nevergrad import NevergradSearch
    import nevergrad as ng

    algo = NevergradSearch(
        optimizer=ng.optimizers.ParametrizedBO(),
        # optimizer=ng.optimizers.ParametrizedCMA()
    )
    algo = tune.search.ConcurrencyLimiter(algo, max_concurrent=8)


def seed_everything(config: Root):
    np.random.seed(config.Config.seed)
    random.seed(config.Config.seed)


def initalize_ray(smoke_test: bool):
    # override the resources
    # https://docs.ray.io/en/latest/tune/api_docs/tune.html#tune.run
    # try:
    ray.init(num_cpus=1 if smoke_test else None, local_mode=smoke_test)


def build_runner(config_obj: Root, wandb: bool):
    r = tune.with_parameters(
        WandbRunner if wandb else Runner,
        run_config=config_obj,
    )
    # set the resources
    tune.with_resources(r, {"cpu": 1, "gpu": 0})
    return r


def run_single_opt(config_obj: Root, smoke_test: bool, wandb: bool, replay: bool):
    # check that GUI is off if we aren't in smoke test mode
    if (not smoke_test and not replay) and config_obj.SUMOParameters.gui:
        print("Turning off GUI for calibration.")
        config_obj.SUMOParameters.gui = False

    # set the seed
    seed_everything(config_obj)

    # build the runner
    runner = build_runner(config_obj, wandb)

    if replay:
        return replay_f(runner, config_obj)

    # initalize ray
    # check first if it is not already initalized
    if not ray.is_initialized():
        initalize_ray(smoke_test)

    # run the optimization
    tuner = tune.Tuner(
        runner,
        tune_config=tune.TuneConfig(
            metric="score",  # "mean_loss
            mode="min",
            # default search algorithm is random search
            search_alg=nevergrad_optimizer(config_obj),
            # search_alg=hebo_optimizer(config_obj),
            scheduler=AsyncHyperBandScheduler(),
            # scheduler=HyperBandScheduler(),
            reuse_actors=True,
            num_samples=1 if smoke_test else 500,
        ),
        param_space=config_obj.CFParameters.build_tune_parameter_space(),
        **(
            dict(
                run_config=air.RunConfig(
                    callbacks=[
                        WandbLoggerCallback(
                            project="car-following-calibration",
                            group="__max__",
                            api_key=os.environ.get("WANDB_API_KEY", ""),
                        )
                    ]
                )
            )
            if wandb
            else {}
        )
    )

    analysis = tuner.fit()

    print("Best config: ", analysis.get_best_result().config)
    print("Path: ", analysis.get_best_result().path)

    analysis.get_dataframe().to_csv(Path(config_obj.Config.output_path) / "results.csv")