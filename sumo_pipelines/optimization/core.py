# function to wrap the target function for optimization
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Dict

from omegaconf import OmegaConf
from ray import train, tune

# from ray.air
from sumo_pipelines.optimization.config import OptimizationConfig
from sumo_pipelines.pipe_handlers.create import execute_pipe_block, get_pipeline_by_name
from sumo_pipelines.utils.config_helpers import create_custom_resolvers


def target_wrapper(
    config: OptimizationConfig,
) -> Callable:
    """
    This function wraps the target function for optimization,
    adding the function to pass the ray search space back config,
    as well as implementing the special Pre-Processing and Cleanup pipelines.
    """

    def optimize_me(
        config: Dict[str, Any], global_config: OptimizationConfig, *args, **kwargs
    ) -> dict:
        # this is some fuckery
        create_custom_resolvers()
        local_global_config = OmegaConf.create(deepcopy(global_config))

        # update with the ray convention
        context = train.get_context()
        local_global_config.Metadata.run_id = context.get_trial_id()
        local_global_config.Metadata.cwd = context.get_trial_dir()

        # update the config with the ray dictionary
        local_global_config.Optimization.SearchSpace.update_function(
            local_global_config.Optimization.SearchSpace, config
        )

        for _ in range(global_config.Optimization.ObjectiveFn.n_iterations):
            # execute the pre-processing pipeline
            block = get_pipeline_by_name(local_global_config, "Pre-Processing")
            if block is not None:
                execute_pipe_block(block, local_global_config)

            # execute the target function
            res = local_global_config.Optimization.ObjectiveFn.function(
                *args,
                config=local_global_config,
                function_config=local_global_config.Optimization.ObjectiveFn.config,
                **kwargs,
            )

            # try to execute the cleanup pipeline
            block = get_pipeline_by_name(local_global_config, "Cleanup")
            if block is not None:
                execute_pipe_block(block, local_global_config)

            if local_global_config.Optimization.ObjectiveFn.additional_returns:
                res.update(
                    local_global_config.Optimization.ObjectiveFn.additional_returns
                )

        return res

    return optimize_me


def with_parameter_wrapper(
    trainable: Callable,
    config: OptimizationConfig,
) -> Callable:
    if config.Optimization.ObjectiveWrapper.function is not None:
        kwargs = config.Optimization.ObjectiveWrapper.function(
            config.Optimization.ObjectiveWrapper.config, config
        )
    else:
        kwargs = {}

    # resolve the Metadata
    OmegaConf.resolve(config.Metadata)

    return tune.with_parameters(
        trainable, global_config=OmegaConf.to_container(config, resolve=False), **kwargs
    )


def handle_results(res: tune.ResultGrid, config: OptimizationConfig):
    """
    This function handles the results of the optimization.
    """
    local_config = deepcopy(config)
    print("Best config: ", res.get_best_result().config)
    print("Path: ", res.get_best_result().path)
    output_path = Path(local_config.Optimization.Output.save_path)

    if local_config.Optimization.Output.save_results_table:
        output_path.mkdir(parents=True, exist_ok=True)
        res.get_dataframe().to_csv(Path(local_config.Metadata.output) / "results.csv")

    if local_config.Optimization.Output.save_best_config:
        cp_path = Path(res.get_best_result().path) / "config.yaml"
        # try to just copy the file
        if cp_path.exists():
            # copy the config file to the output directory
            with open(cp_path) as f:
                with open(output_path / "best_config.yaml", "w") as f2:
                    f2.write(f.read())
        else:
            # this was early stopped
            # so we have to update the global config with the parameters from the best trial
            local_config = deepcopy(config)
            local_config.Optimization.SearchSpace.update_function(
                local_config.Optimization.SearchSpace,
                res.get_best_result().config,
            )
            # save the config file


def final_cleanup(res: tune.ResultGrid, config: OptimizationConfig) -> None:
    """
    This function handles the final cleanup of the optimization.


    Args:
        res (tune.ResultGrid): _description_
        config (OptimizationConfig): _description_
    """

    pass
