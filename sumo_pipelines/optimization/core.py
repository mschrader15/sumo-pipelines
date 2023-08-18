# function to wrap the target function for optimization
from copy import deepcopy
from typing import Any, Callable, Dict, Optional
from pathlib import Path

from omegaconf import OmegaConf
from ray.tune.logger import Logger
from ray.tune.syncer import SyncConfig

from sumo_pipelines.optimization.config import OptimizationConfig

from ray import tune

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
    # runner = config.Optimization.ObjectiveFn.function


    # build a class to wrap the target function
    # class TrainMe(tune.Trainable):

    #     def __init__(self, config: Dict[str, Any] = None, *args, **kwargs):
    #         super().__init__(config, *args, **kwargs)

    #         self._omega_config: OmegaConf = None
    #         self._setup_args: Dict[str, Any] = {
    #             "args" : None,
    #             "kwargs" : None
    #         }
        
    #     def setup(self, config: Dict[str, Any], global_config: OptimizationConfig = None, *args, **kwargs):
    #         # store the args for the call to the train function
    #         self.setup_args = {
    #             "args" : args,
    #             "kwargs" : kwargs
    #         }

    #         self._omega_config = OmegaConf.create(deepcopy(global_config))
    #         self._omega_config.Metadata.run_id = tune.get_trial_id()
    #         self._omega_config.Metadata.cwd = tune.get_trial_dir()

    #         # update the config with the ray dictionary
    #         self._omega_config.Optimization.SearchSpace.update_function(
    #             self._omega_config.Optimization.SearchSpace, config
    #         )

    #         # execute the pre-processing pipeline
    #         block = get_pipeline_by_name(self._omega_config, "Pre-Processing")
    #         if block is not None:
    #             execute_pipe_block(block, self._omega_config)
            
    #     def train(self):
    #         return self._omega_config.Optimization.ObjectiveFn.function(
    #             config=self._omega_config,
    #             *self.setup_args["args"],
    #             **self.setup_args["kwargs"],
    #         )
        
    #     def save_checkpoint(self, checkpoint_dir: str):
    #         # save the config
    #         with open(Path(checkpoint_dir) / "config.yaml", "w") as f:
    #             f.write(OmegaConf.to_yaml(self._omega_config))
    
    #     def cleanup(self):
    #         # try to execute the cleanup pipeline
    #         block = get_pipeline_by_name(self._omega_config, "Cleanup")
    #         if block is not None:
    #             execute_pipe_block(block, self._omega_config)
    def optimize_me(
        config: Dict[str, Any], global_config: OptimizationConfig, *args, **kwargs
    ) -> dict:
        # this is some fuckery
        create_custom_resolvers()

        local_global_config = OmegaConf.create(deepcopy(global_config))

        # update with the ray convention
        local_global_config.Metadata.run_id = tune.get_trial_id()
        local_global_config.Metadata.cwd = tune.get_trial_dir()

        # update the config with the ray dictionary
        local_global_config.Optimization.SearchSpace.update_function(
            local_global_config.Optimization.SearchSpace, config
        )

        # execute the pre-processing pipeline
        block = get_pipeline_by_name(local_global_config, "Pre-Processing")
        if block is not None:
            execute_pipe_block(block, local_global_config)

        # execute the target function
        res = local_global_config.Optimization.ObjectiveFn.function(
            config=local_global_config,
            *args,
            **kwargs,
        )

        tune.get_trial_name()
        tune.get_trial_dir()

        # try to execute the cleanup pipeline
        block = get_pipeline_by_name(local_global_config, "Cleanup")
        if block is not None:
            execute_pipe_block(block, local_global_config)

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
            with open(cp_path, "r") as f:
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