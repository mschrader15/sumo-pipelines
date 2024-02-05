from pathlib import Path
from typing import List

import click
from sumo_pipelines.optimization.optimize import run_optimization
from sumo_pipelines.run import run_pipeline


try:
    import ray
    ray_exists = True
except ImportError:
    ray_exists = False


@click.command()
@click.argument("config", type=click.Path(exists=True, resolve_path=True), nargs=-1)
@click.option("--debug", is_flag=True, default=False)
@click.option('--gui', is_flag=True, default=False)
@click.option('--replay', is_flag=True, default=False)
def main(config: List[str], debug: bool, gui: bool, replay: bool) -> None:
    """Run the pipeline"""
    if isinstance(config, (tuple, list)):
        config = list(map(Path, config))
        
    run_pipeline(config, debug, gui, replay)


@click.command()
@click.argument("config", type=click.Path(exists=True, resolve_path=True), nargs=-1)
@click.option("--debug", is_flag=True, default=False)
@click.option('--gui', is_flag=True, default=False)
def optimize(config: List[str], debug: bool, gui: bool) -> None:
    """Run the pipeline"""
    if isinstance(config, (tuple, list)):
        config = list(map(Path, config))
    
    if gui:
        raise NotImplementedError('gui not implemented')
    
    run_optimization(config, debug)



if __name__ == "__main__":
    main()
