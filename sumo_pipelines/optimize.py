from pathlib import Path
from typing import List

import click

from sumo_pipelines.optimization.optimize import run_optimization


@click.command()
@click.argument("config", type=click.STRING, nargs=-1)
@click.option("--debug", is_flag=True, default=False)
@click.option("--gui", is_flag=True, default=False)
def optimize(config: List[str], debug: bool, gui: bool) -> None:
    """Run the pipeline"""
    if isinstance(config, (tuple, list)):
        config = list(map(Path, config))

    run_optimization(config, debug, gui=gui)


if __name__ == "__main__":
    optimize()
