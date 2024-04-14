from pathlib import Path
from typing import List

import click

from sumo_pipelines.run import run_pipeline


@click.command()
@click.argument("config", type=click.STRING, nargs=-1)
@click.option("--debug", is_flag=True, default=False)
@click.option("--gui", is_flag=True, default=False)
@click.option("--replay", is_flag=True, default=False)
@click.option("--skip-pipe-blocks", default=[], type=click.STRING, nargs=-1)
def main(
    config: List[str], debug: bool, gui: bool, replay: bool, skip_pipe_blocks: List[str]
) -> None:
    """Run the pipeline"""
    if isinstance(config, (tuple, list)):
        config = list(map(Path, config))

    run_pipeline(config, debug, gui, replay, skip_pipe_blocks)


if __name__ == "__main__":
    main()
