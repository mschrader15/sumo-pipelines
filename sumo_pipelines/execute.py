from pathlib import Path
from typing import List

import click

from sumo_pipelines.run import run_pipeline


@click.command()
@click.argument("config", type=click.STRING, nargs=-1)
@click.option("--debug", is_flag=True, default=False)
@click.option("--gui", is_flag=True, default=False)
@click.option("--replay", is_flag=True, default=False)
@click.option("--skip-pipe-block", type=click.STRING, default="")
def main(
    config: List[str], debug: bool, gui: bool, replay: bool, skip_pipe_block: str
) -> None:
    """Run the pipeline"""
    if isinstance(config, (tuple, list)):
        config = list(map(Path, config))

    skip_pipe_blocks = skip_pipe_block.split(",")

    run_pipeline(config, debug, gui, replay, skip_pipe_blocks)


if __name__ == "__main__":
    main()
