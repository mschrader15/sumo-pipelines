from pathlib import Path

import click
from sumo_pipelines.run import run_pipeline


try:
    import ray
    ray_exists = True
except ImportError:
    ray_exists = False


@click.command()
@click.argument("config", type=click.Path(exists=True, resolve_path=True))
@click.option("--debug", is_flag=True, default=False)
def main(config: Path, debug: bool) -> None:
    """Run the pipeline"""
    run_pipeline(config, debug)


if __name__ == "__main__":
    main()
