from copy import deepcopy
from pathlib import Path

from omegaconf import OmegaConf
from ray.util.queue import Queue

from sumo_pipelines.utils.config_helpers import config_wrapper

from .config import DuckDBConfig, RemoveFileConfig, SaveConfig


@config_wrapper
def save_config(config: SaveConfig, parent_config: OmegaConf) -> None:
    """
    This function saves the config file to a json file.

    Args:
        config (SaveConfig): The config file to save.
    """
    from sumo_pipelines.config import MetaData

    local_config = deepcopy(parent_config)
    Path(config.save_path).parent.mkdir(parents=True, exist_ok=True)
    with open(config.save_path, "w") as f:
        # resolve only the metadata
        local_config.Metadata = MetaData(
            **OmegaConf.to_container(local_config.Metadata, resolve=True)
        )
        d = OmegaConf.to_container(local_config, resolve=False)
        # pop blocks that we don't want to save
        keys = list(d["Blocks"].keys())
        for b in keys:
            if isinstance(d["Blocks"][b], str) or (d["Blocks"][b] is None):
                d["Blocks"].pop(b)

        f.write(OmegaConf.to_yaml(OmegaConf.create(d), resolve=False))


@config_wrapper
def mv_file(config: SaveConfig, parent_config: OmegaConf) -> None:
    """
    This function moves a list of target files to their proper destination

    Args:
        config (SaveConfig): The config file to save.
    """
    for f in config.mv_files:
        Path(f.target).parent.mkdir(parents=True, exist_ok=True)
        Path(f.source).rename(f.target)


@config_wrapper
def rm_file(config: RemoveFileConfig, parent_config: OmegaConf) -> None:
    """
    This function removes a list of target files

    Args:
        config (RemoveFileConfig): The config file to save.
    """
    import os

    for f in config.rm_files:
        if os.path.isfile(f):
            os.remove(f)
            print(f"Removed file: {f}")
        else:
            try:
                p = Path(f)
                for _p in p.parent.glob(f"{p.name}"):
                    os.remove(str(_p))
                    print(f"Removed file: {_p}")
            except Exception:
                print(f"Error removing file: {f}")


@config_wrapper
def build_duckdb_database(config: DuckDBConfig, *args, **kwargs) -> None:
    import duckdb as db

    from sumo_pipelines.utils.file_helpers import SystemMutex

    # create a connection
    with SystemMutex(config.db_path), db.connect(config.db_path) as con:
        # create a table
        con.execute(f"CREATE TABLE {config.table_name} ")
        # add all the fields
        for col in config.write_values:
            con.execute(
                f"ALTER TABLE {config.table_name} ADD COLUMN {col.name} {col.dtype}"
            )


@config_wrapper
def dump_duckdb_database(config: DuckDBConfig, *args, **kwargs) -> None:
    import duckdb as db

    from sumo_pipelines.utils.file_helpers import SystemMutex

    # create a connection
    with db.connect(config.db_path) as con, SystemMutex(config.db_path):
        # create a table
        con.execute(
            f"COPY {config.table_name} TO '{config.parquet_path}' (FORMAT PARQUET);"
        )


@config_wrapper
def persistent_db_writer(config: DuckDBConfig, queue: Queue, *args, **kwargs) -> None:
    import duckdb as db

    from sumo_pipelines.utils.file_helpers import SystemMutex

    # create a connection
    with db.connect(config.db_path) as con, SystemMutex(config.db_path):
        # # create a table
        # con.execute(f"INSERT INTO {config.table_name} VALUES {config.values}")
        # create a batched insert
        while True:
            batch = queue.get()
            if batch is None:
                break
            con.execute(f"INSERT INTO {config.table_name} VALUES {batch}")
            queue.task_done()
