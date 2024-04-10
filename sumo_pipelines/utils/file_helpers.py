import concurrent.futures
import fcntl
import hashlib
import os
import sys
from concurrent.futures import ProcessPoolExecutor as ThreadPoolExecutor
from pathlib import Path
from typing import Generator

from sumo_pipelines.config import PipelineConfig
from sumo_pipelines.utils.config_helpers import open_completed_config


def open_config_file(
    file: Path,
    root_dir: Path,
) -> PipelineConfig:
    """Open and parse a config file if it's not in the root directory"""
    if file.parent == root_dir:
        return None
    return open_completed_config(file, validate=False)


def walk_directory(
    root_dir: Path, file_type: str = ".yaml"
) -> Generator[PipelineConfig, None, None]:
    """Walk a directory and sub directories and yield the parsed config files"""
    # skip the config at the parent level
    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(
                open_config_file,
                file,
                root_dir,
            ): file
            for file in root_dir.glob(f"*/*{file_type}")
        }

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result is not None:
                yield result


# Ray Specific Mutex
# from ray.util.queue import Queue


class SystemMutex:
    # from this madlad: https://github.com/ray-project/ray/issues/8017#issuecomment-657500234

    def __init__(self, name):
        self.name = name

    def __enter__(self):
        lock_id = hashlib.md5(self.name.encode("utf8")).hexdigest()
        self.fp = open(f"/tmp/.lock-{lock_id}.lck", "wb")
        fcntl.flock(self.fp.fileno(), fcntl.LOCK_EX)

    def __exit__(self, _type, value, tb):
        fcntl.flock(self.fp.fileno(), fcntl.LOCK_UN)
        self.fp.close()


def import_sumo_tools_module(module_name: str):
    try:
        sys.path.append(os.path.join(os.environ.get("SUMO_HOME"), "tools"))
    except TypeError as e:
        raise TypeError("SUMO_HOME is not set") from e

    try:
        imported_module = __import__(module_name)
        return imported_module
    except ImportError as e:
        raise ImportError(f"{module_name} must be in your path") from e
