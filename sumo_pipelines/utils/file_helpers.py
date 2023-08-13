from pathlib import Path
from typing import Generator
from sumo_pipelines.config import PipelineConfig, open_completed_config



def walk_directory(root_dir: Path, file_type: str = ".yaml") -> Generator[PipelineConfig, None, None]:
    """Walk a directory and sub directories and yield the parsed config files"""
    # skip the config at the parent level
    for file in root_dir.rglob(f"*{file_type}"):
        if file.parent == root_dir:
            continue
        yield open_completed_config(file, validate=False)