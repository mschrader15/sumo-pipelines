from dataclasses import dataclass, field
from typing import List


@dataclass
class XMLConvertConfig:
    """
    This class is custom to our use case. It is used to sample parameters from a table
    """

    source: str
    target: str
    elements: List[dict]
    format: str = "parquet"
    delete_source: bool = False

    def __post_init__(self):
        assert len(self.elements) < 3