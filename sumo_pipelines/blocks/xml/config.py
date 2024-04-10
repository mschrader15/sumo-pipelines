from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class XMLConvertConfig:
    """
    This class is custom to our use case. It is used to sample parameters from a table
    """

    source: str
    target: str
    elements: List[Dict]
    format: str = "parquet"
    delete_source: bool = False

    def __post_init__(self):
        assert len(self.elements) < 3


@dataclass
class XMLSimpleRegexConfig:
    source: str
    target: str
    regex: str
    replace: str


@dataclass
class XMLChangeConfig:
    source: str
    target: str
    new_output: str


@dataclass
class XMLChangeOutputConfig:
    changes: List[XMLChangeConfig]


@dataclass
class EmissionXMLtoParquetConfig:
    input_file: str
    output_file: str
    remove_input: bool = field(default=False)
