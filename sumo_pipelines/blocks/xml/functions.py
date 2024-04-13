import re
from collections import namedtuple
from pathlib import Path
from typing import Any, Generator, List

import pyarrow as pa
import pyarrow.parquet as pq
import sumolib
from omegaconf import DictConfig

# from sumo_pipelines.sumo_pipelines_rs import parse_emissions_xml
from .config import (
    XMLChangeOutputConfig,
    XMLConvertConfig,
    XMLSimpleRegexConfig,
)


def update_output_file(
    config: XMLChangeOutputConfig, parent_config: DictConfig
) -> None:
    """
    This function takes an xml file that has an output path (like SUMO's detector files),
    and changes the target output destination to the target path.

    """
    for change in config.changes:
        # Open the xml file
        with open(change.source) as file:
            filedata = file.read()

        # Replace the target output destination
        filedata = re.sub(r'file="([^"]*)"', f'file="{change.new_output}"', filedata)

        # Write the file out again
        with open(change.target, "w") as file:
            file.write(filedata)


def simple_regex_update(config: XMLSimpleRegexConfig, *args, **kwargs) -> None:
    with open(config.source) as file:
        filedata = file.read()

    # Replace the target output destination
    filedata = re.sub(config.regex, config.replace, filedata)

    # Write the file out again
    with open(config.target, "w") as file:
        file.write(filedata)


def _build_sumolib_parser(
    source, elements: List[dict]
) -> Generator[namedtuple, Any, Any]:
    if len(elements) == 1:
        yield from sumolib.xml.parse_fast(
            source,
            elements[0]["name"],
            elements[0]["attributes"],
            optional=True,
        )
    elif len(elements) == 2:
        yield from sumolib.xml.parse_fast_nested(
            source,
            elements[0]["name"],
            elements[0]["attributes"],
            elements[1]["name"],
            elements[1]["attributes"],
            optional=True,
        )


def _build_decomposer(elements: List[dict], element_dict) -> callable:
    if len(elements) == 1:

        def decompose(row):
            for attr in elements[0].attributes:
                element_dict[attr].append(getattr(row, attr))

    elif len(elements) == 2:

        def decompose(row):
            for i in range(2):
                for attr in elements[i].attributes:
                    element_dict[attr].append(getattr(row[i], attr))

    else:
        raise ValueError("Only 1 or 2 elements are supported")

    return decompose


def convert_xml_to_parquet(config: XMLConvertConfig, parent_config: DictConfig) -> None:
    """
    This function converts a sumo xml file to a parquet file using pyarrow.

    Args:
        config (XMLConvertConfig): The config file to save.
    """
    elements = {a: [] for element in config.elements for a in element.attributes}

    decomp_func = _build_decomposer(config.elements, elements)

    for row in _build_sumolib_parser(config.source, config.elements):
        decomp_func(row)

    for k, v in elements.items():
        elements[k] = pa.array(
            v,
        )

    table = pa.Table.from_pydict(elements)

    pq.write_table(table, config.target)

    # remove the source file
    if config.delete_source:
        Path(config.source).unlink()


def convert_xml_to_parquet_pandas(config: XMLConvertConfig, *args, **kwargs) -> None:
    import pandas as pd

    pd.read_xml(
        config.source,
        # xpath="./detector//*",
        namespaces={"detector": "http://sumo.dlr.de/xsd/det_e2_file.xsd"},
        parser="etree",
    ).to_parquet(
        config.target,
    )

    # remove the source file
    if config.delete_source:
        Path(config.source).unlink()


# def convert_emissions_xml_to_parquet(
#     config: EmissionXMLtoParquetConfig, *args, **kwargs
# ) -> None:
#     parse_emissions_xml(config.input_file, config.output_file)

#     if config.remove_input:
#         Path(config.input_file).unlink()


# if __name__ == "__main__":

#     convert_emissions_xml_to_parquet(
#         EmissionXMLtoParquetConfig(
#             input_file=
#     )
