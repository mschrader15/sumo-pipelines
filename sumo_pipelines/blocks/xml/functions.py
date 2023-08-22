from pathlib import Path
from omegaconf import DictConfig
import sumolib
import pyarrow as pa
import pyarrow.parquet as pq
import re

from .config import XMLConvertConfig, XMLChangeOutputConfig


def update_output_file(config: XMLChangeOutputConfig, parent_config: DictConfig) -> None:
    """
    This function takes an xml file that has an output path (like SUMO's detector files), 
    and changes the target output destination to the target path.

    """
    for change in config.changes:    
        # Open the xml file
        with open(change.source, 'r') as file:
            filedata = file.read()

        # Replace the target output destination
        filedata = re.sub(r'file="([^"]*)"', f'file="{change.new_output}"', filedata)

        # Write the file out again
        with open(change.target, 'w') as file:
            file.write(filedata)


def convert_xml_to_parquet(config: XMLConvertConfig, parent_config: DictConfig) -> None:
    """
    This function converts a sumo xml file to a parquet file using pyarrow.

    Args:
        config (XMLConvertConfig): The config file to save.
    """
    elements = {a: [] for element in config.elements for a in element.attributes}

    for row in sumolib.xml.parse_fast_nested(
        config.source,
        config.elements[0]["name"],
        config.elements[0]["attributes"],
        config.elements[1]["name"],
        config.elements[1]["attributes"],
        optional=True,  
    ):
        for attr in config.elements[0].attributes:
            elements[attr].append(getattr(row[0], attr))
        
        for attr in config.elements[1].attributes:
            elements[attr].append(getattr(row[1], attr))

    for k, v in elements.items():
        elements[k] = pa.array(v, type=pa.string())

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
    

