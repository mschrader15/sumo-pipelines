from typing import List, Union
import sumolib
import hydra
from pathlib import Path
from omegaconf import DictConfig, OmegaConf
from outputs.detector_relationships import DetectorRelationships


@hydra.main(version_base=None, config_path=".", config_name="config.yaml")
def get_locations(config: OmegaConf) -> List[DetectorRelationships]:
    config.detector_files = (
        [config.detector_files]
        if isinstance(config.detector_files, str)
        else config.detector_files
    )

    net = sumolib.net.readNet(config.net_file)

    # read the the detectors
    detectors = []
    for detector_file in config.detector_files:
        detectors += sumolib.xml.parse(detector_file, f"{config.detector_type}")

    # get the detector locations
    detector_locations = []
    for detector in detectors:
        detector_locations.append(net.getLane(detector.lane))

    return detector_locations


if __name__ == "__main__":
    get_locations()
