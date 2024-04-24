import os
import xml.etree.ElementTree as ET
from collections import OrderedDict
from dataclasses import dataclass, fields
from os import PathLike
from typing import Dict, Union

import polars as pl
import sumolib


@dataclass
class Phase:
    name: int
    state: str
    minDur: float
    maxDur: float
    vehext: float
    yellow: float
    red: float
    duration: float

    def to_xml(
        self,
    ) -> str:
        return f"""<phase {' '.join(f'{k}="{getattr(self, k)}"' for k in self.__dataclass_fields__)}/>"""

    @classmethod
    def from_xml(cls, xml: str):
        type_dict = {k.name: k.type for k in fields(cls)}

        return cls(
            **dict(
                (k, type_dict[k](v))
                for k, v in (
                    p.split("=")
                    for p in xml.replace('"', "")
                    .replace("<phase", "")
                    .replace("/>", "")
                    .split()
                )
            )
        )

    @property
    def total_duration(
        self,
    ) -> float:
        return self.maxDur + self.yellow + self.red


@dataclass
class Param:
    key: str
    value: str

    def to_xml(
        self,
    ) -> str:
        return f"""<param key="{self.key}" value="{self.value}"/>"""

    @classmethod
    def from_xml(cls, xml: str):
        type_dict = {k.name: k.type for k in fields(cls)}

        return cls(
            **dict(
                (k, type_dict[k](v))
                for k, v in (
                    p.split("=")
                    for p in xml.replace('"', "")
                    .replace("<param", "")
                    .replace("/>", "")
                    .split()
                )
            )
        )


@dataclass
class NEMALight:
    id: str
    programID: str
    offset: float
    type: str

    def __post_init__(self):
        self.phases: Dict[int, Phase] = {}
        self.params: Dict[str, str] = {}

    def add_phase(self, phase: Phase):
        self.phases[phase.name] = phase

    def add_param(self, param: Param):
        self.params[param.key] = param

    def to_xml(
        self,
    ) -> str:
        params = "\t\t".join(p.to_xml() + "\n" for p in self.params.values())
        phases = "\t\t".join(p.to_xml() + "\n" for p in self.phases.values())

        return f"""
        <add>
            <tlLogic {' '.join(f'{k}="{getattr(self, k)}"' for k in self.__dataclass_fields__)}>
                {params}
                {phases}
            </tlLogic>
        </add>
        """

    @classmethod
    def from_xml(
        cls, xml: Union[str, PathLike], id: str, programID: str
    ) -> "NEMALight":
        import re

        if os.path.exists(xml):
            with open(xml) as f:
                xml = f.read()

        tl = None
        for tl in re.finditer(r"<tlLogic (.*)>", xml):
            tl = cls(
                **dict([g.split("=") for g in tl.group(1).replace('"', "").split(" ")])
            )
            if (tl.id == id) and (tl.programID == programID):
                break

        assert (
            tl is not None
        ), f"No traffic light found with id: {id} and programID: {programID}"

        assert tl.type == "NEMA", "The traffic light is not a NEMA traffic light"

        # get the phases
        for phase in re.findall(r"<phase.*?/>", xml):
            tl.add_phase(Phase.from_xml(phase))

        # get the params
        for param in re.findall(r"<param.*?/>", xml):
            tl.add_param(Param.from_xml(param))

        return tl

    def update_offset(self, offset: float):
        self.offset = offset

    def update_phase(self, phase_name: int, **params):
        for k, v in params.items():
            setattr(self.phases[phase_name], k, v)

    def update_coordinate_splits(
        self,
        splits: dict[int, float],
    ) -> None:
        raise NotImplementedError(
            "This method is not implemented yet. The below code is a work in progress."
        )

        # get the coordinate phases
        coord_phases = self.params.get("coordinatePhases", "").value.split(",")
        if not coord_phases:
            raise ValueError("No coordinate phases found in the NEMA configuration")

        # get the rings
        ring1 = self.params.get("ring1", "").value.split(",")
        ring2 = self.params.get("ring2", "").value.split(",")
        barrier_phases = self.params.get("barrierPhases", "").value.split(",")
        barrier2_phases = self.params.get(
            "barrier2Phases", self.params.get("coordinatePhases", "")
        ).value.split(",")

        # construct a list of list of phases
        ring_1 = [int(p) for p in ring1 if int(p) > 0]
        ring_2 = [int(p) for p in ring2 if int(p) > 0]

        barrier_mapping = {}
        b1, b2 = 0, 0
        for p1, p2 in zip(ring1, ring2):
            barrier_mapping[int(p1)] = b1
            barrier_mapping[int(p2)] = b2
            if p1 in {barrier_phases[0], barrier2_phases[0]}:
                b1 += 1
            if p2 in {barrier_phases[1], barrier2_phases[1]}:
                b2 += 1

        # get the cycle length
        cycle_length = sum(self.phases[p].total_duration for p in ring_1)
        assert cycle_length == sum(self.phases[p].total_duration for p in ring_2)

        total_greens = [
            sum(self.phases[p].maxDur for p in ring_1),
            sum(self.phases[p].maxDur for p in ring_2),
        ]

        # update the split as a percentage
        for ring_num, (p, split) in enumerate(splits.items()):
            ring_num += 1

            # self.phases[p].maxDur
            self.phases[p].maxDur = cycle_length * split
            extra_time = total_greens[ring_num - 1] - sum(
                self.phases[p].maxDur for p in [ring_1, ring_2][ring_num - 1]
            )

            # distribute the extra time evenly to the other phases
            for p_other in ring_1 if ring_num == 1 else ring_2:
                if p != p_other:
                    self.phases[p_other].maxDur += extra_time / (
                        len(ring_1 if ring_num == 1 else ring_2) - 1
                    )

        assert cycle_length == sum(self.phases[p].total_duration for p in ring_1)

        assert cycle_length == sum(self.phases[p].total_duration for p in ring_2)


def read_nema_config(path: str, tlsID: str = "") -> OrderedDict:
    """Reads a SUMO NEMA configuration file.

    Description here: https://sumo.dlr.de/docs/Simulation/NEMA.html

    Args:
        path (str): Path to the sumo add.xml file describing
            the NEMA traffic light
        tlsID (str, optional): The tlsID, useful if there are
            multiple traffic lights described in the same file. Defaults to "".

    Returns:
        OrderedDict: _description_
    """
    try:
        import xml2dict as xmltodict
    except ImportError as e:
        raise ImportError(
            "python-xml2dict must be installed to use this utility"
        ) from e

    with open(path) as f:
        raw = xmltodict.parse(f.read())
        # get passed the first element (either called "add" or "additional")
        tl_dict = raw[next(iter(raw.keys()))]["tlLogic"]

    # If there are multiple traffic lights in the file get the one we want
    if isinstance(tl_dict, list):
        assert len(tlsID)
        tl_dict = next(o for o in tl_dict if o["@programID"] == tlsID)

    # turn all the "params" into a unique dictionary with the
    # key being the "Key" and "Value" being the "value"
    tl_dict["param"] = {p["@key"]: p["@value"] for p in tl_dict["param"]}

    # find "skip" phases. These are phases that are always green.
    skip_phases = []
    for i, p in enumerate(tl_dict["phase"][:-1]):
        skip_phases.extend(
            iid
            for iid, (_p, _pnext) in enumerate(
                zip(p["@state"], tl_dict["phase"][i + 1]["@state"])
            )
            if _p == "G" and _pnext == "G"
        )
    skip_phases = set(skip_phases)

    # add a parameter called the controlling index,
    # useful for mapping phase names to lanes
    # this might not always be right,
    # but can't think of immediately better way to do it
    for phase in tl_dict["phase"]:
        phase["controlling_index"] = [
            i
            for i, s in enumerate(phase["@state"])
            if s == "G" and i not in skip_phases
        ]

    # CRITICAL, sort the phases by their controlling indexs
    tl_dict["phase"] = OrderedDict(
        p[:-1]
        for p in sorted(
            (
                (phase["@name"], phase, min(phase["controlling_index"]))
                for phase in tl_dict["phase"]
            ),
            key=lambda x: x[-1],
        )
    )

    return tl_dict


def build_lane_index(nema_config_xml: str, net_file_xml: str) -> None:
    # sourcery skip: low-code-quality
    """Builds a mapping of NEMA phase # to the actuating detectors.

    Args:
        nema_config_xml (str): the SUMO additional file describing the NEMA behavior
        net_file_xml (str): the
    """

    tl_dict = read_nema_config(nema_config_xml)

    # # set the program id
    # self._programID = tl_dict["@programID"]

    # open the network file to get the order of the lanes
    lane_order = {
        i: l_list[0][0]
        for i, l_list in sumolib.net.readNet(net_file_xml)
        .getTLS(tl_dict["@id"])
        .getLinks()
        .items()
    }

    p_string_map = {}
    p_lane_map = {}
    phase_2_detect = {}
    # loop through the traffic light phases, find the order that they control
    # and then the "controllng" detectors
    for phase_name, phase in tl_dict["phase"].items():
        phase_int = int(phase_name)
        # set the minimum green time
        # self._time_tracker[phase_int] = [float(phase["@minDur"]), 0]

        # save the index of the light head string
        p_string_map[phase_int] = phase["controlling_index"]
        # loop through the controlled lanes
        p_lane_map[phase_int] = []
        for lane_index in phase["controlling_index"]:
            p_lane_map[phase_int].append(lane_order[lane_index].getID())

            if detect_id := tl_dict["param"].get(lane_order[lane_index].getID(), ""):
                # the phase is controlled by a custom detector
                detect_name = detect_id
            else:
                # the phase is controlled by a generated detector.
                # They are generated according to
                # https://github.com/eclipse/sumo/issues/10045#issuecomment-1022207944
                detect_name = (
                    tl_dict["@id"]
                    + "_"
                    + tl_dict["@programID"]
                    + "_D"
                    + str(phase_int)
                    + "."
                    + str(lane_order[lane_index].getIndex())
                )

            # add the detector as a controlled detector
            # self._all_detectors.append(detect_name)
            # # add the NEMA phase to detector mapping
            if phase_int not in phase_2_detect.keys():
                phase_2_detect[phase_int] = [
                    detect_name,
                ]
            else:
                phase_2_detect[phase_int].append(detect_name)

    # # create a list of the sumo order of phases
    # temp_list = sorted(
    #     ((p, indexes) for p, indexes in p_string_map.items()),
    #     key=lambda x: x[1][0],
    # )
    # self._phases_sumo_order = [int(t[0]) for t in temp_list]

    # # find the barriers
    # bs = [
    #     tl_dict["param"]["barrierPhases"],
    #     tl_dict["param"].get("barrier2Phases", ""),
    # ]

    # bs[-1] = bs[-1] if bs[-1] != "" else tl_dict["param"]["coordinatePhases"]

    # # convert the barriers to a list of integers
    # bs = [tuple(map(int, b.split(","))) for b in bs]

    return tl_dict, phase_2_detect, p_string_map, p_lane_map


def add_params_to_xml(
    xml_file,
    params_df: pl.DataFrame,
    tl_id: str,
    program_id: str,
    write_key_column: str,
    write_param_column: str,
):
    # Parse the XML file
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # Find the tlLogic element with the desired ID
    tlLogic = None
    for element in root.findall("tlLogic"):
        if (element.get("id") == tl_id) and (element.get("programID") == program_id):
            tlLogic = element
            break

    if tlLogic is None:
        raise ValueError(f"No tlLogic element found with ID: {tl_id}")

    # Iterate over the rows of the params DataFrame
    for row in params_df.iter_rows(named=True):
        key = row[write_key_column]
        value = row[write_param_column]

        # Create a new param element
        param = ET.Element("param")
        param.set("key", key)
        param.set("value", value)

        # Append the param element to the tlLogic element
        tlLogic.append(param)

    # Overwrite the existing XML file
    tree.write(xml_file)


if __name__ == "__main__":
    # test the NEMALight class

    tl = NEMALight.from_xml(
        "/Users/max/Development/DOE-Project/airport-harper-calibration/simulation/additional/signals/63082002.NEMA.Coordinated.xml",
        id="63082002",
        programID="63082002_12",
    )

    tl.update_offset(22)

    tl.update_coordinate_splits(
        {
            2: 0.5,
            6: 0.5,
        }
    )

    tl.update_phase(2, vehext=10)

    res = tl.to_xml()

    print(res)
