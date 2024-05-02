import os
import xml.etree.ElementTree as ET
from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass, fields
from os import PathLike
from typing import Dict, List, Union

import polars as pl
import sumolib

try:
    import iteround
except ImportError:
    iteround = None


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

    @property
    def yellow_red_duration(
        self,
    ) -> float:
        return self.red + self.yellow


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

    def get_phase_list(self) -> List[Phase]:
        return list(self.phases.values())

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
            params = dict(
                [g.split("=") for g in tl.group(1).replace('"', "").split(" ")]
            )
            if "offset" not in params:
                params["offset"] = 0
            tl = cls(**params)
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

    def get_valid_phase_combos(
        self,
    ) -> None:
        barrier_phases = tuple(
            map(int, self.params.get("barrierPhases", "").value.split(","))
        )
        barrier2_phases = tuple(
            map(
                int,
                self.params.get(
                    "barrier2Phases", self.params.get("coordinatePhases", "")
                ).value.split(","),
            )
        )

        coordinated_phases = tuple(
            map(int, self.params.get("coordinatePhases", "").value.split(","))
        )

        ring_1 = [
            int(p) for p in self.params.get("ring1", "").value.split(",") if int(p) > 0
        ]
        ring_2 = [
            int(p) for p in self.params.get("ring2", "").value.split(",") if int(p) > 0
        ]

        barrier_mapping = {}
        b1, b2 = 0, 0
        for p1, p2 in zip(ring_1, ring_2):
            barrier_mapping[int(p1)] = b1
            barrier_mapping[int(p2)] = b2
            if p1 in {barrier_phases[0], barrier2_phases[0]}:
                b1 += 1
            if p2 in {barrier_phases[1], barrier2_phases[1]}:
                b2 += 1

        valid_phases = set()
        for p1 in ring_1:
            for p2 in ring_2:
                if barrier_mapping[p1] == barrier_mapping[p2]:
                    valid_phases.add((p1, p2))
        valid_phases = list(valid_phases)

        return sorted(
            valid_phases,
            key=lambda x: 2
            * (
                x
                in [
                    coordinated_phases,
                ]
            )
            or 1 * (x in [barrier2_phases, barrier_phases]),
            reverse=True,
        )

    def update_coordinate_splits(
        self,
        splits: dict[int, float],
    ) -> None:
        # get the coordinate phases
        coord_phases = self.params.get("coordinatePhases", "").value.split(",")
        if not coord_phases:
            raise ValueError("No coordinate phases found in the NEMA configuration")

        barrier_phases = list(
            map(int, self.params.get("barrierPhases", "").value.split(","))
        )
        barrier2_phases = list(
            map(
                int,
                self.params.get(
                    "barrier2Phases", self.params.get("coordinatePhases", "")
                ).value.split(","),
            )
        )

        # construct a list of list of phases
        ring_1 = [
            int(p) for p in self.params.get("ring1", "").value.split(",") if int(p) > 0
        ]
        ring_2 = [
            int(p) for p in self.params.get("ring2", "").value.split(",") if int(p) > 0
        ]

        barrier_mapping = {}
        b1, b2 = 0, 0
        for p1, p2 in zip(ring_1, ring_2):
            barrier_mapping[int(p1)] = b1
            barrier_mapping[int(p2)] = b2
            if p1 in {barrier_phases[0], barrier2_phases[0]}:
                b1 += 1
            if p2 in {barrier_phases[1], barrier2_phases[1]}:
                b2 += 1

        # get the cycle length
        cycle_length = sum(self.phases[p].total_duration for p in ring_1)
        assert cycle_length == sum(self.phases[p].total_duration for p in ring_2)

        phase_holder = {
            0: {p: deepcopy(self.phases[p]) for p in ring_1},
            1: {p: deepcopy(self.phases[p]) for p in ring_2},
        }

        # update the main side of the barrier
        b_num = None
        for p, _ in splits.items():
            if b_num is not None:
                assert barrier_mapping[p] == b_num
            else:
                b_num = barrier_mapping[p]

        new_phases = deepcopy(phase_holder)
        barrier_adjustments = []
        for ring_num, (p, split) in enumerate(splits.items()):
            new_phases[ring_num][p].maxDur = (
                cycle_length * split - phase_holder[ring_num][p].yellow_red_duration
            ) // 1

            # include
            extra_time = (
                # include
                cycle_length
                - sum(
                    new_phases[ring_num][_p].total_duration
                    for _p in new_phases[ring_num].keys()
                )
            )

            distribute_time = sum(
                _p.maxDur - _p.minDur
                for _p in phase_holder[ring_num].values()
                if _p.name != p
            )
            assert -1 * extra_time < distribute_time

            add_time = {
                _p.name: ((_p.maxDur - _p.minDur) / distribute_time) * extra_time
                for _p in phase_holder[ring_num].values()
                if _p.name != p
            }

            # rounds = list(add_time.values())

            add_time = {
                _p: rounded_time
                for _p, rounded_time in zip(
                    add_time.keys(), iteround.saferound(add_time.values(), places=1)
                )
            }

            for _p, t in add_time.items():
                new_phases[ring_num][_p].maxDur += t

                # if new_phases[_p].maxDur

            barrier_split = sum(
                _p.total_duration
                for _p in new_phases[ring_num].values()
                if barrier_mapping[_p.name] == b_num
            )
            barrier_adjustments.append(barrier_split)

        max_barrier = max(barrier_adjustments)

        for ring_num, ((_, _), b_length) in enumerate(
            zip(splits.items(), barrier_adjustments)
        ):
            if b_length < max_barrier:
                update_phases = [
                    _p
                    for _p in new_phases[ring_num].values()
                    if barrier_mapping[_p.name] == b_num
                ]

                # assert len(update_phases) == 1

                update_phase = update_phases[0]

                update_phase.maxDur = (
                    max_barrier
                    - sum(
                        _p.total_duration
                        for _p in new_phases[ring_num].values()
                        if barrier_mapping[_p.name] == b_num
                        and _p.name != update_phase.name
                    )
                    - update_phase.yellow
                    - update_phase.red
                )

                assert update_phase.maxDur >= update_phase.minDur

        # update the side street barrier
        # other_barrier_length = cycle_length - max_barrier
        other_barrier = 1 - b_num
        # one of the rings should now add up to the cycle length
        good_r = None
        new_barrier_time = 0
        for good_ring, r_ in enumerate([ring_1, ring_2]):
            if (
                round(sum(new_phases[good_ring][p].total_duration for p in r_), 1)
                == cycle_length
            ):
                good_r = r_
                new_barrier_time += sum(
                    [
                        new_phases[good_ring][r__].total_duration
                        for r__ in r_
                        if barrier_mapping[r__] == other_barrier
                    ]
                )
                break

        assert good_r is not None

        # update the bad ring
        bad_ring = [ring_1, ring_2][1 - good_ring]
        bad_phases = [p for p in bad_ring if barrier_mapping[p] == other_barrier]

        distribute_time = new_barrier_time - sum(
            new_phases[1 - good_ring][p].total_duration for p in bad_phases
        )
        available_time = sum(
            new_phases[1 - good_ring][_p].maxDur - new_phases[1 - good_ring][_p].minDur
            for _p in bad_phases
        )

        assert -1 * distribute_time <= available_time

        add_time = {
            _p: (
                (
                    new_phases[1 - good_ring][_p].maxDur
                    - new_phases[1 - good_ring][_p].minDur
                )
                / available_time
            )
            * distribute_time
            for _p in bad_phases
        }

        add_time = {
            _p: rounded_time
            for _p, rounded_time in zip(
                add_time.keys(), iteround.saferound(add_time.values(), places=1)
            )
        }

        for k, v in add_time.items():
            new_phases[1 - good_ring][k].maxDur += v

        assert cycle_length == round(
            sum(p.total_duration for p in new_phases[0].values()), 1
        )
        assert cycle_length == round(
            sum(p.total_duration for p in new_phases[1].values()), 1
        )

        new_phases = {
            p.name: p for ring_dict in new_phases.values() for p in ring_dict.values()
        }

        self.phases = deepcopy(new_phases)

        # # update the split as a percentage
        # for ring_num, (p, split) in enumerate(splits.items()):
        #     ring_num += 1

        #     # self.phases[p].maxDur
        #     self.phases[p].maxDur = cycle_length * split
        #     extra_time = total_greens[ring_num - 1] - sum(
        #         self.phases[p].maxDur for p in [ring_1, ring_2][ring_num - 1]
        #     )

        #     # distribute the extra time evenly to the other phases
        #     for p_other in ring_1 if ring_num == 1 else ring_2:
        #         if p != p_other:
        #             self.phases[p_other].maxDur += extra_time / (
        #                 len(ring_1 if ring_num == 1 else ring_2) - 1
        #             )

        # assert cycle_length == sum(self.phases[p].total_duration for p in ring_1)

        # assert cycle_length == sum(self.phases[p].total_duration for p in ring_2)


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
            2: 0.76666666,
            6: 0.63333333,
        }
    )

    tl.get_valid_phase_combos()

    # tl.update_coordinate_splits(
    #     {
    #         4: 0.2,
    #         8: 0.1,
    #     }
    # )

    # tl.update_coordinate_splits(
    #     {
    #         1: 0.5,
    #         5: 0.5,
    #     }
    # )

    tl.update_phase(2, vehext=10)

    res = tl.to_xml()

    print(res)
