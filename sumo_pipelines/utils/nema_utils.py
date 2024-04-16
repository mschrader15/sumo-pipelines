import xml.etree.ElementTree as ET
from collections import OrderedDict

import polars as pl
import sumolib


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
