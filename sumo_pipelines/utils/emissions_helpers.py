import os
from functools import lru_cache
from typing import Optional

import numpy as np
import polars as pl

GRAVITY_CONST = 9.81
AIR_DENSITY_CONST = 1.182
NORMALIZING_SPEED = 19.444
NORMALIZING_ACCELARATION = 0.45
SPEED_DCEL_MIN = 10 / 3.6
ZERO_SPEED_ACCURACY = 0.5
DRIVE_TRAIN_EFFICIENCY_All = 0.9
DRIVE_TRAIN_EFFICIENCY_CB = 0.8


def read_line(line):
    line = line.rstrip()
    return line.strip()


@lru_cache(maxsize=128)
def read_vehicle_file(data_path: str, emission_class):
    # taken directly from https://github.com/eclipse-sumo/sumo/blob/4a6c257cf1f911e5bcd19912d1e3849de680a84c/src/foreign/PHEMlight/cpp/CEPHandler.cpp#L109
    # translated with claude.ai
    vehicle_data = {
        "vehicle_mass": 0.0,
        "vehicle_loading": 0.0,
        "vehicle_mass_rot": 0.0,
        "cross_area": 0.0,
        "cw_value": 0.0,
        "f0": 0.0,
        "f1": 0.0,
        "f2": 0.0,
        "f3": 0.0,
        "f4": 0.0,
        "axle_ratio": 0.0,
        "rated_power": 0.0,
        "aux_power": 0.0,
        "engine_idling_speed": 0.0,
        "engine_rated_speed": 0.0,
        "effective_wheel_diameter": 0.0,
        "vehicle_mass_type": "",
        "vehicle_fuel_type": "",
        "p_norm_v0": 0.0,
        "p_norm_p0": 0.0,
        "p_norm_v1": 0.0,
        "p_norm_p1": 0.0,
        "transmission_gear_ratios": [],
        "matrix_speed_inertia_table": [],
        "normed_drag_table": [],
    }

    # Open file
    vehicle_file = None

    file_path = f"{data_path}/{emission_class}.PHEMLight.veh"
    if os.path.exists(file_path):
        vehicle_file = open(file_path)

    if vehicle_file is None:
        print(f"File does not exist! ({emission_class}.PHEMLight.veh)")
        return False, vehicle_data

    # Skip header
    read_line(vehicle_file.readline())

    comment_prefix = "c"  # Specify the comment prefix used in the file
    line_no = 0
    for _, line in enumerate(vehicle_file, start=1):
        line = read_line(line)
        if line.startswith(comment_prefix) or not line:
            continue

        line_no += 1
        cell = line.split(",")[0]

        if line_no == 1:
            vehicle_data["vehicle_mass"] = float(cell)
        elif line_no == 2:
            vehicle_data["vehicle_loading"] = float(cell)
        elif line_no == 3:
            vehicle_data["cw_value"] = float(cell)
        elif line_no == 4:
            vehicle_data["cross_area"] = float(cell)
        elif line_no == 7:
            vehicle_data["vehicle_mass_rot"] = float(cell)
        elif line_no == 9:
            vehicle_data["aux_power"] = float(cell)
        elif line_no == 10:
            vehicle_data["rated_power"] = float(cell)
        elif line_no == 11:
            vehicle_data["engine_rated_speed"] = float(cell)
        elif line_no == 12:
            vehicle_data["engine_idling_speed"] = float(cell)
        elif line_no == 14:
            vehicle_data["f0"] = float(cell)
        elif line_no == 15:
            vehicle_data["f1"] = float(cell)
        elif line_no == 16:
            vehicle_data["f2"] = float(cell)
        elif line_no == 17:
            vehicle_data["f3"] = float(cell)
        elif line_no == 18:
            vehicle_data["f4"] = float(cell)
        elif line_no == 21:
            vehicle_data["axle_ratio"] = float(cell)
        elif line_no == 22:
            vehicle_data["effective_wheel_diameter"] = float(cell)
        elif 23 <= line_no <= 40:
            vehicle_data["transmission_gear_ratios"].append(float(cell))
        elif line_no == 45:
            vehicle_data["vehicle_mass_type"] = cell
        elif line_no == 46:
            vehicle_data["vehicle_fuel_type"] = cell
        elif line_no == 47:
            vehicle_data["p_norm_v0"] = float(cell)
        elif line_no == 48:
            vehicle_data["p_norm_p0"] = float(cell)
        elif line_no == 49:
            vehicle_data["p_norm_v1"] = float(cell)
        elif line_no == 50:
            vehicle_data["p_norm_p1"] = float(cell)
            break

    comment_flag = False
    for line in vehicle_file:
        line = read_line(line)
        if line.startswith(comment_prefix) or not line:
            if comment_flag:
                break
            comment_flag = True
            continue
        vehicle_data["matrix_speed_inertia_table"].append(
            [float(x) for x in line.split(",")]
        )

    vehicle_data["matrix_speed_inertia_table"] = np.array(
        vehicle_data["matrix_speed_inertia_table"]
    )
    # drop the middle column
    vehicle_data["matrix_speed_inertia_table"] = np.delete(
        vehicle_data["matrix_speed_inertia_table"], 1, 1
    )

    for line in vehicle_file:
        line = read_line(line)
        if line.startswith(comment_prefix) or not line:
            continue
        vehicle_data["normed_drag_table"].append([float(x) for x in line.split(",")])

    vehicle_file.close()
    return vehicle_data


def calc_instant_power(
    df: pl.DataFrame,
    accel_col: str,
    velocity_col: str,
    output_col: str,
    emissions_data_path: Optional[str] = None,
    emission_class_column: str = "eclass",
) -> pl.DataFrame:
    if emissions_data_path is None:
        emissions_data_path = os.path.join(
            os.environ["SUMO_HOME"],
            "data/emissions",
        )

    assert (
        df[emission_class_column].str.contains("PHEMLight/").all()
    ), "Only PHEMLight (not V5) emission classes are supported"

    def _calc_power_vclass(
        _df: pl.DataFrame,
    ) -> float:
        # this is take straight from https://github.com/eclipse-sumo/sumo/blob/main/src/foreign/PHEMlight/V5/cpp/CEP.cpp#L462
        # obviously, you could ignore the constants and just focus on where accel and speed matter, but nice to have in real units

        type_data = read_vehicle_file(
            emissions_data_path,
            _df[emission_class_column][0],
        )

        power = 0
        speed = _df[velocity_col].to_numpy(zero_copy_only=True)
        acc = _df[accel_col].to_numpy(zero_copy_only=True)

        rotFactor = np.interp(
            speed,
            type_data["matrix_speed_inertia_table"][0],
            type_data["matrix_speed_inertia_table"][1],
        )

        rolling_resistance = (
            type_data["f0"] + type_data["f1"] * speed + type_data["f4"] * speed**4
        )

        # Calculate the power
        force = (
            # road load
            (
                (type_data["vehicle_mass"] + type_data["vehicle_loading"])
                * GRAVITY_CONST
                * rolling_resistance
                # * speed
            )
            + (type_data["cw"] * type_data["cross_area"] * AIR_DENSITY_CONST / 2)
            * speed**2
            + (
                (
                    type_data["vehicle_mass"] * rotFactor
                    + type_data["vehicle_mass_rot"]
                    + type_data["vehicle_loading"]
                )
                * acc
            )
        )

        power = force * speed / 1000

        return _df.with_columns(
            pl.Series(
                name=output_col,
                values=power,
                dtype=pl.Float64,
            )
        )

    # Return result
    return df.group_by(emission_class_column).apply(_calc_power_vclass)


# if __name__ == "__main__":
#     read_vehicle_file(
#         "/Users/max/Development/sumo/data/emissions",
#         "PHEMLight/PC_G_EU6",
#     )
