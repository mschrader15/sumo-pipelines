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


@lru_cache(maxsize=128)
def get_max_fc(
    data_path: str,
    emission_class: str,
) -> dict:
    # file looks like this:
    """
    cp_norm(rated),FC
        [-],[g/h/kWrated]
        Pdrive (Engine)= 156.3425,Extrapolation: Pe: -0.2
        idle,16.58278
        -0.2,0
        -0.1,3.704487
        0,10.14675
        0.1,28.73112
        0.2,45.0095
        0.3,63.09936
        0.4,83.91012
        0.5,104.7304
        0.6,126.562
        0.7,143.5274
        0.8000001,160.0402
        0.9000001,180.7397
        1,200.8281
    """
    file = f"{data_path}/{emission_class}_FC.csv"

    assert os.path.exists(file), f"File does not exist! ({emission_class}_FC.csv)"

    with open(file) as f:
        # just read that last line with text
        s = f.read().strip().split("\n")[-1]

    # get the values
    values = s.split(",")
    return {
        "cp_norm": float(values[0]),
        "FC": float(values[1])
        * 1e3
        * 1
        / 3600,  # g/h/kWrated -> g/s/kWrated -> mg/s/kWrated
    }


def calc_normalized_fc(
    df: pl.DataFrame,
    fc_col: str,
    output_col: str,
    emission_class_column: str = "eclass",
    emissions_data_path: Optional[str] = None,
) -> pl.DataFrame:
    if emissions_data_path is None:
        emissions_data_path = os.path.join(
            os.environ["SUMO_HOME"],
            "data/emissions",
        )

    assert (
        df[emission_class_column].str.contains("PHEMlight/").all()
    ), "Only PHEMLight (not V5) emission classes are supported"

    norm_constants = {}

    for d in df[emission_class_column].unique():
        mg_s_kw = get_max_fc(emissions_data_path, d)["FC"]
        # case PollutantsInterface::FUEL: {
        #     if (myVolumetricFuel && fuelType == PHEMlightdll::Constants::strDiesel) { // divide by average diesel density of 836 g/l
        #         return getEmission(oldCep, currCep, "FC", power, corrSpeed) / 836. / SECONDS_PER_HOUR * 1000.;
        #     }
        #     if (myVolumetricFuel && fuelType == PHEMlightdll::Constants::strGasoline) { // divide by average gasoline density of 742 g/l
        #         return getEmission(oldCep, currCep, "FC", power, corrSpeed) / 742. / SECONDS_PER_HOUR * 1000.;
        #     }
        #     if (fuelType == PHEMlightdll::Constants::strBEV) {
        #         return 0.;
        #     }
        #     return getEmission(oldCep, currCep, "FC", power, corrSpeed) / SECONDS_PER_HOUR * 1000.; // still in mg even if myVolumetricFuel is set!
        # }
        max_power = float(read_vehicle_file(emissions_data_path, d)["rated_power"])

        norm_constants[d] = mg_s_kw * max_power  # -> mg/s/kW -> mg/s

    df = df.with_columns(
        (
            pl.col(fc_col)
            / pl.col(emission_class_column).replace(
                norm_constants, return_dtype=pl.Float32()
            )
        ).alias(output_col)
    )

    # assert df[output_col].max() <= 1, "Normalized FC should be less than 1"

    return df


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
        df[emission_class_column].str.contains("PHEMlight/").all()
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
            + (type_data["cw_value"] * type_data["cross_area"] * AIR_DENSITY_CONST / 2)
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
