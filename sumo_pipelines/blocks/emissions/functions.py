import mmap
import re
from pathlib import Path

# from sumo_pipelines.utils.geo_helpers import is_inside_sm_parallel
# from sumo_pipelines.sumo_pipelines_rs import (
#     is_inside_sm_parallel_py as is_inside_sm_parallel,
# )
from .config import (
    EmissionsTableFuelTotalConfig,
    FuelTotalConfig,
    TripInfoTotalFuelConfig,
)

SUMO_DIESEL_GRAM_TO_JOULE: float = 42.8e3
SUMO_GASOLINE_GRAM_TO_JOULE: float = 43.4e3

pattern = (
    rb'id="(.+?)" eclass="\w+\/(\w+?)".+fuel="([\d\.]*)".+x="([\d\.]*)" y="([\d\.]*)"'
)


def pattern_matcher(eclass_regex: str) -> re.Pattern:
    if eclass_regex:
        return re.compile(
            rf'id="(.+?)" eclass="\w+\/({eclass_regex})".+fuel="([\d\.]*)".+x="([\d\.]*)" y="([\d\.]*)"'.encode()
        )
    return re.compile(
        rb'id="(.+?)" eclass="\w+\/(\w+?)".+fuel="([\d\.]*)".+x="([\d\.]*)" y="([\d\.]*)"'
    )


def get_polygon(config):
    if config.polygon_file:
        from sumolib.shapes import polygon

        assert Path(config.polygon_file).exists(), "The polygon file does not exist"

        polygons = polygon.read(config.polygon_file)
        assert len(polygons) == 1, "The polygon file should only contain one polygon"

        return polygons[0].shape
    return None


def get_time_indices(config, data):
    time_low_i = (
        re.search(
            'time="{}"'.format(f"{config.output_time_filter_lower:.2f}").encode(),
            data,
        ).span()[-1]
        if config.output_time_filter_lower
        else 0
    )
    try:
        time_high_i = (
            re.search(
                'time="{}"'.format(f"{config.output_time_filter_upper:.2f}").encode(),
                data,
            ).span()[0]
            if config.output_time_filter_upper
            else -1
        )
    except AttributeError:
        # this means that the time high does not exist in the file so we count all the way to the end
        time_high_i = -1
    return time_low_i, time_high_i


def get_fuel_and_position_vec(
    data,
    time_low_i,
    time_high_i,
):
    all_vehicles = []
    position_vec = []
    fuel_vec = []
    diesel_filt = b"_D_"
    gas_filt = b"_G_"
    for match in re.finditer(pattern_matcher(None), data[time_low_i:time_high_i]):
        fc = match[3]
        all_vehicles.append(match[1])

        conv = 1
        # get the fuel type
        if diesel_filt in match[2]:
            conv = SUMO_DIESEL_GRAM_TO_JOULE
        elif gas_filt in match[2]:
            conv = SUMO_GASOLINE_GRAM_TO_JOULE
        else:
            continue

        fuel_vec.append(float(fc) / 1e3 * conv)
        position_vec.append((float(match[4]), float(match[5])))
    return all_vehicles, position_vec, fuel_vec


def delete_xml(config):
    if config.delete_xml:
        Path(config.emissions_xml).unlink()


def save_to_file(config, fc_t, cars_total):
    with open(config.output_path, "w") as f:
        f.write(f"{fc_t!s},{cars_total}")


# def fast_total_energy(
#     config: FuelTotalConfig,
#     *args,
#     **kwargs,
# ) -> float:
#     """
#     This function reads the emissions xml file and returns the total energy consumption in MJ in the time range.


#     Args:
#         config (FuelTotalConfig): _description_

#     Returns:
#         float: _description_
#     """
#     polygon = get_polygon(config)

#     fc_t = 0
#     with open(config.emissions_xml, "r+") as f:
#         data = mmap.mmap(f.fileno(), 0)
#         time_low_i, time_high_i = get_time_indices(config, data)

#         all_vehicles, position_vec, fuel_vec = get_fuel_and_position_vec(
#             data,
#             time_low_i,
#             time_high_i,
#         )

#         if polygon:
#             is_inside = is_inside_sm_parallel(position_vec, polygon)
#             fuel_vec = np.array(fuel_vec)[is_inside]
#             all_vehicles = np.array(all_vehicles)[is_inside]

#         fc_t = fuel_vec.sum() * config.sim_step
#         cars_total = np.unique(all_vehicles).shape[0]

#     config.total_energy = float(fc_t)
#     config.total_vehicles = cars_total

#     delete_xml(config)


def fast_timestep_energy(
    config: FuelTotalConfig,
    *args,
    **kwargs,
) -> float:
    """
    This function reads the emissions xml file and returns the total energy consumption in MJ in the time range.
    """

    raise NotImplementedError("This function is not implemented yet")

    # diesel_filter = "_D_"  # just do this by default
    # gasoline_filter = "_G_"

    # polygon = get_polygon(config)

    # with open(config.emissions_xml, "r+") as f:
    #     data = mmap.mmap(f.fileno(), 0)
    #     time_low_i, time_high_i = get_time_indices(config, data)

    #     all_vehicles, position_vec, fuel_vec = get_fuel_and_position_vec(
    #         data, time_low_i, time_high_i, diesel_filter, SUMO_DIESEL_GRAM_TO_JOULE
    #     )
    #     all_vehicles_g, position_vec_g, fuel_vec_g = get_fuel_and_position_vec(
    #         data, time_low_i, time_high_i, gasoline_filter, SUMO_GASOLINE_GRAM_TO_JOULE
    #     )

    #     all_vehicles.extend(all_vehicles_g)
    #     position_vec.extend(position_vec_g)
    #     fuel_vec.extend(fuel_vec_g)

    #     if polygon:
    #         is_inside = is_inside_sm_parallel(position_vec, polygon)
    #         fuel_vec = fuel_vec[is_inside]
    #         all_vehicles = all_vehicles[is_inside]

    #     fc_t = sum(fuel_vec) * config.sim_step
    #     cars_total = len(set(all_vehicles))

    # if config.delete_xml:
    #     Path(config.emissions_xml).unlink()

    # # save the total fuel consumption to a file
    # with open(config.output_path, "w") as f:
    #     for time, fuel, unique_ids in time_list:
    #         # TODO: add a way to write the unique ids
    #         f.write(",".join((str(time), str(fuel))) + "\n")


def fast_tripinfo_fuel(config: TripInfoTotalFuelConfig, *args, **kwargs) -> None:
    time_high_filter = config.time_high_filter
    time_low_filter = config.time_low_filter

    with open(config.input_file) as file:
        # Memory-map the file
        mmapped_file = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
        pattern = r'<tripinfo id=".+?" depart="(.+?)" .+? arrival="(.+?)" .+?>\s*<emissions .+? fuel_abs="([^"]*)"'
        matches = re.findall(pattern, mmapped_file.read().decode("utf-8"))

    total_fuel = 0
    for match in matches:
        fuel_abs = float(match[2])

        # add logic to check if both the arrival and departure and > than time low and < time high
        # if the tiem filters are none, skip the check and sum the total fuels
        if time_low_filter is not None and time_high_filter is not None:
            depart_time = float(match[0])
            arrival_time = float(match[1])
            if (
                time_low_filter < depart_time < time_high_filter
                and time_low_filter < arrival_time < time_high_filter
            ):
                total_fuel += fuel_abs
        else:
            total_fuel += fuel_abs

    config.val = total_fuel


def emissions_table_to_total(
    config: EmissionsTableFuelTotalConfig, *args, **kwargs
) -> None:
    import polars as pl

    df = (
        pl.scan_parquet(config.input_file)
        .filter(
            pl.col("timestep").is_between(
                config.time_low_filter, config.time_high_filter
            )
        )
        .sort("timestep")
        .with_columns(
            [
                (pl.col("fuel") * config.sim_step / 1e3).alias("fuel"),
                (pl.col("x").diff() ** 2 + pl.col("y").diff() ** 2)
                .sqrt()
                .over("id")
                .fill_null(0)
                .alias("distance"),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("eclass").str.contains("_D_"))
                .then(pl.col("fuel") * SUMO_DIESEL_GRAM_TO_JOULE)
                .otherwise(pl.col("fuel") * SUMO_GASOLINE_GRAM_TO_JOULE)
                .alias("fuel_energy")
            ]
        )
    )

    config.total_fuel = df.select(pl.col("fuel").sum()).collect()["fuel"][0]
    config.total_distance = df.select(pl.col("distance").sum()).collect()["distance"][0]
    config.num_vehicles = df.select(pl.col("id").n_unique()).collect()["id"][0]
