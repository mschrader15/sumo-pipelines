import mmap
from pathlib import Path
import re

from .config import FuelTotalConfig, TripInfoTotalFuelConfig

SUMO_DIESEL_GRAM_TO_JOULE: float = 42.8e-3
SUMO_GASOLINE_GRAM_TO_JOULE: float = 43.4e-3

pattern = (
    rb'id="(.+?)" eclass="\w+\/(\w+?)".+fuel="([\d\.]*)".+x="([\d\.]*)" y="([\d\.]*)"'
)


def fast_total_energy(
    config: FuelTotalConfig,
    *args,
    **kwargs,
) -> float:
    """
    This function reads the emissions xml file and returns the total energy consumption in MJ in the time range.

    Diesel and gasoline filters are needed to calculate the energy consumption of diesel and gasoline separately. It is applied to the vehicles' emissions class

    Args:
        file_path: path to the emissions xml file
        sim_step: simulation step
        time_low: lower time bound
        time_high: upper time bound
        diesel_filter: function to filter diesel vehicles, as a string
        gasoline_filter: function to filter gasoline vehicles, as a string

    Returns:
        total energy consumption in MJ
    """
    diesel_filter = eval(config.diesel_filter) if config.diesel_filter else False
    gasoline_filter = eval(config.gasoline_filter) if config.gasoline_filter else False
    x_filter = eval(config.x_filter) if config.x_filter else False
    y_filter = eval(config.y_filter) if config.y_filter else False

    fc_t = 0
    with open(config.emissions_xml, "r+") as f:
        data = mmap.mmap(f.fileno(), 0)
        time_low_i = (
            re.search(
                'time="{}"'.format(
                    "{:.2f}".format(config.output_time_filter_lower)
                ).encode(),
                data,
            ).span()[-1]
            if config.output_time_filter_lower
            else 0
        )
        try:
            time_high_i = (
                re.search(
                    'time="{}"'.format(
                        "{:.2f}".format(config.output_time_filter_upper)
                    ).encode(),
                    data,
                ).span()[0]
                if config.output_time_filter_upper
                else -1
            )
        except AttributeError:
            # this means that the time high does not exist in the file so we count all the way to the end
            time_high_i = -1

        all_vehicles = set()
        for match in re.finditer(pattern, data[time_low_i:time_high_i]):
            if (not x_filter or (x_filter and x_filter(float(match[4])))) and (
                not y_filter or (y_filter and y_filter(float(match[5])))
            ):
                fc = float(match[3]) / 1e3  # this is in mg/s * 1 / 1000 g/mg
                all_vehicles.add(match[1])
                if diesel_filter or gasoline_filter:
                    if gasoline_filter(match[2].decode()):
                        fc *= SUMO_GASOLINE_GRAM_TO_JOULE
                    elif diesel_filter(match[2].decode()):
                        fc *= SUMO_DIESEL_GRAM_TO_JOULE
                    else:
                        raise ValueError("The filter did not match any of the classes")
                fc_t += fc
        del data
    total_fc = fc_t * config.sim_step  # output is in MJ

    if config.delete_xml:
        Path(config.emissions_xml).unlink()

    # save the total fuel consumption to a file
    with open(config.output_path, "w") as f:
        f.write(f"{str(total_fc)},{len(all_vehicles)}")


def fast_timestep_energy(
    config: FuelTotalConfig,
    *args,
    **kwargs,
) -> float:
    """
    This function reads the emissions xml file and returns the total energy consumption in MJ in the time range.

    Diesel and gasoline filters are needed to calculate the energy consumption of diesel and gasoline separately. It is applied to the vehicles' emissions class

    Args:
        file_path: path to the emissions xml file
        sim_step: simulation step
        time_low: lower time bound
        time_high: upper time bound
        diesel_filter: function to filter diesel vehicles, as a string
        gasoline_filter: function to filter gasoline vehicles, as a string

    Returns:
        total energy consumption in MJ
    """
    diesel_filter = eval(config.diesel_filter) if config.diesel_filter else False
    gasoline_filter = eval(config.gasoline_filter) if config.gasoline_filter else False
    x_filter = eval(config.x_filter) if config.x_filter else False
    y_filter = eval(config.y_filter) if config.y_filter else False

    with open(config.emissions_xml, "r+") as f:
        data = mmap.mmap(f.fileno(), 0)
        time_low_i = (
            re.search(
                'time="{}"'.format(
                    "{:.2f}".format(config.output_time_filter_lower)
                ).encode(),
                data,
            ).span()[-1]
            if config.output_time_filter_lower
            else 0
        )
        try:
            time_high_i = (
                re.search(
                    'time="{}"'.format(
                        "{:.2f}".format(config.output_time_filter_upper)
                    ).encode(),
                    data,
                ).span()[0]
                if config.output_time_filter_upper
                else -1
            )
        except AttributeError:
            # this means that the time high does not exist in the file so we count all the way to the end
            time_high_i = -1

        finder = re.finditer(rb'time="([\d\.]*)"', data[time_low_i - 100 : time_high_i])
        time_start = next(finder)
        time_list = []
        for time_end in finder:
            time_list.append([time_start[1].decode(), 0, set()])
            for match in re.finditer(
                pattern, data[time_start.span()[1] : time_end.span()[0]]
            ):
                if (not x_filter or (x_filter and x_filter(float(match[4])))) and (
                    not y_filter or (y_filter and y_filter(float(match[5])))
                ):
                    time_list[-1][-1].add(match[1])
                    fc = float(match[3]) / 1e3  # this is in mg/s * 1 / 1000 g/mg
                    if diesel_filter or gasoline_filter:
                        if gasoline_filter(match[2].decode()):
                            fc *= SUMO_GASOLINE_GRAM_TO_JOULE
                        elif diesel_filter(match[2].decode()):
                            fc *= SUMO_DIESEL_GRAM_TO_JOULE
                        else:
                            raise ValueError(
                                "The filter did not match any of the classes"
                            )
                    time_list[-1][-1] += fc
                time_list[-1][-1] *= config.sim_step
            time_start = time_end
        del data

    if config.delete_xml:
        Path(config.emissions_xml).unlink()

    # save the total fuel consumption to a file
    with open(config.output_path, "w") as f:
        for time, fuel, unique_ids in time_list:
            # TODO: add a way to write the unique ids
            f.write(",".join((str(time), str(fuel))) + "\n")


def fast_tripinfo_fuel(config: TripInfoTotalFuelConfig, *args, **kwargs) -> None:
    time_high_filter = config.time_high_filter
    time_low_filter = config.time_low_filter

    with open(config.emissions_xml, "r") as file:
        # Memory-map the file
        mmapped_file = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
        pattern = r'<tripinfo id=".+?" depart="(.+?)" .+? arrival="(.+?)" .+? fuel_abs="([^"]*)"'
        matches = re.findall(pattern, mmapped_file.read().decode("utf-8"))

    total_fuel = 0
    for match in matches:
        depart_time = float(match[0])
        arrival_time = float(match[1])
        fuel_abs = float(match[2])

        # add logic to check if both the arrival and departure and > than time low and < time high
        # if the tiem filters are none, skip the check and sum the total fuels
        if time_low_filter is not None and time_high_filter is not None:
            if (
                time_low_filter < depart_time < time_high_filter
                and time_low_filter < arrival_time < time_high_filter
            ):
                total_fuel += fuel_abs
        else:
            total_fuel += fuel_abs

    return total_fuel
