# try to import LIBSUMO

try:
    import libsumo  # type: ignore  # noqa: PGH003

    LIBSUMO = True
except ImportError:
    import traci as libsumo

    LIBSUMO = False

import polars as pl
import traci.constants as tc

from sumo_pipelines.blocks.simulation.functions import make_cmd
from sumo_pipelines.blocks.simulation_complex.config import ComplexSimulationConfig

# def run_sumo_delay(config, *args, **kwargs) -> None:

#     # this function should run sumo with libsumo and return the average delay


def traci_vehicle_state_runner(
    config: ComplexSimulationConfig, *args, **kwargs
) -> None:
    sumo_cmd = make_cmd(config=config)

    if config.simulation_output:
        f = open(config.simulation_output, "w")

    libsumo.start(sumo_cmd, stdout=f)

    libsumo.simulation.step(config.warmup_time)

    for vehicle in libsumo.vehicle.getIDList():
        libsumo.vehicle.subscribe(
            vehicle,
            varIDs=[
                tc.VAR_SPEED,
                tc.VAR_POSITION,
                tc.VAR_LANE_ID,
            ],
        )

    t = int(config.warmup_time * 1000)
    step_size = int(config.step_length * 1000)

    results = []

    while t < int(config.end_time * 1000):
        libsumo.simulationStep()

        for vehicle in libsumo.simulation.getDepartedIDList():
            libsumo.vehicle.subscribe(
                vehicle,
                varIDs=[
                    tc.VAR_SPEED,
                    tc.VAR_POSITION,
                    tc.VAR_LANE_ID,
                ],
            )

        results.extend(
            [k, t, *v.pop(tc.VAR_POSITION), *v.values()]
            for k, v in libsumo.vehicle.getAllSubscriptionResults().items()
        )

        t += step_size

    libsumo.close()
    f.close()

    pl.DataFrame(
        results, schema=["id", "time", "x", "y", "speed", "lane"]
    ).with_columns(pl.col("time") / 1000).write_parquet(config.vehicle_state_output)
