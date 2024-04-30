# try to import LIBSUMO

try:
    import libsumo  # type: ignore  # noqa: PGH003

    LIBSUMO = True
except ImportError:
    import traci as libsumo

    LIBSUMO = False


import numpy as np
import traci.constants as tc

from sumo_pipelines.blocks.simulation.functions import make_cmd
from sumo_pipelines.blocks.simulation_complex.config import (
    PriorityTrafficLightsRunnerConfig,
)
from sumo_pipelines.utils.geo_helpers import get_polygon, is_inside_sm_parallel
from sumo_pipelines.utils.nema_utils import NEMALight

# def run_sumo_delay(config, *args, **kwargs) -> None:

#     # this function should run sumo with libsumo and return the average delay


class PhaseHolder:
    TRUCK_WAITING_TIME_FACTOR = 3

    def __init__(self, tl, phase, sim_step, e2_detector_ids):
        self.tl = tl
        self.phase = phase

        self._e3 = f"e3_{tl}_{phase}"
        self._e2s = []

        self._ids = set()
        self.accumulated_wtime_holder = dict()
        self.accumuilated_wtime = 0
        self.veh_speed_factor = 0
        self.veh_count = 0
        self.sim_step = sim_step

        self.subscribe()
        self.get_e2s(e2_detector_ids)
        self._on = True
        self.turn_off()

    def get_e2s(self, all_e2_detector_ids):
        self._e2s = list(
            filter(lambda x: f"{self.tl}_{self.phase}" in x, all_e2_detector_ids)
        )

    def turn_off(
        self,
    ):
        if self._on:
            for e2 in self._e2s:
                libsumo.lanearea.overrideVehicleNumber(e2, 0)
        self._on = False

    def turn_on(
        self,
    ):
        for e2 in self._e2s:
            libsumo.lanearea.overrideVehicleNumber(e2, 1)
        self._on = True

    def subscribe(
        self,
    ):
        libsumo.multientryexit.subscribe(
            self._e3,
            [
                tc.LAST_STEP_VEHICLE_ID_LIST,
            ],
        )

    def update(self, e3_subs, veh_subs, sim_time):
        ids = set(
            (_id, ("t" in veh_subs[_id][tc.VAR_VEHICLECLASS]))
            for _id in e3_subs[self._e3][tc.LAST_STEP_VEHICLE_ID_LIST]
        )
        add_ids = ids.difference(self._ids)
        remove_ids = self._ids.difference(ids)

        self._ids = ids
        for veh_id in add_ids:
            self.accumulated_wtime_holder[veh_id] = sim_time

        for veh_id in remove_ids:
            self.accumulated_wtime_holder.pop(
                veh_id,
            )

        self._ids = ids

        self.veh_count = 0
        self.veh_speed_factor = 0
        self.accumulated_wtime = 0
        for _id, truck in self._ids:
            self.veh_count += 6 * truck + 1
            self.veh_speed_factor += max(
                veh_subs[_id][tc.VAR_SPEED] * (6 * truck + 1), 0
            )
            self.accumulated_wtime += (
                sim_time - self.accumulated_wtime_holder[(_id, truck)]
            ) * (2 * truck + 1)


_vehicle_subscriptions = (
    tc.VAR_VEHICLECLASS,
    tc.VAR_SPEED,
    tc.VAR_POSITION,
    tc.VAR_FUELCONSUMPTION,
)


def traci_priority_light_control(
    config: PriorityTrafficLightsRunnerConfig, *args, **kwargs
) -> None:
    # config.gui = True
    sumo_cmd = make_cmd(config=config)

    mainline_weights = (
        config.intersection_weights.mainline_a,
        config.intersection_weights.mainline_b,
        config.intersection_weights.mainline_c,
    )
    side_weights = (
        config.intersection_weights.side_a,
        config.intersection_weights.side_b,
        config.intersection_weights.side_c,
    )

    if config.simulation_output:
        f = open(config.simulation_output, "w")

    libsumo.start(sumo_cmd, stdout=f)

    libsumo.simulation.step(config.warmup_time)
    step_size = int(libsumo.simulation.getDeltaT() * 1000)

    e2_detectors = libsumo.lanearea.getIDList()

    lights = []
    for junction, programID, file in config.controlled_intersections:
        nema_light = NEMALight.from_xml(xml=file, id=junction, programID=programID)
        lights.append(
            (
                junction,
                [
                    combo if combo[0] != combo[1] else (combo[0],)
                    for combo in nema_light.get_valid_phase_combos()
                ],
                {
                    p.name: PhaseHolder(
                        junction, p.name, step_size / 1000, e2_detectors
                    )
                    for p in nema_light.get_phase_list()
                },
            )
        )

        libsumo.trafficlight.setProgram(junction, programID)

    sim_time = int(libsumo.simulation.getTime() * 1000)
    end_time = int(config.end_time * 1000)

    for veh_id in libsumo.vehicle.getIDList():
        libsumo.vehicle.subscribe(veh_id, _vehicle_subscriptions)

    fuel_vec = []
    veh_vec = []

    while sim_time < end_time:
        libsumo.simulation.step()

        e3_subs = libsumo.multientryexit.getAllSubscriptionResults()
        veh_subs = libsumo.vehicle.getAllSubscriptionResults()

        for veh, veh_info in veh_subs.items():
            fuel_vec.append(
                [*veh_info[tc.VAR_POSITION], veh_info[tc.VAR_FUELCONSUMPTION]]
            )
            veh_vec.append(veh)

        for _, phase_combos, phase_holders in lights:
            for phase in phase_holders.values():
                phase.update(e3_subs, veh_subs, sim_time / 1000)
                # print(
                #     f"tl: {_} phase: {phase.phase} speed: {phase.veh_speed_factor} wait: {phase.accumulated_wtime} count: {phase.veh_count}"
                # )

            priority = []
            for combo in phase_combos:
                if combo == (2, 6):
                    weights = mainline_weights
                else:
                    weights = side_weights

                priority.append(
                    sum(
                        weights[0] * phase_holders[phase].veh_speed_factor
                        + weights[1] * phase_holders[phase].accumulated_wtime * 60
                        + weights[2] * phase_holders[phase].veh_count * 60
                        for phase in combo
                    )
                )

            best_combo = sorted(
                enumerate(phase_combos), key=lambda x: priority[x[0]], reverse=True
            )[0][1]

            for p_num, phase in phase_holders.items():
                if p_num in best_combo:
                    phase.turn_on()
                else:
                    phase.turn_off()

        for veh_id in libsumo.simulation.getDepartedIDList():
            libsumo.vehicle.subscribe(veh_id, _vehicle_subscriptions)

        sim_time += step_size

    libsumo.close()

    fuel_vec = np.array(fuel_vec)
    veh_vec = np.array(veh_vec)

    poly = get_polygon(config.crop_polygon)
    is_inside = is_inside_sm_parallel(fuel_vec[:, :2], poly)
    fuel_vec = fuel_vec[is_inside, -1]
    veh_vec = veh_vec[is_inside]
    num = np.unique(
        veh_vec,
    ).shape[0]
    # replace negative fuel consumption with 0
    fuel_vec[fuel_vec < 0] = 0
    config.average_fuel_consumption = float(fuel_vec.sum() / num)
