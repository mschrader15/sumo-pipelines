# try to import LIBSUMO

try:
    import libsumo  # type: ignore  # noqa: PGH003

    LIBSUMO = True
except ImportError:
    import traci as libsumo

    LIBSUMO = False


import itertools

import polars as pl
import traci.constants as tc

from sumo_pipelines.blocks.simulation.functions import make_cmd
from sumo_pipelines.blocks.simulation_complex.config import (
    PriorityTrafficLightsRunnerConfig,
)
from sumo_pipelines.utils.nema_utils import NEMALight

# def run_sumo_delay(config, *args, **kwargs) -> None:

#     # this function should run sumo with libsumo and return the average delay


class PhaseHolder:
    def __init__(
        self,
        tl,
        phase,
        sim_step,
        e2_detector_ids,
        truck_waiting_time_factor=3,
        truck_speed_factor=6,
        truck_count_factor=6,
    ):
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

        self.truck_waiting_time_factor = truck_waiting_time_factor
        self.truck_speed_factor = truck_speed_factor
        self.truck_count_factor = truck_count_factor

        self.subscribe()
        self.get_e2s(e2_detector_ids)
        self._on = True
        self.turn_off()

    @property
    def on(self):
        return self._on

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
            self.veh_count += self.truck_count_factor * truck + 1
            self.veh_speed_factor += max(
                veh_subs[_id][tc.VAR_SPEED] * (self.truck_speed_factor * truck + 1), 0
            )

            wt = sim_time - self.accumulated_wtime_holder[(_id, truck)]
            if wt > 150:
                wt = 1e6

            self.accumulated_wtime += max(
                (wt) * (self.truck_waiting_time_factor * truck + 1),
                0,
            )


_vehicle_subscriptions = (
    tc.VAR_VEHICLECLASS,
    tc.VAR_SPEED,
    tc.VAR_POSITION,
    tc.VAR_FUELCONSUMPTION,
    tc.VAR_ACCELERATION,
    tc.VAR_LANE_ID,
    tc.VAR_EMISSIONCLASS,
    tc.VAR_TIMELOSS,
)

UPSTREAM_MAP = {
    2: {
        "63082004": None,
        "63082003": "63082004",
        "63082002": "63082003",
    },
    6: {
        "63082004": "63082003",
        "63082003": "63082002",
        "63082002": None,
    },
}


def traci_priority_light_control(
    config: PriorityTrafficLightsRunnerConfig, *args, **kwargs
) -> None:
    sumo_cmd = make_cmd(config=config)

    mainline_weights = (
        config.intersection_weights.mainline_a,
        config.intersection_weights.mainline_b,
        config.intersection_weights.mainline_c,
        config.intersection_weights.mainline_d,
        config.intersection_weights.mainline_e,
    )
    side_weights = (
        config.intersection_weights.side_a,
        config.intersection_weights.side_b,
        config.intersection_weights.side_c,
        config.intersection_weights.side_d,
    )

    # I don't like this but easiest way for the moment
    # PhaseHolder.TRUCK_WAITING_TIME_FACTOR = (
    #     config.intersection_weights.truck_waiting_time_factor
    # )
    # PhaseHolder.TRUCK_SPEED_FACTOR = config.intersection_weights.truck_speed_factor
    # PhaseHolder.TRUCK_COUNT_FACTOR = config.intersection_weights.truck_count_factor

    if config.simulation_output:
        f = open(config.simulation_output, "w")

    libsumo.start(sumo_cmd, stdout=f)

    libsumo.simulation.step(config.warmup_time)
    step_size = int(libsumo.simulation.getDeltaT() * 1000)

    e2_detectors = libsumo.lanearea.getIDList()

    lights = {}
    for junction, programID, file in config.controlled_intersections:
        nema_light = NEMALight.from_xml(xml=file, id=junction, programID=programID)
        lights[junction] = (
            [
                combo if combo[0] != combo[1] else (combo[0],)
                for combo in nema_light.get_valid_phase_combos()
            ],
            {
                p.name: PhaseHolder(
                    junction,
                    p.name,
                    step_size / 1000,
                    e2_detectors,
                    truck_waiting_time_factor=config.intersection_weights.truck_waiting_time_factor,
                    truck_speed_factor=config.intersection_weights.truck_speed_factor,
                    # truck_count_factor=config.intersection_weights.truck_count_factor,
                )
                for p in nema_light.get_phase_list()
            },
        )

        libsumo.trafficlight.setProgram(junction, programID)
        libsumo.trafficlight.subscribe(
            junction, [tc.TL_RED_YELLOW_GREEN_STATE, tc.VAR_NAME]
        )
        libsumo.trafficlight.getPhaseName("63082002")

    sim_time = int(libsumo.simulation.getTime() * 1000)
    end_time = int(config.end_time * 1000)
    action_step = config.action_step * step_size

    for veh_id in libsumo.vehicle.getIDList():
        libsumo.vehicle.subscribe(veh_id, _vehicle_subscriptions)

    fuel_vec = []
    veh_vec = []

    def upstream_factor(tl, phase, signal_subs):
        if (upstream_tl := UPSTREAM_MAP.get(phase.phase, {}).get(tl, None)) is not None:
            if phase.phase in signal_subs[upstream_tl]:
                return lights[upstream_tl][1][phase.phase].veh_speed_factor
        return 0

    all_phases = list(itertools.product(*(p[0] for p in lights.values())))
    tl_index = list(lights.keys())  # python dict keys hold order

    while sim_time < end_time:
        libsumo.simulation.step()

        e3_subs = libsumo.multientryexit.getAllSubscriptionResults()
        veh_subs = libsumo.vehicle.getAllSubscriptionResults()
        signal_subs = libsumo.trafficlight.getAllSubscriptionResults()

        for veh, veh_info in veh_subs.items():
            fuel_vec.append(
                [
                    veh,
                    sim_time / 1000,
                    veh_info[tc.VAR_SPEED],
                    veh_info[tc.VAR_ACCELERATION],
                    *veh_info[tc.VAR_POSITION],
                    veh_info[tc.VAR_FUELCONSUMPTION],
                    veh_info[tc.VAR_LANE_ID],
                    veh_info[tc.VAR_EMISSIONCLASS],
                    veh_info[tc.VAR_TIMELOSS],
                ]
            )
            veh_vec.append(veh)

        for _, phase_holders in lights.values():
            for phase in phase_holders.values():
                phase.update(e3_subs, veh_subs, sim_time / 1000)

                # if not phase.on and phase.phase in (2, 6):

                # print(
                #     f"tl: {_} phase: {phase.phase} speed: {phase.veh_speed_factor} wait: {phase.accumulated_wtime} count: {phase.veh_count}"
                # )

        if sim_time % action_step == 0:
            combo_scores = []

            for tl, (phase_combos, phase_holders) in lights.items():
                # priority = []
                combo_scores.append({})
                for combo in phase_combos:
                    if combo == (2, 6):
                        weights = mainline_weights
                    else:
                        weights = side_weights

                    combo_scores[-1][combo] = sum(
                        weights[0] * phase_holders[phase].veh_speed_factor
                        + weights[2] * phase_holders[phase].accumulated_wtime * 60
                        + weights[1] * 60
                        - weights[3] * (str(phase) not in signal_subs[tl][tc.VAR_NAME])
                        for phase in combo
                    )

            adjusted_scores = []
            for combo in all_phases:
                score = 0
                states = dict(zip(tl_index, combo))
                for tl_i, light_state in enumerate(combo):
                    score += combo_scores[tl_i][light_state]
                    # if light_state == (2, 6):
                    for phase in light_state:
                        score += mainline_weights[4] * upstream_factor(
                            tl_index[tl_i], lights[tl_index[tl_i]][1][phase], states
                        )
                adjusted_scores.append((combo, score))

            best_combo = max(adjusted_scores, key=lambda x: x[1])[0]

            for i, (_, phase_holders) in enumerate(lights.values()):
                for p_num, phase in phase_holders.items():
                    if p_num in best_combo[i]:
                        phase.turn_on()
                    else:
                        phase.turn_off()

        for veh_id in libsumo.simulation.getDepartedIDList():
            libsumo.vehicle.subscribe(veh_id, _vehicle_subscriptions)

        sim_time += step_size

    libsumo.close()

    # make a polars dataframe from veh_vec
    # save the dataframe to a parquet file
    df = pl.DataFrame(
        fuel_vec,
        schema={
            "id": pl.Utf8,
            "time": pl.Float64,
            "speed": pl.Float64,
            "accel": pl.Float64,
            "x": pl.Float64,
            "y": pl.Float64,
            "fuel": pl.Float64,
            "lane": pl.Utf8,
            "eclass": pl.Utf8,
            "time_loss": pl.Float64,
        },
    )

    df.write_parquet(config.fcd_output)
