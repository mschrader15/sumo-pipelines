#include <iostream>
#include <libsumo/libsumo.h>
#include <cmath>
#ifndef __INTELLISENSE__ // code that generates an error squiggle
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#endif

#include "include/libsumo_wrapper.h"


using namespace libsumo;
#ifndef __INTELLISENSE__ // code that generates an error squiggle
namespace py = pybind11;
#endif


void traci_vehicle_state_runner(const std::vector<std::string>& simulation_start, int warmup_time, std::string& file_name) {

    Simulation::start(simulation_start);
    Simulation::step(warmup_time);

    auto parquet_writer = StreamWriter(file_name);

    double t = static_cast<double>(warmup_time);
    const auto end_time = Simulation::getEndTime();

    while (t < (end_time - 1)) {
        Simulation::step();

        t = Simulation::getTime();

        const auto collision_ids = Simulation::getCollidingVehiclesIDList();

        for (const std::string& vehicle : Vehicle::getIDList()) {
            bool collision = false;
            if (std::find(collision_ids.begin(), collision_ids.end(), vehicle) != collision_ids.end()) {
                collision = true;
            }
            parquet_writer.writeRow(vehicle, t, collision);
        }


    };
    Simulation::close();
}

#ifndef __INTELLISENSE__ // code that generates an error squiggle

PYBIND11_MODULE(_sumo_pipelines, m) {
    m.def("traci_vehicle_state_runner", &traci_vehicle_state_runner, "Run SUMO simulation and collect vehicle states",
        py::arg("simulation_start"), py::arg("warmup_time"), py::arg("fcd_output_file"));
}

#endif
