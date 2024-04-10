#include <iostream>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/pretty_print.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <libsumo/libsumo.h>
#include <cmath>
#ifndef __INTELLISENSE__ // code that generates an error squiggle
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#endif

#include "include/libsumo_wrapper.h"


#define ARROW_WITH_ZSTD

using namespace libsumo;
#ifndef __INTELLISENSE__ // code that generates an error squiggle
namespace py = pybind11;
#endif


void traci_vehicle_state_runner(const std::vector<std::string>& simulation_start, int warmup_time, std::string& file_name) {

    Simulation::start(simulation_start);
    Simulation::step(warmup_time);

    auto parquet_writer = StreamWriter(file_name);

    auto t = warmup_time;
    const auto end_time = Simulation::getEndTime();

    while ((t < (end_time - 1)) && (Simulation::getMinExpectedNumber() > 0)) {
        Simulation::step();

        t = Simulation::getTime();

        for (const std::string& vehicle : Vehicle::getIDList()) {
            parquet_writer.writeRow(vehicle, t);
        }


    };
    Simulation::close();
}

#ifndef __INTELLISENSE__ // code that generates an error squiggle

PYBIND11_MODULE(sumo_pipelines, m) {
    m.def("traci_vehicle_state_runner", &traci_vehicle_state_runner, "Run SUMO simulation and collect vehicle states",
        py::arg("simulation_start"), py::arg("warmup_time"), py::arg("fcd_output_file"));
}

#endif
