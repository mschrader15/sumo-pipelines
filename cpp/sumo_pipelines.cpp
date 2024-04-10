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


void traci_vehicle_state_runner(const std::vector<std::string>& simulation_start, int warmup_time, float_t step_length, float_t end_time, std::string& file_name) {

    Simulation::start(simulation_start);
    Simulation::step(warmup_time);

    int t = static_cast<int>(warmup_time * 1000);
    int step_size = static_cast<int>(step_length * 1000);
    int int_end_time = static_cast<int>(end_time * 1000);

    auto parquet_writer = StreamWriter(file_name);

    while (t < int_end_time) {
        libsumo::Simulation::step();

        for (const std::string& vehicle : libsumo::Vehicle::getIDList()) {
            parquet_writer.writeRow(vehicle, t);
        }

        t += step_size;
    }

    libsumo::Simulation::close();
}

#ifndef __INTELLISENSE__ // code that generates an error squiggle

PYBIND11_MODULE(sumo_pipelines, m) {
    m.def("traci_vehicle_state_runner", &traci_vehicle_state_runner, "Run SUMO simulation and collect vehicle states",
        py::arg("simulation_start"), py::arg("warmup_time"), py::arg("step_length"), py::arg("end_time"), py::arg("fcd_output_file"));
}

#endif
