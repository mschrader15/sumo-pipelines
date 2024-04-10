cmake_minimum_required(VERSION 3.4...3.18)

project(_sumo_pipelines)

set(CMAKE_CXX_STANDARD 17)

# Set the path to SUMO_HOME
set(SUMO_HOME "$ENV{SUMO_HOME}")
# set(libsumo_DIR "${SUMO_HOME}/bin")

# Find the Arrow package
find_package(Arrow REQUIRED)
find_library(SUMOCPP sumocpp)
find_package(pybind11 REQUIRED)

# Include directories
include_directories("${SUMO_HOME}/src")
# link the include directory relative to this file
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/cpp/include")

# Source files
set(SOURCES cpp/sumo_pipelines.cpp)

# Executable target add_executable(sumo_cpp ${SOURCES})
pybind11_add_module(_sumo_pipelines ${SOURCES})

# Link libraries
target_link_libraries(_sumo_pipelines PRIVATE "${SUMOCPP}")
target_link_libraries(_sumo_pipelines PRIVATE Arrow::arrow_shared)

# Set the RPATH
set_target_properties(PROPERTIES INSTALL_RPATH "${SUMO_HOME}/bin")

# EXAMPLE_VERSION_INFO is defined by setup.py and passed into the C++ code as a
# define (VERSION_INFO) here.
target_compile_definitions(_sumo_pipelines
                           PRIVATE VERSION_INFO=${EXAMPLE_VERSION_INFO})
