cmake_minimum_required(VERSION 3.4...3.18)

project(_sumo_pipelines)

set(CMAKE_CXX_STANDARD 17)

option(ARROW_LINK_SHARED "Link to the Arrow shared library" ON)

# Set the path to SUMO_HOME
set(SUMO_HOME "$ENV{SUMO_HOME}")
set(libsumo_DIR "${SUMO_HOME}/bin")

# Find the Arrow package
add_subdirectory(extern/pybind11)

# Find the Arrow package and store the path to the shared library
find_package(Arrow REQUIRED )
find_package(Parquet REQUIRED)
find_library(SUMOCPP sumocpp HINTS "${libsumo_DIR}" REQUIRED)

# link the include directory relative to this file
include_directories("${SUMO_HOME}/src")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/cpp/include")

# print the ARROW shared library path
# message(STATUS "Arrow shared library path: ${ARROW_FULL_SO_VERSION}")

# Source files
set(SOURCES cpp/sumo_pipelines.cpp)

# Executable target add_executable(sumo_cpp ${SOURCES})
pybind11_add_module(_sumo_pipelines ${SOURCES})

# Link libraries
target_link_libraries(_sumo_pipelines PRIVATE "${SUMOCPP}")

if(ARROW_LINK_SHARED)
target_link_libraries(_sumo_pipelines PRIVATE Arrow::arrow_shared)
target_link_libraries(_sumo_pipelines PRIVATE Parquet::parquet_shared)
else()
target_link_libraries(_sumo_pipelines PRIVATE Arrow::arrow_static)
target_link_libraries(_sumo_pipelines PRIVATE Parquet::parquet_static)
endif()

# target_link_libraries(_sumo_pipelines PRIVATE "-Wl,--disable-new-dtags")

# udate the LD_LIBRARY_PATH
set_target_properties(_sumo_pipelines PROPERTIES INSTALL_RPATH "${SUMO_HOME}/bin" BUILD_WITH_INSTALL_RPATH TRUE)

# EXAMPLE_VERSION_INFO is defined by setup.py and passed into the C++ code as a
# define (VERSION_INFO) here.
target_compile_definitions(_sumo_pipelines
                           PRIVATE VERSION_INFO=${EXAMPLE_VERSION_INFO})

                           