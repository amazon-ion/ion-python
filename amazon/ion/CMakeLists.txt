cmake_minimum_required(VERSION 3.6 FATAL_ERROR)

project(ionc_ext)

find_package(Python REQUIRED Development)
include_directories(${PYTHON_INCLUDE_DIRS})

set(BUILD_SHARED_LIBS  ON)
add_library(ionc_ext SHARED ioncmodule.c)
set_target_properties(ionc_ext PROPERTIES OUTPUT_NAME "better_name")

target_link_libraries(ionc_ext PUBLIC ionc decNumber)
target_link_libraries(ionc_ext PRIVATE Python::Python)
