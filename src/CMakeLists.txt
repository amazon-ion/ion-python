cmake_minimum_required(VERSION 3.6 FATAL_ERROR)
project(ion)

find_package(Python3 REQUIRED COMPONENTS Development.Module)

set(IONC_BUILD_TESTS OFF CACHE BOOL "disable ionc test builds")
add_subdirectory(ion-c/ EXCLUDE_FROM_ALL)

Python3_add_library(_ioncmodule MODULE "ioncmodule.c")

if (MSVC)
   target_link_libraries(_ioncmodule PRIVATE objlib decNumber_static)
else()
   # We need to link against libm explicitly for *nix.
   target_link_libraries(_ioncmodule PRIVATE objlib decNumber_static m)
endif()

target_include_directories(_ioncmodule PUBLIC
   $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/ion-c/decNumber/include>
   $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/ion-c/ionc/include>
)

install(TARGETS _ioncmodule objlib
        EXCLUDE_FROM_ALL
        COMPONENT python_module
        DESTINATION ${PY_BUILD_CMAKE_MODULE_NAME})
