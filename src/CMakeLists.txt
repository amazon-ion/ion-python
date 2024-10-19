cmake_minimum_required(VERSION 3.6 FATAL_ERROR)
project(ion)

find_package(Python3 REQUIRED COMPONENTS Development.Module)

set(IONC_BUILD_TESTS OFF)
add_subdirectory(ion-c/ EXCLUDE_FROM_ALL)

Python3_add_library(_ioncmodule MODULE "ioncmodule.c")
target_link_libraries(_ioncmodule PRIVATE objlib decNumber m)
target_include_directories(_ioncmodule PUBLIC
   $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/ion-c/decNumber/include>
   $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/ion-c/ionc/include>
)

install(TARGETS _ioncmodule
        EXCLUDE_FROM_ALL
        COMPONENT python_module
        DESTINATION ${PY_BUILD_CMAKE_MODULE_NAME})

# install(FILES _ioncmodule.pyi
#         EXCLUDE_FROM_ALL
#         COMPONENT python_module
#         DESTINATION ${PY_BUILD_CMAKE_MODULE_NAME})
