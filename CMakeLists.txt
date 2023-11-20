cmake_minimum_required(VERSION 3.6)

project(ion_python_c)

add_subdirectory(ion-c)
add_subdirectory(amazon/ion)

add_dependencies(ionc_ext ionc decNumber)
