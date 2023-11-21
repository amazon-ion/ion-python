cmake_minimum_required(VERSION 3.26)

include(FindPython3)

find_package(Python3 COMPONENTS Development)

Python3_ADD_LIBRARY(ionc_ext MODULE WITH_SOABI ioncmodule.c)

target_link_libraries(ionc_ext PUBLIC ionc decNumber)
