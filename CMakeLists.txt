cmake_minimum_required(VERSION 3.5)

find_package(IonC REQUIRED)
find_package(PythonLibs REQUIRED) # TODO this should be used when making public, not hardcoding paths below.

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O0")

# TODO just here because the extension doesn't support 2.7 (the system python version) yet.
#set(PYTHON_LIBRARIES /Users/greggt/.pyenv/versions/3.5.2/lib/libpython3.5m.a)
#set(PYTHON_INCLUDE_DIRS /Users/greggt/.pyenv/versions/3.5.2/include/python3.5m)

add_subdirectory(tests)
