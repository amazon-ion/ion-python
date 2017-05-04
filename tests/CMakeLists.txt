include_directories(${PYTHON_INCLUDE_DIRS} ${IONC_INCLUDE_DIRS})
MESSAGE(STATUS ${IONC_INCLUDE_DIRS})
MESSAGE(STATUS ${IONC_LIBRARIES})
MESSAGE(STATUS ${PYTHON_LIBRARIES})
MESSAGE(STATUS ${PYTHON_INCLUDE_DIRS})
add_executable(test_extension
  ../amazon/ion/ioncmodule.c
  test_extension.c
)
set_property(TARGET test_extension PROPERTY C_STANDARD 99)
target_include_directories(test_extension PRIVATE ../amazon/ion)
target_link_libraries(test_extension ${PYTHON_LIBRARIES} ${IONC_LIBRARIES})
