cmake_minimum_required(VERSION 3.22)
# set(CMAKE_C_COMPILER               "/opt/intel/oneapi/compiler/latest/linux/bin/icx")
# set(CMAKE_CXX_COMPILER             "/opt/intel/oneapi/compiler/latest/linux/bin/icpx")
set(CMAKE_C_COMPILER               "/usr/bin/clang")
set(CMAKE_CXX_COMPILER             "/usr/bin/clang++")

project(KDSep)

set(CMAKE_CXX_STANDARD 17)

set(EXECUTABLE_OUTPUT_PATH ${KDSep_SOURCE_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${KDSep_SOURCE_DIR}/lib)

add_subdirectory(src)
