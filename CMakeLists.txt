cmake_minimum_required(VERSION 3.26)

project(armv8)

set(CMAKE_C_STANDARD 17)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -D_POSIX_SOURCE -D_DEFAULT_SOURCE -Wall -pedantic")

set(SRC_DIR "${CMAKE_SOURCE_DIR}/src")

add_subdirectory(src)
