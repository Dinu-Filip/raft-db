cmake_minimum_required(VERSION 3.26)

file(GLOB_RECURSE SRC_FILES "${SRC_DIR}/lib/*.c")
file(GLOB_RECURSE HEADER_FILES "${SRC_DIR}/lib/*.h")

add_library(lib STATIC ${SRC_FILES} ${HEADER_FILES})

target_include_directories(lib PRIVATE "${SRC_DIR}/lib")
