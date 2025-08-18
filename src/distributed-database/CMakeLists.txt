cmake_minimum_required(VERSION 3.26)

set(DISTRIBUTED_DATABASE_DIR "${SRC_DIR}/distributed-database")

file(GLOB_RECURSE MAIN_SRC_FILES "${DISTRIBUTED_DATABASE_DIR}/*.c")
list(FILTER MAIN_SRC_FILES EXCLUDE REGEX ".*test.c")

file(GLOB_RECURSE HEADER_FILES "${DISTRIBUTED_DATABASE_DIR}/*.h")

add_executable(databasenode ${MAIN_SRC_FILES} ${HEADER_FILES})

target_include_directories(databasenode PRIVATE "${SRC_DIR}/lib")
target_link_libraries(databasenode PRIVATE lib m)

target_include_directories(databasenode PRIVATE "${DISTRIBUTED_DATABASE_DIR}")

file(GLOB_RECURSE NETWORKING_TEST_SRC_FILES "${DISTRIBUTED_DATABASE_DIR}/*.c")
list(FILTER NETWORKING_TEST_SRC_FILES EXCLUDE REGEX "main.c|execute.c|.*test.c")

add_executable(networkingtest ${NETWORKING_TEST_SRC_FILES} ${HEADER_FILES} networking/networking-test.c)

target_include_directories(networkingtest PRIVATE "${SRC_DIR}/lib")
target_link_libraries(networkingtest PRIVATE lib m)

target_include_directories(networkingtest PRIVATE "${DISTRIBUTED_DATABASE_DIR}")

file(GLOB_RECURSE TEST_SRC_FILES "${DISTRIBUTED_DATABASE_DIR}/*.c")
list(FILTER TEST_SRC_FILES EXCLUDE REGEX "main.c|.*-test.c")

add_executable(dbtest ${TEST_SRC_FILES} ${HEADER_FILES})

target_include_directories(dbtest PRIVATE "${SRC_DIR}/lib")
target_link_libraries(dbtest PRIVATE lib m)

target_include_directories(dbtest PRIVATE "${DISTRIBUTED_DATABASE_DIR}")

