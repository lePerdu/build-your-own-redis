cmake_minimum_required(VERSION 3.21)
project(byor)

set(CMAKE_C_STANDARD 17)
set(CMAKE_C_STANDARD_REQUIRED true)
set(CMAKE_C_EXTENSIONS false)

add_library(common OBJECT
    server/buffer.c
    server/commands.c
    server/hashmap.c
    server/object.c
    server/protocol.c
    server/store.c
)
add_executable(byor $<TARGET_OBJECTS:common> server/server.c)

set(CMAKE_C_FLAGS "-Wall -Wextra")

set(ASAN_FLAGS "-fsanitize=address -fno-omit-frame-pointer")
set(CMAKE_C_FLAGS_DEBUG "-Og -g ${ASAN_FLAGS}")
set(CMAKE_LD_FLAGS_DEBUG "-g ${ASAN_FLAGS}")

enable_testing()

add_executable(unit-test
    $<TARGET_OBJECTS:common>
    server/test.c
)
add_test(NAME unit-test COMMAND unit-test)
