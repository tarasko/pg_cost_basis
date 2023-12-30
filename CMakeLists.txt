cmake_minimum_required(VERSION 3.1)
project(pg_cost_basis)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(DEBUG_MEMORY "Log every allocations from postgres memory context" 0)

# Make sure FindPostgreSQL.cmake is in the module path.
list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

find_package(PostgreSQL REQUIRED)

if(DEBUG_MEMORY)
    add_definitions("-DDEBUG_MEMORY=1")
endif()

add_subdirectory(src)

