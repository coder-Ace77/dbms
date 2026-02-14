cmake_minimum_required(VERSION 3.10)
project(DocDB)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

include_directories(${CMAKE_SOURCE_DIR}/src)

file(GLOB_RECURSE SOURCES "src/*.cpp")

add_executable(doc_db_engine ${SOURCES})