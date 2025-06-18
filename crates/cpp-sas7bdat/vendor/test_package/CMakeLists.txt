cmake_minimum_required(VERSION 3.16)
set(CMAKE_VERBOSE_MAKEFILE ON)

project(PackageTest CXX)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

find_package("cppsas7bdat" REQUIRED)
find_package(spdlog)

add_executable(example example.cpp)
target_link_libraries(example PUBLIC cppsas7bdat::cppsas7bdat PRIVATE spdlog::spdlog
)
