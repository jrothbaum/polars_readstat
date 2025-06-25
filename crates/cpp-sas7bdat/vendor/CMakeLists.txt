cmake_minimum_required(VERSION 3.16)
set(CMAKE_VERBOSE_MAKEFILE ON)
#Set the project name
project(
  cppsas7bdat
  VERSION 1.0.1
  DESCRIPTION "A C++17 SAS7BDAT reader library"
  LANGUAGES CXX
  )

file(WRITE package.txt ${PROJECT_NAME})
file(WRITE version.txt ${PROJECT_VERSION})

include(cmake/StandardProjectSettings.cmake)
include(cmake/PreventInSourceBuilds.cmake)
 
# variable controlling whether the build is a shared or static library
if (WIN32)
  set(LIBRARY_TYPE STATIC CACHE STRING "type of library to make for lib_cppsas7bdat")
  set(Boost_USE_STATIC_LIBS OFF)
else()
  #set(LIBRARY_TYPE SHARED CACHE STRING "type of library to make for lib_cppsas7bdat")
  set(LIBRARY_TYPE STATIC CACHE STRING "type of library to make for lib_cppsas7bdat")
  set(Boost_USE_STATIC_LIBS OFF)
endif()

#

# Link this 'library' to set the c++ standard / compile-time options requested
add_library(project_options INTERFACE)
target_compile_features(project_options INTERFACE cxx_std_20)

set_target_properties(project_options PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  CXX_EXTENSIONS OFF
  CXX_STANDARD_REQUIRED ON)

if(CMAKE_CXX_COMPILER_ID MATCHES ".*Clang")
  option(ENABLE_BUILD_WITH_TIME_TRACE "Enable -ftime-trace to generate time tracing .json files on clang" OFF)
  if(ENABLE_BUILD_WITH_TIME_TRACE)
    target_compile_options(project_options INTERFACE -ftime-trace)
  endif()
endif()

# Link this 'library' to use the warnings specified in CompilerWarnings.cmake
add_library(project_warnings INTERFACE)

# enable cache system
include(cmake/Cache.cmake)

# Add linker configuration
include(cmake/Linker.cmake)
configure_linker(project_options)

# standard compiler warnings
include(cmake/CompilerWarnings.cmake)
set_project_warnings(project_warnings)

# sanitizer options if supported by compiler
include(cmake/Sanitizers.cmake)
enable_sanitizers(project_options)

# enable doxygen
include(cmake/Doxygen.cmake)
enable_doxygen()

# allow for static analysis options
include(cmake/StaticAnalyzers.cmake)

option(BUILD_SHARED_LIBS "Enable compilation of shared libraries" OFF)
option(ENABLE_TESTING "Enable Test Builds" ON)
option(ENABLE_FUZZING "Enable Fuzzing Builds" OFF)

if(ENABLE_TESTING)
  enable_testing()
  message("Building Tests. Be sure to check out test/constexpr_tests for constexpr testing")
  add_subdirectory(test)
endif()

add_subdirectory(src)

add_subdirectory(apps)

option(ENABLE_PYTHON "Enable building python module" OFF)
if(ENABLE_PYTHON)
  message("Building python module")
  add_subdirectory(python)
endif()

option(ENABLE_R "Enable build R module" OFF)
if(ENABLE_R)
  message("Building R module")
  add_subdirectory(R/CPPSAS7BDAT)
  add_subdirectory(R/CPPSAS7BDAT/src)
endif()
