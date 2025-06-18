configure_file(
  "${PROJECT_SOURCE_DIR}/include/cppsas7bdat/version.hpp.in"
  "${PROJECT_BINARY_DIR}/cppsas7bdat/version.hpp"
  )

find_package(fmt REQUIRED)
find_package(spdlog REQUIRED)
find_package(Boost REQUIRED COMPONENTS date_time)
message(STATUS "Boost version: ${Boost_VERSION}")
message(STATUS "BOOST LIBRARIES LOCATION: " ${Boost_LIBRARIES})
#include_directories( ${Boost_INCLUDE_DIR} )

# Check for Arrow and set up conditional compilation
find_package(Arrow QUIET)
message(STATUS "After find_package(Arrow QUIET):")
message(STATUS "  Arrow_FOUND: ${Arrow_FOUND}")
message(STATUS "  Arrow_DIR: ${Arrow_DIR}")
message(STATUS "  Arrow_VERSION: ${Arrow_VERSION}")

# Try different target names that Arrow might use
if(TARGET Arrow::arrow_shared)
    message(STATUS "  Target Arrow::arrow_shared: EXISTS")
elseif(TARGET arrow_shared)
    message(STATUS "  Target arrow_shared: EXISTS (no namespace)")
elseif(TARGET Arrow::arrow_static)
    message(STATUS "  Target Arrow::arrow_static: EXISTS")
else()
    message(STATUS "  No Arrow targets found")
    
    # Try to find it more verbosely
    message(STATUS "Trying verbose Arrow search...")
    find_package(Arrow REQUIRED)  # This will show the actual error
endif()

if(Arrow_FOUND AND (TARGET Arrow::arrow_shared OR TARGET Arrow::arrow_static))
    message(STATUS "Arrow found - enabling Arrow support")
    set(ARROW_SOURCES arrow_ffi.cpp arrow_sink.cpp)
    set(ARROW_DEFINITIONS CPPSAS7BDAT_HAVE_ARROW)
    
    if(TARGET Arrow::arrow_shared)
        set(ARROW_TARGET Arrow::arrow_shared)
    elseif(TARGET Arrow::arrow_static)
        set(ARROW_TARGET Arrow::arrow_static)
    endif()
    message(STATUS "   Arrow version: ${Arrow_VERSION}")
    set(ARROW_SOURCES arrow_ffi.cpp)
    set(ARROW_DEFINITIONS CPPSAS7BDAT_HAVE_ARROW)
    set(ARROW_INTERFACE_LIBRARIES ${ARROW_TARGET})
    set(ARROW_PRIVATE_LIBRARIES ${ARROW_TARGET})
else()
    message(STATUS "Arrow not found - disabling Arrow support")
    set(ARROW_SOURCES "")
    set(ARROW_DEFINITIONS "")
    set(ARROW_INTERFACE_LIBRARIES "")
    set(ARROW_PRIVATE_LIBRARIES "")
endif()


add_library(cppsas7bdat ${LIBRARY_TYPE}
  buffer.hpp
  column.cpp
  datasource_ifstream.cpp
  decompressors.hpp
  encodings.cpp
  encodings.hpp
  exceptions.hpp
  header.hpp
  metadata.hpp
  reader.cpp
  stream.hpp
  types.hpp
  data.hpp
  debug.hpp
  exceptions.cpp
  formatters.hpp
  memory.hpp
  page.hpp
  sas7bdat-impl.hpp
  subheaders.hpp
  version.cpp
  types.cpp
  stream.hpp
  stream.cpp
  ${ARROW_SOURCES}
    )

target_compile_definitions(cppsas7bdat PRIVATE BOOST_DATE_TIME_NO_LIB ${ARROW_DEFINITIONS})

target_include_directories(cppsas7bdat INTERFACE $<INSTALL_INTERFACE:include> $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>)
target_include_directories(cppsas7bdat INTERFACE ${PROJECT_BINARY_DIR} )
target_include_directories(cppsas7bdat PUBLIC $<INSTALL_INTERFACE:include> $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>)
target_include_directories(cppsas7bdat PUBLIC ${PROJECT_BINARY_DIR} )
# Updated target_link_libraries section in your CMakeLists.txt
target_link_libraries(cppsas7bdat
  PUBLIC
    fmt::fmt
    spdlog::spdlog
    Boost::date_time
    ${ARROW_INTERFACE_LIBRARIES}
  PRIVATE
    project_options
    project_warnings
    ${ARROW_PRIVATE_LIBRARIES}
  )

# If you want to bundle everything into a single static library for Rust FFI,
# you might want to consider creating a separate target:
add_library(cppsas7bdat_bundled STATIC
  $<TARGET_OBJECTS:cppsas7bdat>
)

# Link all dependencies statically into the bundled library
target_link_libraries(cppsas7bdat_bundled
  PUBLIC
    fmt::fmt
    spdlog::spdlog
    Boost::date_time
    ${ARROW_INTERFACE_LIBRARIES}
)

include(GNUInstallDirs)

install(TARGETS cppsas7bdat
        DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
install(
  DIRECTORY ${PROJECT_SOURCE_DIR}/include/
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
  COMPONENT Devel
  FILES_MATCHING PATTERN "*.hpp"
  )
install(
  FILES "${PROJECT_BINARY_DIR}/cppsas7bdat/version.hpp"
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/cppsas7bdat
  COMPONENT Devel
  )

if( ipo_supported )
    set_property(TARGET cppsas7bdat PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()
