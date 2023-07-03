# (C) 2020 The University of Chicago
# See COPYRIGHT in top-level directory.
cmake_minimum_required (VERSION 3.8)
project (colza C CXX)
set (CMAKE_CXX_STANDARD 14)
enable_testing ()

add_definitions (-g -Wextra -Wall -Wpedantic)

option (ENABLE_TESTS    "Build tests" OFF)
option (ENABLE_EXAMPLES "Build examples" OFF)
option (ENABLE_BEDROCK  "Build bedrock module" ON)
option (ENABLE_DRC      "Enable Cray DRC when compiling examples" OFF)

#set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
#set (CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")

# add our cmake module directory to the path
set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
     "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# link shared lib with full rpath
set (CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
set (CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# setup cache variables for ccmake
if (NOT CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE Release
         CACHE STRING "Choose the type of build." FORCE)
    set_property (CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
                  "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif ()
set (CMAKE_PREFIX_PATH "" CACHE STRING "External dependencies path")
set (BUILD_SHARED_LIBS "ON" CACHE BOOL "Build a shared library")


# search for MPI
find_package (MPI REQUIRED)
# search for pkg-config
find_package (PkgConfig REQUIRED)
# search for thallium
find_package (thallium REQUIRED)
# search for mona
pkg_check_modules (MONA REQUIRED IMPORTED_TARGET mona)
# search for ssg
pkg_check_modules (SSG REQUIRED IMPORTED_TARGET ssg)
# search for nlohmann/json
find_package (nlohmann_json REQUIRED)
# search for TCLAP
find_package (TCLAP REQUIRED)
# search for spdlog
find_package(spdlog REQUIRED)
# search for bedrock if needed
if (${ENABLE_BEDROCK})
    find_package(bedrock REQUIRED)
endif ()
# search for DRC if needed
if (${ENABLE_DRC})
    find_package (DRC REQUIRED)
    add_definitions (-DCOLZA_ENABLE_DRC)
    include_directories (${DRC_INCLUDE_DIRS})
endif ()

# library version set here (e.g. for shared libs).
set (COLZA_VERSION_MAJOR 0)
set (COLZA_VERSION_MINOR 4)
set (COLZA_VERSION_PATCH 0)
set (COLZA_VERSION
     "${COLZA_VERSION_MAJOR}.${COLZA_VERSION_MINOR}.${COLZA_VERSION_PATCH}")

add_subdirectory (src)
if(${ENABLE_TESTS})
  pkg_check_modules (cppunit REQUIRED IMPORTED_TARGET cppunit)
  add_subdirectory (tests)
endif(${ENABLE_TESTS})
if(${ENABLE_EXAMPLES})
  add_subdirectory (examples)
endif(${ENABLE_EXAMPLES})
