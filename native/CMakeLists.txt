cmake_minimum_required(VERSION 3.15)
cmake_policy(SET CMP0091 NEW)
cmake_policy(SET CMP0042 NEW)

project(timestar_compression)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# [H10 fix] Safe baseline arch for distribution; use x86-64-v2 (SSE4.2) not -march=native
# [M14 fix] Add LTO for cross-TU inlining of FFOR/ALP templates
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-O3)
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|amd64")
        include(CheckCXXCompilerFlag)
        check_cxx_compiler_flag(-march=x86-64-v2 HAS_X86_64_V2)
        if(HAS_X86_64_V2)
            add_compile_options(-march=x86-64-v2)
        else()
            add_compile_options(-msse4.2 -mpopcnt)
        endif()
    endif()
    # LTO for cross-TU inlining (opt-in, can cause issues with some gcc-ar setups)
    option(ENABLE_LTO "Enable Link-Time Optimization" ON)
    if(ENABLE_LTO)
        add_compile_options(-flto)
        add_link_options(-flto)
    endif()
elseif(MSVC)
    add_compile_options(/O2)
endif()

include_directories(${CMAKE_JS_INC})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# Homebrew paths for macOS
if(APPLE)
    list(APPEND CMAKE_PREFIX_PATH "/opt/homebrew" "/usr/local")
endif()

# Find zstd
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(ZSTD QUIET libzstd)
endif()
if(NOT ZSTD_FOUND)
    find_library(ZSTD_LIBRARIES NAMES zstd)
    find_path(ZSTD_INCLUDE_DIRS NAMES zstd.h)
endif()

if(ZSTD_LIBRARIES)
    message(STATUS "Found zstd: ${ZSTD_LIBRARIES}")
else()
    message(FATAL_ERROR "zstd not found - install libzstd-dev (apt) or zstd (brew)")
endif()

set(SOURCES
    src/addon.cpp
    src/ffor_encoder.cpp
    src/alp_encoder.cpp
    src/bool_rle_encoder.cpp
    src/string_encoder.cpp
)

add_library(${PROJECT_NAME} SHARED ${SOURCES} ${CMAKE_JS_SRC})

set_target_properties(${PROJECT_NAME} PROPERTIES PREFIX "" SUFFIX ".node")

target_include_directories(${PROJECT_NAME} PRIVATE
    ${CMAKE_JS_INC}
    ${ZSTD_INCLUDE_DIRS}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(${PROJECT_NAME} ${CMAKE_JS_LIB} ${ZSTD_LIBRARIES})

# node-addon-api headers
execute_process(
    COMMAND node -p "require('node-addon-api').include"
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE NODE_ADDON_API_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
string(REPLACE "\"" "" NODE_ADDON_API_DIR ${NODE_ADDON_API_DIR})
target_include_directories(${PROJECT_NAME} PRIVATE ${NODE_ADDON_API_DIR})

# [M15 fix] Do NOT disable C++ exceptions — our code throws std::runtime_error
# and node-addon-api will convert them to JS exceptions automatically.
target_compile_definitions(${PROJECT_NAME} PRIVATE NAPI_VERSION=8)
