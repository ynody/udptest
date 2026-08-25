cmake_minimum_required(VERSION 3.20)

project(udpcomapp VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(ASIO_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/asio/include")
set(NLOHMANN_JSON_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/nlohmann_json/include")
set(SPDLOG_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/spdlog/include")

foreach(required_header
    "${ASIO_INCLUDE_DIR}/asio.hpp"
    "${NLOHMANN_JSON_INCLUDE_DIR}/nlohmann/json.hpp"
    "${SPDLOG_INCLUDE_DIR}/spdlog/spdlog.h")
  if(NOT EXISTS "${required_header}")
    message(FATAL_ERROR
      "Required dependency header not found: ${required_header}\n"
      "Run: cmake -P scripts/download_dependencies.cmake")
  endif()
endforeach()

find_package(Threads REQUIRED)

add_library(udpcomapp_options INTERFACE)
target_compile_features(udpcomapp_options INTERFACE cxx_std_20)
target_compile_definitions(udpcomapp_options INTERFACE ASIO_STANDALONE)
target_include_directories(udpcomapp_options INTERFACE
  "${ASIO_INCLUDE_DIR}"
  "${NLOHMANN_JSON_INCLUDE_DIR}"
  "${SPDLOG_INCLUDE_DIR}"
)
target_link_libraries(udpcomapp_options INTERFACE Threads::Threads)

if(MSVC)
  target_compile_options(udpcomapp_options INTERFACE /W4 /permissive-)
else()
  target_compile_options(udpcomapp_options INTERFACE -Wall -Wextra -Wpedantic)
endif()

if(WIN32)
  target_link_libraries(udpcomapp_options INTERFACE ws2_32 mswsock)
endif()

add_executable(udp_server src/udp_server.cpp)
target_link_libraries(udp_server PRIVATE udpcomapp_options)

add_executable(udp_sender src/udp_sender.cpp)
target_link_libraries(udp_sender PRIVATE udpcomapp_options)
