cmake_minimum_required(VERSION 3.30.0 FATAL_ERROR)
project(agent-muliplexer VERSION 0.1.0 LANGUAGES CXX)

cmake_policy(SET CMP0167 NEW)

find_package(Boost REQUIRED COMPONENTS program_options REQUIRED)
find_package(range-v3 REQUIRED)

set(multiplexer am)

add_executable(${multiplexer} main.cpp)
target_compile_definitions(${multiplexer} PRIVATE META_NO_STD_FORWARD_DECLARATIONS)
set_target_properties(${multiplexer} PROPERTIES
  CXX_STANDARD 23
  CXX_STANDARD_REQUIRED ON
  CXX_EXTENSIONS OFF)
target_link_libraries(${multiplexer} PRIVATE
  Boost::boost
  Boost::program_options
  range-v3::range-v3)
