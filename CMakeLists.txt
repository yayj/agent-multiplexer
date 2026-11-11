cmake_minimum_required(VERSION 3.30.0 FATAL_ERROR)
project(agent-muliplexer VERSION 0.1.0 LANGUAGES CXX)

set(multiplexer am)

add_executable(${multiplexer} main.cpp)
set_target_properties(${multiplexer} PROPERTIES
  CXX_STANDARD 20
  CXX_STANDARD_REQUIRED ON
  CXX_EXTENSIONS OFF)
