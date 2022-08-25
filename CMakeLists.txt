cmake_minimum_required (VERSION 3.16.3)
project (ARIES)

find_package (Seastar REQUIRED)

add_executable (store
  store.cc)

target_link_libraries (store
  Seastar::seastar)
