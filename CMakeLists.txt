cmake_minimum_required (VERSION 3.16.3)
project (ARIES)

find_package (Seastar REQUIRED)
find_package (Valgrind REQUIRED)

add_executable (store
  src/os/store/store_main.cc)

target_link_libraries (store
  Seastar::seastar)
