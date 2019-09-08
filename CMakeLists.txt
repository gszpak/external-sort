cmake_minimum_required (VERSION 3.5)

project (ExternalSort)

#include_directories(/home/gszpak/seastar/include)
#include_directories(/home/gszpak/seastar/include/seastar)

find_package (Seastar REQUIRED)

add_executable (external-sort
        external-sort.cc)

target_link_libraries (external-sort
        PRIVATE Seastar::seastar)