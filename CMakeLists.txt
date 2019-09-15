cmake_minimum_required (VERSION 3.5)

project (ExternalSort)

find_package (Seastar REQUIRED)

add_executable (external-sort
        external-sort.cc utils.hh sorted_files_split_phase.hh sorted_files_split_phase.cc)

target_link_libraries (external-sort
        PRIVATE Seastar::seastar)