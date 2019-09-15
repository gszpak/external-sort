#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/sstring.hh>
#include <iostream>
#include "utils.hh"
#include "sorted_files_split_phase.hh"

seastar::future<> k_way_merge(std::vector<temporary_file_info>& temporary_files) {
    for (auto&& elem : temporary_files) {
        std::cout << current_shard() << ": " << elem.start_offset << " " << elem.size << " " << elem.temporary_file_num << "\n";
    }
    return seastar::make_ready_future();
}

int main(int argc, char **argv) {
    namespace bpo = boost::program_options;
    seastar::app_template app;
    app.add_options()
        ("input-file,i", bpo::value<seastar::sstring>()->required(), "Input file path")
        ("output-file,o", bpo::value<seastar::sstring>()->required(), "Output file path")
        ("temp-dir,td", bpo::value<seastar::sstring>()->default_value("/tmp/external-sort"),
         "Directory to store temporary files");
    app.run(argc, argv, [&] {
        return seastar::parallel_for_each(
            boost::irange<unsigned>(0, seastar::smp::count),
            [&](unsigned c) {
                return seastar::smp::submit_to(c, [&]() {
                    auto& configuration = app.configuration();
                    auto input_file_name = configuration["input-file"].as<seastar::sstring>();
//                    std::cout << "Free memory: " << seastar::memory::stats().free_memory() << "\n";
//                    std::cout << seastar::smp::count << "\n";
                    return convert_file_to_temporary_sorted_files(std::move(input_file_name)).then(
                        [](std::vector<temporary_file_info>&& temporary_files) {
                            return seastar::do_with(
                                std::move(temporary_files), [] (auto& temporary_files) {
                                    return k_way_merge(temporary_files);
                                }
                            );
                        }
                    );
                });
            }
        );
    });
}
