#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/sstring.hh>
#include <iostream>
#include <queue>
#include <utility>
#include "utils.hh"
#include "sorted_files_split_phase.hh"

inline constexpr const uint64_t K_WAY_MERGE_BUFFER_SIZE = 2 * FILE_BLOCK_SIZE;
// type of heap containing pairs (block, index of origin file)
using block_with_index_type = std::pair<seastar::sstring, int>;
using blocks_min_heap = std::priority_queue<
    block_with_index_type, std::vector<block_with_index_type>, std::greater<>>;
using file_info_vector = std::vector<temporary_file_info>;
using files_vector = std::vector<seastar::file>;


seastar::sstring get_temp_file_name(temporary_file_info& file_info) {
    return "temp_out_" + seastar::to_sstring(file_info.temporary_file_num) + ".txt";
}

seastar::future<std::vector<seastar::future<seastar::file>>>
open_temporary_files(file_info_vector& temporary_files_info) {
    std::vector<seastar::future<seastar::file>> open_temporary_file_futures;
    for (auto&& temp_file_info : temporary_files_info) {
        open_temporary_file_futures.emplace_back(
            seastar::open_file_dma(get_temp_file_name(temp_file_info), seastar::open_flags::ro));
    }
    return seastar::when_all(open_temporary_file_futures.begin(), open_temporary_file_futures.end());
}

seastar::future<> init_heap(file_info_vector& file_infos, files_vector& temporary_files, blocks_min_heap& min_heap) {
    std::vector<seastar::future<seastar::temporary_buffer<char>>> read_from_files_futures;
    for (uint64_t i = 0; i < temporary_files.size(); ++i) {
        seastar::file file = temporary_files[i];
        uint64_t file_size = file_infos[i].size;
        read_from_files_futures.emplace_back(file.dma_read_exactly<char>(
            0, std::min(K_WAY_MERGE_BUFFER_SIZE, file_size)));
    }
    return seastar::when_all(read_from_files_futures.begin(), read_from_files_futures.end()).then(
        [&min_heap](std::vector<seastar::future<seastar::temporary_buffer<char>>> read_futures) {
            int idx = 0;
            for (auto&& read_future : read_futures) {
                seastar::temporary_buffer<char> buffer = std::get<0>(read_future.get());
                std::vector<seastar::sstring> blocks = get_blocks_vector(buffer);
                for (auto&& block : blocks) {
                    min_heap.push(std::make_pair(std::move(block), idx));
                }
                idx++;
            }
            return seastar::make_ready_future();
        }
    );
}

//seastar::future<seastar::sstring>
seastar::future<> merge_files(file_info_vector& file_infos, files_vector& temporary_files, seastar::file& out_file) {
    // heap containing pairs (block, index of origin file)
    return seastar::do_with(
        blocks_min_heap(),
        seastar::temporary_buffer<char>::aligned(out_file.memory_dma_alignment(), K_WAY_MERGE_BUFFER_SIZE),
        [&file_infos, &temporary_files, &out_file](auto& min_heap, auto& out_buffer) {
            return init_heap(file_infos, temporary_files, min_heap).then(
                [&file_infos, &temporary_files, &out_file, &min_heap, &out_buffer]() {
                    while (!min_heap.empty()) {
                        std::cout << current_shard() << ": " << min_heap.top().first << "," << min_heap.top().second << "\n";
                        min_heap.pop();
                    }
                    return seastar::make_ready_future();
                }
            );
        }
    );
}

seastar::future<> close_files(files_vector& temporary_files, seastar::file& out_file) {
    std::vector<seastar::future<>> futures;
    for (auto&& temporary_file : temporary_files) {
        futures.emplace_back(temporary_file.close());
    }
    futures.emplace_back(out_file.close());
    return seastar::when_all_succeed(futures.begin(), futures.end());
}

seastar::future<> k_way_merge(file_info_vector& temporary_files) {
    for (auto&& elem : temporary_files) {
        std::cout << current_shard() << ": " << elem.start_offset << " " << elem.size << " " << elem.temporary_file_num << "\n";
    }
    return open_temporary_files(temporary_files).then(
        [&temporary_files](std::vector<seastar::future<seastar::file>> open_files_futures) {
            files_vector open_files;
            for (auto&& open_file_future : open_files_futures) {
                open_files.emplace_back(std::get<0>(open_file_future.get()));
            }
            return seastar::do_with(std::move(open_files), [&temporary_files] (auto& open_files) {
                seastar::sstring out_file_name = "merged_" + seastar::to_sstring(current_shard()) + ".txt";
                seastar::open_flags flags = seastar::open_flags::wo |
                    seastar::open_flags::create |
                    seastar::open_flags::truncate;
                return seastar::open_file_dma(out_file_name, flags).then(
                    [&open_files, &temporary_files](seastar::file out_file) {
                        return seastar::do_with(std::move(out_file), [&open_files, &temporary_files](auto& out_file) {
                            return merge_files(temporary_files, open_files, out_file).then(
                                [&open_files, &out_file]() {
                                    return close_files(open_files, out_file);
                                }
                            );
                        });
                    }
                );
            });
        }
    );
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
                    std::cout << "Total memory: " << seastar::memory::stats().total_memory() << "\n";
                    std::cout << "Free memory: " << seastar::memory::stats().free_memory() << "\n";
//                    std::cout << seastar::smp::count << "\n";
                    return convert_file_to_temporary_sorted_files(std::move(input_file_name)).then(
                        [](file_info_vector&& temporary_files) {
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
