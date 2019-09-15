#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sstring.hh>
#include <iostream>
#include <utility>
#include <stdexcept>
#include <boost/algorithm/string/join.hpp>
#include "utils.hh"
#include "sorted_files_split_phase.hh"


std::vector<temporary_file_info> get_temporary_files_info(ssize_t file_size) {
    uint64_t num_of_temporary_files = file_size / TEMPORARY_FILE_SIZE + (file_size % TEMPORARY_FILE_SIZE != 0);
    std::vector<temporary_file_info> blocks_to_read;
    for (uint64_t temporary_file_num = 0; temporary_file_num < num_of_temporary_files; ++temporary_file_num) {
        if (temporary_file_num % seastar::smp::count == current_shard()) {
            uint64_t bytes_to_read = std::min(TEMPORARY_FILE_SIZE, file_size - temporary_file_num * TEMPORARY_FILE_SIZE);
            blocks_to_read.emplace_back(
                temporary_file_num * TEMPORARY_FILE_SIZE, bytes_to_read, temporary_file_num);
        }
    }
    return blocks_to_read;
}

seastar::future<> write_partial_file(
    std::vector<seastar::sstring>&& blocks_vector, temporary_file_info& temporary_file_info) {
    seastar::sstring out_file_name = "temp_out_" + seastar::to_sstring(temporary_file_info.temporary_file_num) + ".txt";
    seastar::open_flags flags = seastar::open_flags::wo | seastar::open_flags::create | seastar::open_flags::truncate;
    return seastar::open_file_dma(out_file_name, flags).then(
        [&temporary_file_info, blocks_vector = std::move(blocks_vector)](seastar::file output_file) {
            seastar::temporary_buffer<char> out_buffer = seastar::temporary_buffer<char>::aligned(
                output_file.memory_dma_alignment(), temporary_file_info.size);
            // TODO: remove
//            if (current_shard() == 1) {
//                std::cout << "Before copy: " << seastar::sstring(out_buffer.get(), out_buffer.size()) << "\n";
//            }
            // TODO: remove join
            seastar::sstring joined = boost::algorithm::join(blocks_vector, "");
//            if (current_shard() == 1) {
//                std::cout << "Joined: " << joined << "\n";
//            }
            std::copy(joined.begin(), joined.end(), out_buffer.get_write());
//            if (current_shard() == 1) {
//                std::cout << "After copy: " << seastar::sstring(out_buffer.get(), out_buffer.size()) << "\n";
//            }
            return output_file.dma_write(0, out_buffer.get(), out_buffer.size()).then(
                [output_file = std::move(output_file), out_buffer = std::move(out_buffer)](ssize_t unused) mutable {
                    return close_file(std::move(output_file));
                }
            );
        }
    );
}

seastar::future<> create_sorted_temporary_file(seastar::file& input_file, temporary_file_info& temporary_file_info) {
    return input_file.dma_read_exactly<char>(temporary_file_info.start_offset, temporary_file_info.size).then(
        [&temporary_file_info](const seastar::temporary_buffer<char>& buffer) {
            std::cout << current_shard() << " read " << buffer.size() << " bytes\n";
            std::vector<seastar::sstring> blocks_vector = get_blocks_vector(buffer);
            std::sort(blocks_vector.begin(), blocks_vector.end());
            return write_partial_file(std::move(blocks_vector), temporary_file_info);
        }
    );
}

seastar::future<> create_sorted_temporary_files(
    seastar::file& input_file, std::vector<temporary_file_info>& temporary_files_info) {
    std::vector<seastar::future<>> create_temporary_file_futures;
    for (auto&& temp_file_info : temporary_files_info) {
        std::cout << temp_file_info.start_offset << " " << temp_file_info.size << " " << temp_file_info.temporary_file_num << "\n";
        seastar::future<> temporary_file_future = seastar::do_with(
            // copy temp_file_info to reuse the vector later
            temporary_file_info(
                temp_file_info.start_offset, temp_file_info.size, temp_file_info.temporary_file_num),
            [&input_file](auto& temporary_file_info) {
                return create_sorted_temporary_file(input_file, temporary_file_info);
            }
        );
        create_temporary_file_futures.emplace_back(std::move(temporary_file_future));
    }
    return seastar::when_all_succeed(create_temporary_file_futures.begin(), create_temporary_file_futures.end());
}

seastar::future<std::vector<temporary_file_info>>
convert_file_to_temporary_sorted_files(seastar::sstring&& input_file_name) {
    return seastar::open_file_dma(input_file_name, seastar::open_flags::ro).then(
        [](seastar::file input_file) {
            return seastar::do_with(std::move(input_file), [](auto& input_file) {
                return input_file.size().then(
                    [&input_file](uint64_t file_size) {
                        if (file_size % FILE_BLOCK_SIZE != 0) {
                            throw std::invalid_argument("File size is not a multiple of block size");
                        }
                        return seastar::do_with(
                            get_temporary_files_info(file_size), [&input_file] (auto& temporary_files_info) {
                                return create_sorted_temporary_files(input_file, temporary_files_info).then(
                                    [&input_file]() {
                                        return input_file.close();
                                    }
                                ).then(
                                    [&temporary_files_info]() {
                                        return seastar::make_ready_future<
                                            std::vector<temporary_file_info>>(temporary_files_info);
                                    }
                                );
                            }
                        );
                    }
                );
            });
        }
    );
}
