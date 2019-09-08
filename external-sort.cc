#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/sstring.hh>
#include <iostream>
#include <utility>
#include <stdexcept>
#include <boost/algorithm/string/join.hpp>

// TODO: fix name
#define BLK_SIZE 16
#define SHARD_BUFFER_SIZE 2 * BLK_SIZE

//struct block_info {
//    uint64_t start;
//    uint64_t end; // not inclusive
//    uint64_t block_num;
//public:
//    block_info(uint64_t start, uint64_t end, uint64_t block_num) noexcept :
//        start(start), end(end), block_num(block_num) { }
//};

//std::vector<block_info> get_file_chunks_to_read(ssize_t file_size) {
//    // TODO: move this check earlier
//    if (file_size % BLK_SIZE != 0) {
//        throw std::invalid_argument("File size is not a multiple of block size");
//    }
//    uint64_t num_of_parts_to_read = file_size / SHARD_BUFFER_SIZE + (file_size % SHARD_BUFFER_SIZE != 0);
//    uint64_t cpu_id = seastar::engine().cpu_id();
//    std::vector<block_info> blocks_to_read;
//    for (uint64_t block_num = 0; block_num < num_of_parts_to_read; ++block_num) {
//        //blocks_to_read.emplace_back(block_num * SHARD_BUFFER_SIZE, block_num * (SHARD_BUFFER_SIZE));
//    }
//}

inline unsigned int current_shard() {
    return seastar::engine().cpu_id();
}

seastar::future<> write_partial_file(std::vector<seastar::sstring> &&blocks_vector) {
    seastar::sstring out_file_name = "temp_out_" + seastar::to_sstring(current_shard()) + ".txt";
    seastar::open_flags flags = seastar::open_flags::wo | seastar::open_flags::create | seastar::open_flags::truncate;
    return seastar::open_file_dma(out_file_name, flags).then(
        [blocks_vector = std::move(blocks_vector)](seastar::file output_file) {
            uint64_t alignment = output_file.memory_dma_alignment();
            seastar::temporary_buffer<char> out_buffer = seastar::temporary_buffer<char>::aligned(
                alignment, alignment);
            // TODO: remove
            if (current_shard() == 1) {
                std::cout << "Before copy: " << seastar::sstring(out_buffer.get(), out_buffer.size()) << "\n";
            }
            seastar::sstring joined = boost::algorithm::join(blocks_vector, "");
            if (current_shard() == 1) {
                std::cout << "Joined: " << joined << "\n";
            }
            std::copy(joined.begin(), joined.end(), out_buffer.get_write());
            if (current_shard() == 1) {
                std::cout << "After copy: " << seastar::sstring(out_buffer.get(), out_buffer.size()) << "\n";
            }
            return output_file.dma_write(0, out_buffer.get(), out_buffer.size()).then(
                [output_file = std::move(output_file)](ssize_t unused) mutable {
                    std::cout << current_shard() << " finished writing\n";
                    return output_file.close().finally([output_file] {});
                }
            );
        }
    );
}

seastar::future<> read_and_sort_buffer(seastar::file&& input_file, uint64_t file_size) {
    return input_file.dma_read_exactly<char>(current_shard() * SHARD_BUFFER_SIZE, SHARD_BUFFER_SIZE).then(
        [input_file = std::move(input_file)](const seastar::temporary_buffer<char>& buffer) {
            std::cout << current_shard() << " read " << buffer.size() << " bytes\n";
            std::vector<seastar::sstring> blocks_vector;
            for (uint64_t i = 0; i < buffer.size(); i += BLK_SIZE) {
                seastar::sstring block(buffer.get() + i * sizeof(char), BLK_SIZE);
                blocks_vector.emplace_back(std::move(block));
            }
            std::sort(blocks_vector.begin(), blocks_vector.end());
            return write_partial_file(std::move(blocks_vector));
        }
    );
}

seastar::future<> sort_file_blocks(seastar::sstring &&input_file_name) {
    return seastar::open_file_dma(input_file_name, seastar::open_flags::ro).then(
        [](seastar::file input_file) {
            return input_file.size().then(
                [input_file = std::move(input_file)](uint64_t file_size) mutable {
                    std::cout << "Size: " << file_size << "\n";
                    return read_and_sort_buffer(std::move(input_file), file_size);
                }
            );
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
                    auto &configuration = app.configuration();
                    auto input_file_name = configuration["input-file"].as<seastar::sstring>();
                    int cpu_id = seastar::engine().cpu_id();
                    std::cout << "cpu_id: " << cpu_id << "\n";
                    std::cout << "Free memory: " << seastar::memory::stats().free_memory() << "\n";
                    std::cout << "Hello world!" << "\n";
                    std::cout << seastar::smp::count << "\n";
                    return sort_file_blocks(std::move(input_file_name));
                });
            });
    });
}
