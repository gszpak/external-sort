#pragma once

inline constexpr const uint64_t FILE_BLOCK_SIZE = 4096;
inline constexpr const uint64_t TEMPORARY_FILE_SIZE = 2 * FILE_BLOCK_SIZE;

inline unsigned int current_shard() {
    return seastar::engine().cpu_id();
}

inline seastar::future<> close_file(seastar::file&& file) {
    return file.close().finally([file] {});
}

//TODO: remove inline
inline std::vector<seastar::sstring> get_blocks_vector(const seastar::temporary_buffer<char>& buffer) {
    std::vector<seastar::sstring> blocks_vector;
    for (uint64_t i = 0; i < buffer.size(); i += FILE_BLOCK_SIZE) {
        seastar::sstring block(buffer.get() + i * sizeof(char), FILE_BLOCK_SIZE);
        blocks_vector.emplace_back(std::move(block));
    }
    return blocks_vector;
}