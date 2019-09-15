#pragma once

inline constexpr const uint64_t FILE_BLOCK_SIZE = 4096;
inline constexpr const uint64_t TEMPORARY_FILE_SIZE = 2 * FILE_BLOCK_SIZE;

inline unsigned int current_shard() {
    return seastar::engine().cpu_id();
}

inline seastar::future<> close_file(seastar::file&& file) {
    return file.close().finally([file] {});
}
