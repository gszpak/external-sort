#pragma once

struct temporary_file_info {
    uint64_t start_offset; // data offset in original file
    uint64_t size;
    uint64_t temporary_file_num;
    
    temporary_file_info(uint64_t start, uint64_t end, uint64_t block_num) noexcept :
        start_offset(start), size(end), temporary_file_num(block_num) {}
};

seastar::future<std::vector<temporary_file_info>>
convert_file_to_temporary_sorted_files(seastar::sstring&& input_file_name);

