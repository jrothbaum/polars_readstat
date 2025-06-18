// src/rs/chunked_reader.cpp
#include "chunked_reader.hpp"
#include <stdexcept>
#include <cstring>

namespace cppsas7bdat {

// ChunkData implementations
ChunkData::ChunkData(size_t chunk_size) 
    : start_row(0), end_row(0), is_complete(false) {
    // Pre-allocate only what we need - raw row data
    row_buffers.reserve(chunk_size);
}

ChunkData::ChunkData(ChunkData&& other) noexcept 
    : row_buffers(std::move(other.row_buffers))
    , start_row(other.start_row)
    , end_row(other.end_row)
    , is_complete(other.is_complete) {
}

ChunkData& ChunkData::operator=(ChunkData&& other) noexcept {
    if (this != &other) {
        row_buffers = std::move(other.row_buffers);
        start_row = other.start_row;
        end_row = other.end_row;
        is_complete = other.is_complete;
    }
    return *this;
}

void ChunkData::clear() {
    row_buffers.clear();
    start_row = 0;
    end_row = 0;
    is_complete = false;
}

bool ChunkData::is_full(size_t target_size) const {
    return row_buffers.size() >= target_size;
}

size_t ChunkData::row_count() const {
    return row_buffers.size();
}

const void* ChunkData::get_row_buffer(size_t row_index) const {
    if (row_index < row_buffers.size() && !row_buffers[row_index].empty()) {
        return row_buffers[row_index].data();
    }
    return nullptr;
}

// ChunkSink implementation
ChunkSink::ChunkSink(size_t chunk_size) 
    : chunk_size_(chunk_size)
    , current_chunk_(chunk_size)
    , properties_(nullptr)
    , finished_(false)
    , row_buffer_size_(0) {
}

void ChunkSink::set_properties(const Properties& properties) {
    properties_ = &properties;
    // Cache the buffer size calculation once
    row_buffer_size_ = calculate_row_buffer_size();
}

size_t ChunkSink::calculate_row_buffer_size() const {
    if (!properties_) return 0;
    
    size_t total_size = 0;
    for (const auto& col : properties_->columns) {
        total_size += col.length();
    }
    return total_size;
}

void ChunkSink::push_row(size_t row_index, Column::PBUF row_data) {
    // Initialize chunk start row only once
    if (current_chunk_.row_buffers.empty()) {
        current_chunk_.start_row = row_index;
    }
    
    // Store only the raw row data - no column metadata copying
    if (row_data && row_buffer_size_ > 0) {
        // Single allocation and copy operation
        current_chunk_.row_buffers.emplace_back(
            static_cast<const uint8_t*>(row_data), 
            static_cast<const uint8_t*>(row_data) + row_buffer_size_
        );
    } else {
        // Empty row buffer for missing data
        current_chunk_.row_buffers.emplace_back();
    }
    
    current_chunk_.end_row = row_index;
    
    // Check if chunk is full and move to completed chunks
    if (current_chunk_.is_full(chunk_size_)) {
        current_chunk_.is_complete = true;
        completed_chunks_.push(std::move(current_chunk_));
        current_chunk_ = ChunkData(chunk_size_);
    }
}

void ChunkSink::end_of_data() {
    // Push any remaining data as final chunk
    if (!current_chunk_.row_buffers.empty()) {
        current_chunk_.is_complete = true;
        completed_chunks_.push(std::move(current_chunk_));
        current_chunk_ = ChunkData(chunk_size_);
    }
    finished_ = true;
}

bool ChunkSink::has_chunk() const {
    return !completed_chunks_.empty();
}

ChunkData ChunkSink::get_next_chunk() {
    if (completed_chunks_.empty()) {
        return ChunkData(); // Empty chunk
    }
    
    ChunkData chunk = std::move(completed_chunks_.front());
    completed_chunks_.pop();
    return chunk;
}

bool ChunkSink::is_finished() const {
    return finished_;
}

const Properties& ChunkSink::properties() const {
    if (!properties_) {
        throw std::runtime_error("Properties not set");
    }
    return *properties_;
}

// ChunkedReader implementation
ChunkedReader::ChunkedReader(const std::string& filename, size_t chunk_size)
    : chunk_sink_(std::make_unique<ChunkSink>(chunk_size))
    , chunk_size_(chunk_size)
    , initialized_(false) {
    
    try {
        // Create the ifstream datasource
        auto datasource = cppsas7bdat::datasource::ifstream(filename.c_str());

        // Create the reader with the datasource and sink
        reader_ = std::make_unique<cppsas7bdat::Reader>(
            std::move(datasource),
            *chunk_sink_
        );
        
        // Initialize properties early
        if (reader_) {
            chunk_sink_->set_properties(reader_->properties());
        }
        
        initialized_ = true;
    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("Failed to create SAS reader: ") + e.what());
    }
}

bool ChunkedReader::read_next_chunk() {
    if (!initialized_) {
        throw std::runtime_error("Reader not properly initialized");
    }
    
    if (chunk_sink_->is_finished()) {
        return false; // No more data
    }
    
    try {
        // Read exactly chunk_size rows for better predictability
        bool has_more_data = reader_->read_rows(chunk_size_);
        
        // Return true if we read some data or have a completed chunk
        return has_more_data || chunk_sink_->has_chunk();
    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("Error reading chunk: ") + e.what());
    }
}

ChunkData ChunkedReader::get_chunk() {
    return chunk_sink_->get_next_chunk();
}

bool ChunkedReader::has_chunk() const {
    return chunk_sink_->has_chunk();
}

const Properties& ChunkedReader::properties() const {
    if (!initialized_) {
        throw std::runtime_error("Reader not properly initialized");
    }
    return chunk_sink_->properties();
}

void ChunkedReader::ensure_initialized() {
    if (!initialized_) {
        throw std::runtime_error("Reader not properly initialized");
    }
}

} // namespace cppsas7bdat