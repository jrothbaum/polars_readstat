// vendor/src/rs/chunked_reader.hpp
#ifndef CPP_SAS7BDAT_CHUNKED_READER_HPP_
#define CPP_SAS7BDAT_CHUNKED_READER_HPP_

#include <cppsas7bdat/reader.hpp>
#include <cppsas7bdat/source/ifstream.hpp>
#include <vector>
#include <queue>
#include <memory>

namespace cppsas7bdat {

// Optimized chunk data structure - stores only raw buffers
struct ChunkData {
    
    std::vector<std::vector<uint8_t>> row_buffers; // Store raw buffer data only
    size_t start_row;
    size_t end_row;
    bool is_complete;

    ChunkData() : start_row(0), end_row(0), is_complete(false) {}
    ChunkData(size_t chunk_size);
    ChunkData(ChunkData&& other) noexcept;
    ChunkData& operator=(ChunkData&& other) noexcept;
    
    void clear();
    bool is_full(size_t target_size) const;
    
    // New helper methods
    size_t row_count() const;
    const void* get_row_buffer(size_t row_index) const;
};

// Custom sink that accumulates chunks
class ChunkSink {
public:
    explicit ChunkSink(size_t chunk_size);

    // Required sink interface methods
    void set_properties(const Properties& properties);
    void push_row(size_t row_index, Column::PBUF row_data);
    void end_of_data();

    // Chunk management
    bool has_chunk() const;
    ChunkData get_next_chunk();
    bool is_finished() const;
    const Properties& properties() const;

private:
    size_t chunk_size_;
    ChunkData current_chunk_;
    std::queue<ChunkData> completed_chunks_;
    const Properties* properties_;
    bool finished_;
    size_t row_buffer_size_; // Cached buffer size calculation

    // Helper to calculate row buffer size
    size_t calculate_row_buffer_size() const;
};

// Chunked reader wrapper
class ChunkedReader {
public:
    ChunkedReader(const std::string& filename, size_t chunk_size);

    // Reading interface
    bool read_next_chunk();
    ChunkData get_chunk();
    bool has_chunk() const;
    const Properties& properties() const;

private:
    std::unique_ptr<ChunkSink> chunk_sink_;
    std::unique_ptr<cppsas7bdat::Reader> reader_;
    size_t chunk_size_;
    bool initialized_;

    void ensure_initialized();
};

} // namespace cppsas7bdat

#endif