#include "c_api.h"
#include "chunked_reader.hpp"
#include <memory>
#include <cstring>
#include <string>
#include <algorithm> // Required for std::all_of
#include <cmath>     // Required for std::isnan
#include <mutex>     // Required for std::mutex

// Internal state management
struct ChunkedReaderState {
    std::unique_ptr<cppsas7bdat::ChunkedReader> reader;
    cppsas7bdat::ChunkData current_chunk;
    bool has_current_chunk = false;
    // Added mutex for thread-safety if chunked_reader_next_chunk is called concurrently
    std::mutex reader_mutex;

    ChunkedReaderState(const std::string& filename, size_t chunk_size)
        : reader(std::make_unique<cppsas7bdat::ChunkedReader>(filename, chunk_size)) {}
};

// Optimized chunk iterator - processes data in batches
struct ChunkIteratorState {
    cppsas7bdat::ChunkData chunk;
    const cppsas7bdat::Properties* properties;
    size_t current_row_index = 0;

    // Reusable string storage for the current row.
    // Pointers in CColumnValue::string_val will point into these std::strings.
    std::vector<std::string> string_storage;

    ChunkIteratorState(cppsas7bdat::ChunkData&& chunk_data, const cppsas7bdat::Properties* props)
        : chunk(std::move(chunk_data)), properties(props) {
        if (properties) {
            // Reserve enough space for all strings in a row to avoid reallocations
            string_storage.reserve(properties->columns.size());
        }
    }
};

extern "C" {

// Core reader functions
ChunkedReaderHandle chunked_reader_create(const char* filename, size_t chunk_size) {
    if (!filename) return nullptr;

    try {
        auto* state = new ChunkedReaderState(filename, chunk_size);
        return static_cast<ChunkedReaderHandle>(state);
    } catch (const std::exception& e) {
        // Log error if needed: std::cerr << "Error in chunked_reader_create: " << e.what() << std::endl;
        return nullptr;
    } catch (...) {
        // Log error if needed: std::cerr << "Unknown error in chunked_reader_create" << std::endl;
        return nullptr;
    }
}

int chunked_reader_get_properties(ChunkedReaderHandle handle, CProperties* properties) {
    if (!handle || !properties) return -1;

    try {
        auto* state = static_cast<ChunkedReaderState*>(handle);
        const auto& props = state->reader->properties();

        // Convert C++ properties to C structure
        properties->column_count = props.columns.size();
        properties->total_rows = 0; // cpp-sas7bdat doesn't provide total rows upfront

        if (properties->column_count == 0) {
            properties->columns = nullptr;
            return 0;
        }

        properties->columns = static_cast<CColumnInfo*>(
            malloc(properties->column_count * sizeof(CColumnInfo))
        );

        if (!properties->columns) return -1;

        // Use a vector to keep track of allocated names for easier cleanup on error
        std::vector<char*> allocated_names;
        allocated_names.reserve(properties->column_count); // Pre-allocate

        for (size_t i = 0; i < properties->column_count; ++i) {
            const auto& col = props.columns[i];
            auto& c_col = properties->columns[i];

            // Allocate and copy name
            size_t name_len = col.name.length();
            char* name_copy = static_cast<char*>(malloc(name_len + 1));
            if (!name_copy) {
                // Cleanup all names allocated so far
                for (char* name : allocated_names) {
                    free(name);
                }
                free(properties->columns);
                properties->columns = nullptr; // Set to nullptr to avoid double free
                return -1; // Out of memory
            }
            strcpy(name_copy, col.name.c_str());
            c_col.name = name_copy;
            allocated_names.push_back(name_copy); // Add to our tracking vector

            // Map column type based on SAS format
            switch (col.type) {
                case cppsas7bdat::Column::Type::string:
                    c_col.column_type = 0; // string
                    break;
                case cppsas7bdat::Column::Type::number:
                    c_col.column_type = 1; // numeric
                    break;
                case cppsas7bdat::Column::Type::date:
                    c_col.column_type = 2; // date
                    break;
                case cppsas7bdat::Column::Type::datetime:
                    c_col.column_type = 3; // datetime
                    break;
                case cppsas7bdat::Column::Type::time:
                    c_col.column_type = 4; // time
                    break;
                case cppsas7bdat::Column::Type::integer:
                    c_col.column_type = 1; // numeric (treat integers as numeric)
                    break;
                case cppsas7bdat::Column::Type::unknown:
                default:
                    c_col.column_type = 1; // default to numeric
                    break;
            }

            c_col.length = col.length();
        }

        return 0;
    } catch (...) {
        return -1;
    }
}

int chunked_reader_next_chunk(ChunkedReaderHandle handle, CChunkInfo* chunk_info) {
    if (!handle || !chunk_info) return -1;

    try {
        auto* state = static_cast<ChunkedReaderState*>(handle);
        std::lock_guard<std::mutex> lock(state->reader_mutex); // Lock here for thread-safety

        // Try to read next chunk
        if (!state->reader->read_next_chunk()) {
            return 1; // No more data
        }

        if (!state->reader->has_chunk()) {
            return 1; // No chunk available
        }

        // Get the chunk and store it in state
        state->current_chunk = state->reader->get_chunk();
        state->has_current_chunk = true;

        // Fill chunk info
        chunk_info->row_count = state->current_chunk.row_buffers.size();
        chunk_info->start_row = state->current_chunk.start_row;
        chunk_info->end_row = state->current_chunk.end_row;

        return 0; // Success
    } catch (...) {
        return -1;
    }
}

int chunked_reader_has_chunk(ChunkedReaderHandle handle) {
    if (!handle) return 0;

    try {
        auto* state = static_cast<ChunkedReaderState*>(handle);
        // has_current_chunk is set/cleared under reader_mutex, so no need to lock here.
        return state->has_current_chunk ? 1 : 0;
    } catch (...) {
        return 0;
    }
}

void chunked_reader_destroy(ChunkedReaderHandle handle) {
    if (handle) {
        delete static_cast<ChunkedReaderState*>(handle);
    }
}

// Chunk iterator functions
ChunkIteratorHandle chunk_iterator_create(ChunkedReaderHandle reader_handle) {
    if (!reader_handle) return nullptr;

    try {
        auto* reader_state = static_cast<ChunkedReaderState*>(reader_handle);

        if (!reader_state->has_current_chunk) {
            return nullptr;
        }

        // Get properties pointer for efficient access
        const auto& properties = reader_state->reader->properties();

        // Move the chunk to iterator (reader no longer owns it)
        auto* iterator_state = new ChunkIteratorState(
            std::move(reader_state->current_chunk),
            &properties
        );
        reader_state->has_current_chunk = false; // Mark as consumed

        return static_cast<ChunkIteratorHandle>(iterator_state);
    } catch (...) {
        return nullptr;
    }
}

// Optimized row processing - extract values more efficiently
int chunk_iterator_next_row(ChunkIteratorHandle handle, CRowData* row_data) {
    if (!handle || !row_data) return -1;

    try {
        auto* state = static_cast<ChunkIteratorState*>(handle);
        if (state->current_row_index >= state->chunk.row_buffers.size()) {
            return 1; // No more rows
        }

        const void* row_buffer = state->chunk.get_row_buffer(state->current_row_index);
        const auto& columns = state->properties->columns;

        // Clear previous string storage for reuse for this new row
        state->string_storage.clear();

        row_data->column_count = columns.size();
        row_data->values = static_cast<CColumnValue*>(
            malloc(columns.size() * sizeof(CColumnValue))
        );

        if (!row_data->values && columns.size() > 0) {
            return -1;
        }

        // Process all columns more efficiently
        for (size_t i = 0; i < columns.size(); ++i) {
            const auto& col = columns[i];
            auto& c_val = row_data->values[i];

            // Extract value directly from buffer using offset
            if (!row_buffer) {
                c_val.value_type = 0;
                c_val.string_val = nullptr;
                c_val.numeric_val = 0.0;
                c_val.is_null = 1;
                continue;
            }

            switch (col.type) {
                case cppsas7bdat::Column::Type::string: {
                    c_val.value_type = 1; // String type

                    // Extract string directly from buffer - get_string returns string_view
                    auto str_view = col.get_string(row_buffer);
                    std::string str_val(str_view); // Convert string_view to string

                    // Check if string is empty or contains only whitespace (SAS missing for strings)
                    bool is_missing = str_val.empty() ||
                                    std::all_of(str_val.begin(), str_val.end(),
                                              [](unsigned char c) { return std::isspace(c); });

                    if (!is_missing) {
                        // Store string in reusable storage (vector)
                        state->string_storage.push_back(std::move(str_val));
                        // Get a const char* pointer to the internally managed string data.
                        // This pointer is valid ONLY until string_storage is cleared or reallocated.
                        c_val.string_val = state->string_storage.back().c_str();
                        c_val.is_null = 0;
                    } else {
                        c_val.string_val = nullptr;
                        c_val.is_null = 1;
                    }
                    c_val.numeric_val = 0.0; // Ensure numeric_val is zeroed for strings
                    break;
                }

                case cppsas7bdat::Column::Type::number:
                case cppsas7bdat::Column::Type::integer: {
                    c_val.value_type = 2; // Numeric type
                    c_val.string_val = nullptr;

                    double num_val = col.get_number(row_buffer);

                    // Check for SAS missing values (NaN indicates missing)
                    if (std::isnan(num_val)) {
                        c_val.numeric_val = 0.0;
                        c_val.is_null = 1;
                    } else {
                        c_val.numeric_val = num_val;
                        c_val.is_null = 0;
                    }
                    break;
                }

                case cppsas7bdat::Column::Type::date:
                case cppsas7bdat::Column::Type::datetime:
                case cppsas7bdat::Column::Type::time: {
                    c_val.value_type = 2; // Treat dates/times as numeric
                    c_val.string_val = nullptr;

                    double date_val = col.get_number(row_buffer);

                    // Check for SAS missing values (NaN indicates missing)
                    if (std::isnan(date_val)) {
                        c_val.numeric_val = 0.0;
                        c_val.is_null = 1;
                    } else {
                        c_val.numeric_val = date_val;
                        c_val.is_null = 0;
                    }
                    break;
                }

                default: {
                    c_val.value_type = 0; // Unknown/null type
                    c_val.string_val = nullptr;
                    c_val.numeric_val = 0.0;
                    c_val.is_null = 1;
                    break;
                }
            }
        }

        state->current_row_index++;
        return 0;
    } catch (...) {
        return -1;
    }
}

int chunk_iterator_has_next(ChunkIteratorHandle handle) {
    if (!handle) return 0;

    try {
        auto* state = static_cast<ChunkIteratorState*>(handle);
        return (state->current_row_index < state->chunk.row_buffers.size()) ? 1 : 0;
    } catch (...) {
        return 0;
    }
}

void chunk_iterator_destroy(ChunkIteratorHandle handle) {
    if (handle) {
        delete static_cast<ChunkIteratorState*>(handle);
    }
}

// Memory management functions
void free_row_data(CRowData* row_data) {
    if (!row_data || !row_data->values) return;

    // No need to free individual string_val pointers anymore
    // as they point into the ChunkIteratorState's string_storage.
    // The string_storage is owned by ChunkIteratorState and cleaned up on its destruction.

    // Free the values array itself
    free(row_data->values);
    row_data->values = nullptr;
    row_data->column_count = 0;
}

void free_properties(CProperties* properties) {
    if (!properties || !properties->columns) return;

    // Free each column name
    for (size_t i = 0; i < properties->column_count; ++i) {
        if (properties->columns[i].name) {
            free(const_cast<char*>(properties->columns[i].name));
        }
    }

    // Free the columns array
    free(properties->columns);
    properties->columns = nullptr;
    properties->column_count = 0;
    properties->total_rows = 0;
}

} // extern "C"