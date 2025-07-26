/**
 * @file src/arrow_ffi.cpp
 * @brief Enhanced C FFI implementation for SAS7BDAT to Arrow conversion with simple row filtering
 * @note This version uses a simple skip + read approach for row filtering
 */

#include <cppsas7bdat/reader.hpp>
#include <cppsas7bdat/source/ifstream.hpp>
#include <cppsas7bdat/sink/arrow.hpp>
#include <cppsas7bdat/filter/column.hpp>
#include <arrow/c/bridge.h>
#include <memory>
#include <string>
#include <thread>
#include <queue>
#include <mutex>
#include <cstring> // For memset
#include <limits>

// Forward declarations for C interface
extern "C" {

// Forward declarations for Arrow C Data Interface
struct ArrowSchema;
struct ArrowArray;

// Opaque handle for the SAS reader
typedef struct SasArrowReader SasArrowReader;

// Error codes
typedef enum {
    SAS_ARROW_OK = 0,
    SAS_ARROW_ERROR_FILE_NOT_FOUND = 1,
    SAS_ARROW_ERROR_INVALID_FILE = 2,
    SAS_ARROW_ERROR_OUT_OF_MEMORY = 3,
    SAS_ARROW_ERROR_ARROW_ERROR = 4,
    SAS_ARROW_ERROR_END_OF_DATA = 5,
    SAS_ARROW_ERROR_INVALID_BATCH_INDEX = 6,
    SAS_ARROW_ERROR_NULL_POINTER = 7,
    SAS_ARROW_ERROR_INVALID_COLUMN_NAME = 8,
} SasArrowErrorCode;

// Reader info structure
typedef struct {
    uint32_t num_columns;
    uint32_t chunk_size;
    bool schema_ready;
} SasArrowReaderInfo;

// Column information
typedef struct {
    const char* name;
    const char* type_name;
    uint32_t index;
} SasArrowColumnInfo;

} // extern "C"

// Thread-local error message storage
thread_local std::string g_last_error;

// Helper function to set error message
static void set_error(const std::string& message) {
    g_last_error = message;
}

// --- Simple Sink Wrapper (no filtering) ---
// Global sink reference
static std::shared_ptr<cppsas7bdat::datasink::detail::arrow_sink> g_current_sink;

class SinkWrapper {
public:
    void set_properties(const cppsas7bdat::Properties& _properties) {
        if (!g_current_sink) {
            return;
        }
        
        g_current_sink->set_properties(_properties);
    }

    void push_row(size_t irow, cppsas7bdat::Column::PBUF p) {
        if (!g_current_sink) {
            return;
        }
        
        g_current_sink->push_row(irow, p);
    }

    void end_of_data() const noexcept {       
        if (!g_current_sink) {
            return;
        }
        
        g_current_sink->end_of_data();
    }

    explicit SinkWrapper() {}
    SinkWrapper(const SinkWrapper&) {}
    SinkWrapper& operator=(const SinkWrapper&) { return *this; }
};

// Internal SAS reader structure
struct SasArrowReader {
    std::shared_ptr<cppsas7bdat::datasink::detail::arrow_sink> sink;
    std::unique_ptr<cppsas7bdat::Reader> reader;
    std::unique_ptr<cppsas7bdat::ColumnFilter::Include> column_filter;
    std::string file_path;
    uint32_t chunk_size;
    bool schema_initialized;
    bool end_of_sas_file_source;
    bool first_batch_read;        // Track if we've done initial skip
    uint64_t start_row;           // Row to start reading from
    uint64_t end_row;             // Row to stop reading at (0 = no limit)
    uint64_t current_row_count;   // How many rows we've read so far
    
    SasArrowReader(const std::string& path, uint32_t chunk_sz) 
        : file_path(path), chunk_size(chunk_sz), 
          schema_initialized(false), end_of_sas_file_source(false),
          first_batch_read(false), start_row(0), end_row(0), current_row_count(0) {}

    ~SasArrowReader() {
        if (g_current_sink && sink && g_current_sink.get() == sink.get()) {
            g_current_sink.reset();
        }
    }
    
    SasArrowErrorCode ensure_schema_ready() {
        if (!schema_initialized) {
            const auto& properties = reader->properties();
            sink->set_properties(properties);

            if (!sink->get_schema()) { 
                 set_error("Failed to initialize SAS properties or Arrow schema. File might be empty or invalid.");
                 return SAS_ARROW_ERROR_INVALID_FILE;
            }
            schema_initialized = true;
        }
        return SAS_ARROW_OK;
    }
};

// Helper function to validate column names against schema
static SasArrowErrorCode validate_columns(
    const std::string& file_path,
    const char** include_columns
) {
    try {
        auto data_source_factory = [&file_path]() {
            return cppsas7bdat::datasource::ifstream(file_path.c_str());
        };
        
        cppsas7bdat::ColumnFilter::AcceptAll accept_all;
        SinkWrapper temp_wrapper;
        
        auto temp_sink = std::make_shared<cppsas7bdat::datasink::detail::arrow_sink>(1000);
        auto old_sink = g_current_sink;
        g_current_sink = temp_sink;
        
        cppsas7bdat::Reader temp_reader(data_source_factory(), temp_wrapper, accept_all);
        
        const auto& properties = temp_reader.properties();
        temp_sink->set_properties(properties);
        
        auto schema = temp_sink->get_schema();
        if (!schema) {
            g_current_sink = old_sink;
            set_error("Failed to read schema for column validation");
            return SAS_ARROW_ERROR_INVALID_FILE;
        }
        
        // Collect available column names
        std::set<std::string> available_columns;
        for (int i = 0; i < schema->num_fields(); ++i) {
            available_columns.insert(schema->field(i)->name());
        }
        
        // Validate requested columns
        for (const char** col = include_columns; *col != nullptr; ++col) {
            std::string col_name(*col);
            if (available_columns.find(col_name) == available_columns.end()) {
                g_current_sink = old_sink;
                set_error("Column not found: " + col_name);
                return SAS_ARROW_ERROR_INVALID_COLUMN_NAME;
            }
        }
        
        g_current_sink = old_sink;
        return SAS_ARROW_OK;
        
    } catch (const std::exception& e) {
        set_error(std::string("Failed to validate columns: ") + e.what());
        if (std::string(e.what()).find("No such file or directory") != std::string::npos ||
            std::string(e.what()).find("open failed") != std::string::npos) {
            return SAS_ARROW_ERROR_FILE_NOT_FOUND;
        }
        return SAS_ARROW_ERROR_INVALID_FILE;
    }
}

// Helper function to convert C++ exceptions to error codes
template<typename Func>
static SasArrowErrorCode safe_call(Func&& func) {
    try {
        return func();
    } catch (const std::bad_alloc&) {
        set_error("Out of memory");
        return SAS_ARROW_ERROR_OUT_OF_MEMORY;
    } catch (const std::exception& e) {
        set_error(std::string("Error: ") + e.what());
        return SAS_ARROW_ERROR_ARROW_ERROR;
    } catch (...) {
        set_error("Unknown error occurred");
        return SAS_ARROW_ERROR_ARROW_ERROR;
    }
}

extern "C" {

SasArrowErrorCode sas_arrow_reader(
    const char* file_path,
    uint32_t chunk_size,
    const char** include_columns,  // NULL-terminated array of column names, or NULL for all columns
    SasArrowReader** reader_out
) {
    if (!file_path || !reader_out) {
        set_error("Null pointer provided for file_path or reader_out.");
        return SAS_ARROW_ERROR_NULL_POINTER;
    }
    
    return safe_call([&]() -> SasArrowErrorCode {
        auto chunk_sz = chunk_size == 0 ? 65536U : chunk_size;
        
        // Validate columns if specified
        if (include_columns) {
            SasArrowErrorCode validation_result = validate_columns(file_path, include_columns);
            if (validation_result != SAS_ARROW_OK) {
                return validation_result;
            }
        }
        
        auto sas_reader_instance = std::make_unique<SasArrowReader>(file_path, chunk_sz);

        sas_reader_instance->sink = std::make_shared<cppsas7bdat::datasink::detail::arrow_sink>(
            static_cast<int64_t>(chunk_sz)
        );

        // Set the global reference FIRST
        g_current_sink = sas_reader_instance->sink;

        try {
            auto data_source_factory = [path = sas_reader_instance->file_path]() {
                return cppsas7bdat::datasource::ifstream(path.c_str());
            };

            SinkWrapper sink_wrapper;

            if (include_columns) {
                // Create Include filter with specified columns
                sas_reader_instance->column_filter = std::make_unique<cppsas7bdat::ColumnFilter::Include>();
                
                // Add columns to include set
                for (const char** col = include_columns; *col != nullptr; ++col) {
                    sas_reader_instance->column_filter->included.insert(std::string(*col));
                }
                
                sas_reader_instance->reader = std::make_unique<cppsas7bdat::Reader>(
                    data_source_factory(),
                    sink_wrapper,
                    *(sas_reader_instance->column_filter)
                );
            } else {
                // Use AcceptAll filter for all columns
                cppsas7bdat::ColumnFilter::AcceptAll accept_all;
                sas_reader_instance->reader = std::make_unique<cppsas7bdat::Reader>(
                    data_source_factory(),
                    sink_wrapper,
                    accept_all
                );
            }
            
            SasArrowErrorCode err = sas_reader_instance->ensure_schema_ready();
            if (err != SAS_ARROW_OK) {
                return err;
            }
            
        } catch (const std::exception& e) {
            set_error(std::string("Failed to open or initialize SAS file: ") + e.what());
            if (std::string(e.what()).find("No such file or directory") != std::string::npos ||
                std::string(e.what()).find("open failed") != std::string::npos) {
                return SAS_ARROW_ERROR_FILE_NOT_FOUND;
            }
            return SAS_ARROW_ERROR_INVALID_FILE; 
        }
        
        *reader_out = sas_reader_instance.release();
        return SAS_ARROW_OK;
    });
}

// Simple row filtering: just set start/end rows
SasArrowErrorCode sas_arrow_reader_set_row_filter(
    SasArrowReader* reader,
    uint64_t start_row,  // 0 = no start filter
    uint64_t end_row     // 0 = no end filter
) {
    if (!reader) {
        set_error("Null pointer provided for reader.");
        return SAS_ARROW_ERROR_NULL_POINTER;
    }
    
    if (start_row > 0 && end_row > 0 && start_row >= end_row) {
        set_error("Invalid row range: start_row must be less than end_row.");
        return SAS_ARROW_ERROR_INVALID_BATCH_INDEX;
    }
    
    //  printf("DEBUG: set_row_filter called with start_row=%lu, end_row=%lu\n", start_row, end_row);
    
    reader->start_row = start_row;
    //  Reset end since the reader will read end_row rows AFTER skipping start row 
    reader->end_row = end_row - start_row;
    
    return SAS_ARROW_OK;
}

SasArrowErrorCode sas_arrow_reader_get_info(
    const SasArrowReader* reader,
    SasArrowReaderInfo* info
) {
    if (!reader || !info) {
        set_error("Null pointer provided.");
        return SAS_ARROW_ERROR_NULL_POINTER;
    }
    
    return safe_call([&]() -> SasArrowErrorCode {
        SasArrowErrorCode err = const_cast<SasArrowReader*>(reader)->ensure_schema_ready();
        if (err != SAS_ARROW_OK) return err;
        
        auto schema = reader->sink->get_schema();
        
        info->num_columns = static_cast<uint32_t>(schema->num_fields());
        info->chunk_size = reader->chunk_size;
        info->schema_ready = reader->schema_initialized;
        
        return SAS_ARROW_OK;
    });
}

SasArrowErrorCode sas_arrow_reader_get_column_info(
    const SasArrowReader* reader,
    uint32_t column_index,
    SasArrowColumnInfo* column_info
) {
    if (!reader || !column_info) {
        set_error("Null pointer provided.");
        return SAS_ARROW_ERROR_NULL_POINTER;
    }
    
    return safe_call([&]() -> SasArrowErrorCode {
        SasArrowErrorCode err = const_cast<SasArrowReader*>(reader)->ensure_schema_ready();
        if (err != SAS_ARROW_OK) return err;
        
        auto schema = reader->sink->get_schema();
        if (column_index >= static_cast<uint32_t>(schema->num_fields())) {
            set_error("Column index out of range.");
            return SAS_ARROW_ERROR_INVALID_BATCH_INDEX;
        }
        
        auto field = schema->field(static_cast<int>(column_index));
        column_info->name = field->name().c_str();
        column_info->type_name = field->type()->ToString().c_str();
        column_info->index = column_index;
        
        return SAS_ARROW_OK;
    });
}

SasArrowErrorCode sas_arrow_reader_get_schema(
    const SasArrowReader* reader,
    struct ArrowSchema* schema
) {
    if (!reader || !schema) {
        set_error("Null pointer provided.");
        return SAS_ARROW_ERROR_NULL_POINTER;
    }
    
    return safe_call([&]() -> SasArrowErrorCode {
        SasArrowErrorCode err = const_cast<SasArrowReader*>(reader)->ensure_schema_ready();
        if (err != SAS_ARROW_OK) return err;
        
        auto arrow_schema = reader->sink->get_schema();
        auto status = arrow::ExportSchema(*arrow_schema, schema);
        if (!status.ok()) {
            set_error("Failed to export Arrow schema: " + status.ToString());
            return SAS_ARROW_ERROR_ARROW_ERROR;
        }
        
        return SAS_ARROW_OK;
    });
}

SasArrowErrorCode sas_arrow_reader_next_batch(
    SasArrowReader* reader,
    struct ArrowArray* array_out
) {
    if (!reader || !array_out) {
        set_error("Null pointer provided for reader or array_out.");
        return SAS_ARROW_ERROR_NULL_POINTER;
    }

    return safe_call([&]() -> SasArrowErrorCode {
        memset(array_out, 0, sizeof(ArrowArray));

        // Condition 3: Already reached end of SAS file or slice limit in previous call
        if (reader->end_of_sas_file_source) {
            //  printf("DEBUG: Already marked as end of data. Returning END_OF_DATA.\n");
            return SAS_ARROW_ERROR_END_OF_DATA;
        }

        SasArrowErrorCode err = reader->ensure_schema_ready();
        if (err != SAS_ARROW_OK) return err;

        // Condition 2 Check: If we've already processed enough rows for the slice
        if (reader->end_row > 0 && reader->current_row_count >= reader->end_row) {
            // printf("DEBUG: Already at or past end_row (%lu), setting end_of_sas_file_source and returning END_OF_DATA.\n", reader->end_row);
            reader->end_of_sas_file_source = true;
            return SAS_ARROW_ERROR_END_OF_DATA;
        }

        // First batch: skip to start_row if needed
        if (!reader->first_batch_read && reader->start_row > 0) {
            // printf("DEBUG: First batch - skipping %lu rows\n", reader->start_row);

            // Temporarily disable sink during skipping
            auto temp_sink = g_current_sink;
            g_current_sink.reset();

            uint64_t rows_to_skip = reader->start_row;
            while (rows_to_skip > 0 && !reader->end_of_sas_file_source) {
                // Ensure we don't try to read more than available in the file
                uint64_t actual_skip_chunk = std::min(rows_to_skip, static_cast<uint64_t>(reader->chunk_size));
                
                bool more_rows = reader->reader->read_rows(static_cast<size_t>(actual_skip_chunk));
                if (!more_rows) {
                    // Condition 3: Reached end of actual file during skip
                    reader->end_of_sas_file_source = true;
                    // printf("DEBUG: Reached actual end of file during skip.\n");
                    break;
                }
                rows_to_skip -= actual_skip_chunk;
            }

            // Restore sink
            g_current_sink = temp_sink;
            reader->first_batch_read = true;

            if (reader->end_of_sas_file_source) {
                return SAS_ARROW_ERROR_END_OF_DATA;
            }
        }
        reader->first_batch_read = true; // Ensure this is set after potential skipping

        // Determine the maximum rows to read from the file source in this cycle.
        uint64_t max_rows_to_read_from_source = reader->chunk_size;
        if (reader->end_row > 0) {
            // Calculate remaining rows until end_row for this slice
            uint64_t remaining_rows_for_slice = reader->end_row - reader->current_row_count;
            max_rows_to_read_from_source = std::min(max_rows_to_read_from_source, remaining_rows_for_slice);
        }
        
        // If we don't need any more rows based on the end_row limit, signal end of data.
        if (max_rows_to_read_from_source == 0) {
            // printf("DEBUG: No more rows needed based on end_row limit (%lu), returning END_OF_DATA.\n", reader->end_row);
            reader->end_of_sas_file_source = true;
            return SAS_ARROW_ERROR_END_OF_DATA;
        }

        // printf("DEBUG: Attempting to read up to %lu rows from SAS file source.\n", max_rows_to_read_from_source);
        bool more_rows_from_sas = reader->reader->read_rows(static_cast<size_t>(max_rows_to_read_from_source));
        
        // Now, attempt to retrieve a batch from the sink.
        // We first try for a regular batch (if chunk_size was met),
        // then for a final partial batch if the file ended or we hit our slice limit.
        std::shared_ptr<arrow::RecordBatch> batch = nullptr;
        
        auto regular_batch_result = reader->sink->get_next_available_batch();
        if (regular_batch_result.ok() && regular_batch_result.ValueOrDie()) {
            batch = regular_batch_result.ValueOrDie();
            // f("DEBUG: Got a regular batch from sink (size %ld).\n", batch->num_rows());
        } else if (!more_rows_from_sas || max_rows_to_read_from_source < reader->chunk_size) {
            // If `read_rows` returned false (end of file) OR
            // if we requested less than chunk_size (due to end_row limit)
            // THEN, we should check for a final, potentially partial batch.
            // printf("DEBUG: Either end of SAS file reached (%d) or slice limit reached (%d < %d). Requesting final batch.\n", 
                   //   !more_rows_from_sas, (int)max_rows_to_read_from_source, (int)reader->chunk_size);
            auto final_batch_result = reader->sink->get_final_batch();
            if (final_batch_result.ok() && final_batch_result.ValueOrDie()) {
                batch = final_batch_result.ValueOrDie();
                // printf("DEBUG: Got a final partial batch from sink (size %ld).\n", batch->num_rows());
            }
        }

        if (batch) {
            uint64_t batch_rows_original = static_cast<uint64_t>(batch->num_rows());
            
            // Critical: If this batch, even after careful reading, would exceed end_row, slice it.
            // This can happen if end_row falls mid-way through a single row's data reading,
            // or if the sink internally optimizes.
            if (reader->end_row > 0 && (reader->current_row_count + batch_rows_original) > reader->end_row) {
                uint64_t rows_to_take = reader->end_row - reader->current_row_count;
                // printf("DEBUG: Slicing exported batch. Original %lu rows, taking %lu.\n", batch_rows_original, rows_to_take);
                batch = batch->Slice(0, static_cast<int64_t>(rows_to_take));
            }
            
            uint64_t batch_rows_exported = static_cast<uint64_t>(batch->num_rows());
            
            // Only increment current_row_count by what we are actually exporting.
            reader->current_row_count += batch_rows_exported;

            // Condition 2/3: After exporting, check if we've reached our slice limit or end of file.
            if (!more_rows_from_sas || (reader->end_row > 0 && reader->current_row_count >= reader->end_row)) {
                reader->end_of_sas_file_source = true;
                // printf("DEBUG: End condition met after exporting batch (more_rows_from_sas=%d, current_row_count=%lu, end_row=%lu). Setting end_of_sas_file_source.\n", 
                //       more_rows_from_sas, reader->current_row_count, reader->end_row);
            }

            
            auto status = arrow::ExportRecordBatch(*batch, array_out);
            if (!status.ok()) {
                set_error("Failed to export RecordBatch: " + status.ToString());
                return SAS_ARROW_ERROR_ARROW_ERROR;
            }
            return SAS_ARROW_OK; // Successfully returned a batch
        } else {
            // No batch obtained, even after trying for final. This means truly no more data.
            //  printf("DEBUG: No batch obtained from sink, even after attempting to read from source and requesting final. Assuming true end of data.\n");
            reader->end_of_sas_file_source = true;
            return SAS_ARROW_ERROR_END_OF_DATA;
        }
    });
}

const char* sas_arrow_get_last_error(void) {
    return g_last_error.c_str();
}

void sas_arrow_reader_destroy(SasArrowReader* reader) {
    delete reader;
}

const char* sas_arrow_error_message(SasArrowErrorCode error_code) {
    switch (error_code) {
        case SAS_ARROW_OK: return "Success";
        case SAS_ARROW_ERROR_FILE_NOT_FOUND: return "File not found or cannot be opened";
        case SAS_ARROW_ERROR_INVALID_FILE: return "Invalid SAS7BDAT file format";
        case SAS_ARROW_ERROR_OUT_OF_MEMORY: return "Out of memory";
        case SAS_ARROW_ERROR_ARROW_ERROR: return "Arrow library error";
        case SAS_ARROW_ERROR_END_OF_DATA: return "End of data reached";
        case SAS_ARROW_ERROR_INVALID_BATCH_INDEX: return "Invalid column index";
        case SAS_ARROW_ERROR_NULL_POINTER: return "Null pointer provided";
        case SAS_ARROW_ERROR_INVALID_COLUMN_NAME: return "Invalid column name";
        default: return "Unknown error";
    }
}

bool sas_arrow_is_ok(SasArrowErrorCode error_code) {
    return error_code == SAS_ARROW_OK;
}

} // extern "C"