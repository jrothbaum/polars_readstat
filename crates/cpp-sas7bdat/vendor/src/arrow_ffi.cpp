/**
 * @file src/arrow_ffi.cpp
 * @brief Enhanced C FFI implementation for SAS7BDAT to Arrow conversion with true streaming support
 * @note This version includes a SinkWrapper to work around a bug in the underlying cppsas7bdat library
 * without modifying its source files.
 */

#include <cppsas7bdat/reader.hpp>
#include <cppsas7bdat/source/ifstream.hpp>
#include <cppsas7bdat/sink/arrow.hpp>
#include <arrow/c/bridge.h>
#include <memory>
#include <string>
#include <thread>
#include <queue>
#include <mutex>
#include <cstring> // For memset

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

// --- Sink Wrapper ---
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

    // Constructor doesn't store ANYTHING - completely stateless
    explicit SinkWrapper() {
    }

    SinkWrapper(const SinkWrapper&) {
    }

    SinkWrapper& operator=(const SinkWrapper&) {
        return *this;
    }

    // No member variables at all!
};

// Internal SAS reader structure
struct SasArrowReader {
    std::shared_ptr<cppsas7bdat::datasink::detail::arrow_sink> sink;  // CHANGED: shared_ptr instead of unique_ptr
    std::unique_ptr<cppsas7bdat::Reader> reader;
    std::string file_path;
    uint32_t chunk_size;
    bool schema_initialized;
    bool end_of_sas_file_source;
    bool data_reading_started;
    
    SasArrowReader(const std::string& path, uint32_t chunk_sz) 
        : file_path(path), chunk_size(chunk_sz), 
          schema_initialized(false), end_of_sas_file_source(false),
          data_reading_started(false) {}

    ~SasArrowReader() {
        // Clear global reference if it points to our sink
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
    SasArrowReader** reader_out
) {
    if (!file_path || !reader_out) {
        set_error("Null pointer provided for file_path or reader_out.");
        return SAS_ARROW_ERROR_NULL_POINTER;
    }
    
    return safe_call([&]() -> SasArrowErrorCode {
        auto chunk_sz = chunk_size == 0 ? 65536U : chunk_size;
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

            // Create completely stateless wrapper
            SinkWrapper sink_wrapper;  // No parameters!

            sas_reader_instance->reader = std::make_unique<cppsas7bdat::Reader>(
                data_source_factory(),
                sink_wrapper
            );
            
            
            
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

        if (reader->end_of_sas_file_source) {
            return SAS_ARROW_ERROR_END_OF_DATA;
        }

        SasArrowErrorCode err = reader->ensure_schema_ready();
        if (err != SAS_ARROW_OK) return err;

        // Try to get a batch from any data remaining from a previous read.
        // On the first call, the sink is empty, so this will correctly do nothing.
        auto batch_result = reader->sink->get_next_available_batch();
        
        if (batch_result.ok() && batch_result.ValueOrDie()) {
            auto batch = batch_result.ValueOrDie(); 
            auto status = arrow::ExportRecordBatch(*batch, array_out);
            if (!status.ok()) {
                set_error("Failed to export RecordBatch: " + status.ToString());
                return SAS_ARROW_ERROR_ARROW_ERROR;
            }
            return SAS_ARROW_OK;
        }

        // If no batch was ready, read a new chunk of data from the file.
        bool more_rows_from_sas = reader->reader->read_rows(static_cast<size_t>(reader->chunk_size));
        
        if (!more_rows_from_sas) {
            reader->end_of_sas_file_source = true;
            // Check for a final partial batch.
            auto final_batch_result = reader->sink->get_final_batch();
            if (final_batch_result.ok() && final_batch_result.ValueOrDie()) {
                auto batch = final_batch_result.ValueOrDie();
                auto status = arrow::ExportRecordBatch(*batch, array_out);
                if (!status.ok()) {
                    set_error("Failed to export final partial RecordBatch: " + status.ToString());
                    return SAS_ARROW_ERROR_ARROW_ERROR;
                }
                return SAS_ARROW_OK;
            } else {
                return SAS_ARROW_ERROR_END_OF_DATA;
            }
        }

        // After reading new data, try to get a batch again.
        batch_result = reader->sink->get_next_available_batch();
        if (batch_result.ok() && batch_result.ValueOrDie()) {
            auto batch = batch_result.ValueOrDie(); 
            auto status = arrow::ExportRecordBatch(*batch, array_out);
            if (!status.ok()) {
                set_error("Failed to export RecordBatch: " + status.ToString());
                return SAS_ARROW_ERROR_ARROW_ERROR;
            }
            return SAS_ARROW_OK;
        }

        // If we still don't have a batch, it means we're at the end.
        return SAS_ARROW_ERROR_END_OF_DATA;
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
        default: return "Unknown error";
    }
}

bool sas_arrow_is_ok(SasArrowErrorCode error_code) {
    return error_code == SAS_ARROW_OK;
}

} // extern "C"
