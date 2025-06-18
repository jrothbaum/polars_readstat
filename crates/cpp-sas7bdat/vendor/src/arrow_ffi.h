/**
 * @file src/arrow_ffi.h  
 * @brief C FFI interface for SAS7BDAT to Apache Arrow conversion with streaming support
 */

#ifndef CPPSAS7BDAT_ARROW_FFI_H
#define CPPSAS7BDAT_ARROW_FFI_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

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
    SAS_ARROW_ERROR_INVALID_BATCH_INDEX = 6, // Kept for consistency, but less relevant for streaming FFI
    SAS_ARROW_ERROR_NULL_POINTER = 7,
} SasArrowErrorCode;

// Reader info structure - simplified for streaming
typedef struct {
    // In true streaming, total rows/batches are unknown until the entire file has been iterated.
    // These fields will be 0 when retrieved before full iteration completes.    
    uint32_t num_columns; // Number of columns in the dataset, known after schema initialization
    uint32_t chunk_size;  // Configured chunk size for Arrow RecordBatches
    bool schema_ready;    // Indicates if schema information has been successfully loaded
} SasArrowReaderInfo;

// Column information
typedef struct {
    const char* name;
    const char* type_name;  // e.g., "string", "int64", "float64", "timestamp(MICRO)", "date32", "time64(MICRO)"
    uint32_t index;
} SasArrowColumnInfo;

/**
 * Create a new SAS Arrow reader instance. This reader operates in a streaming fashion.
 * The schema (metadata) is initialized upon creation without reading all data.
 * * @param file_path Path to the .sas7bdat file.
 * @param chunk_size Desired number of rows per Arrow batch. A value of 0 uses the default (65536).
 * @param reader_out Output pointer to the created SasArrowReader opaque handle. Must be destroyed with sas_arrow_reader_destroy().
 * @return Error code (SAS_ARROW_OK on success, or an error code if file cannot be opened/initialized).
 */
SasArrowErrorCode sas_arrow_reader(
    const char* file_path,
    uint32_t chunk_size,
    SasArrowReader** reader_out
);

/**
 * Get basic information about the SAS file and reader state.
 * * @param reader The SAS reader instance.
 * @param info Output structure to fill with file information.
 * @return Error code.
 */
SasArrowErrorCode sas_arrow_reader_get_info(
    const SasArrowReader* reader,
    SasArrowReaderInfo* info
);

/**
 * Get detailed information for a specific column by index.
 * The schema must be ready (checked via `sas_arrow_reader_get_info`) before calling this.
 * * @param reader The SAS reader instance.
 * @param column_index Zero-based index of the column.
 * @param column_info Output structure to fill with column information. The `name` and `type_name`
 * pointers are valid as long as the `SasArrowReader` instance exists.
 * @return Error code.
 */
SasArrowErrorCode sas_arrow_reader_get_column_info(
    const SasArrowReader* reader,
    uint32_t column_index,
    SasArrowColumnInfo* column_info
);

/**
 * Get the Arrow schema for the dataset.
 * This can be called immediately after `sas_arrow_reader_new` completes successfully.
 * The caller is responsible for releasing the `ArrowSchema` structure using its `release` callback.
 * * @param reader The SAS reader instance.
 * @param schema Output Arrow schema structure.
 * @return Error code.
 */
SasArrowErrorCode sas_arrow_reader_get_schema(
    const SasArrowReader* reader,
    struct ArrowSchema* schema
);

/**
 * Stream interface: Retrieves the next available Arrow RecordBatch.
 * This function reads from the underlying SAS file progressively.
 * The caller is responsible for releasing the `ArrowArray` structure using its `release` callback.
 * * @param reader The SAS reader instance.
 * @param array_out Output Arrow array structure containing the next record batch.
 * @return Error code. Returns `SAS_ARROW_ERROR_END_OF_DATA` when all batches have been read.
 */
SasArrowErrorCode sas_arrow_reader_next_batch(
    SasArrowReader* reader,
    struct ArrowArray* array_out
);

/**
 * Retrieves the last error message set by an FFI function call on the current thread.
 * * @return A C-style string containing the error message. This string is valid until
 * the next FFI call that might overwrite the thread-local error state.
 */
const char* sas_arrow_get_last_error(void);

/**
 * Destroys the SAS reader instance and frees all associated resources.
 * * @param reader The SAS reader instance to destroy.
 */
void sas_arrow_reader_destroy(SasArrowReader* reader);

// Utility functions for error handling

/**
 * Converts a `SasArrowErrorCode` to a human-readable string message.
 * * @param error_code The error code.
 * @return A static string describing the error.
 */
const char* sas_arrow_error_message(SasArrowErrorCode error_code);

/**
 * Checks if a `SasArrowErrorCode` indicates success.
 * * @param error_code The error code to check.
 * @return `true` if `error_code` is `SAS_ARROW_OK`, `false` otherwise.
 */
bool sas_arrow_is_ok(SasArrowErrorCode error_code);

#ifdef __cplusplus
}
#endif

#endif // CPPSAS7BDAT_ARROW_FFI_H
