#ifndef CPP_SAS7BDAT_C_API_H
#define CPP_SAS7BDAT_C_API_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque handles
typedef void* ChunkedReaderHandle;
typedef void* ChunkIteratorHandle;

// Column value for FFI
typedef struct {
    const char* string_val;
    double numeric_val;
    uint8_t value_type; // 0=null, 1=string, 2=numeric, 3=date, etc. (matches SasColumnType in Rust conceptually)
    uint8_t is_null;
} CColumnValue;

// Row data
typedef struct {
    CColumnValue* values;
    size_t column_count;
} CRowData;

// Chunk information
typedef struct {
    size_t row_count;
    size_t start_row;
    size_t end_row;
} CChunkInfo;

// Column metadata
typedef struct {
    const char* name;
    uint8_t column_type; // 0=string, 1=numeric, 2=date, etc. (matches SasColumnType in Rust conceptually)
    size_t length;
} CColumnInfo;

// Properties/metadata
typedef struct {
    CColumnInfo* columns;
    size_t column_count;
    size_t total_rows; // If known, 0 if unknown
} CProperties;

// Core API functions
ChunkedReaderHandle chunked_reader_create(const char* filename, size_t chunk_size);
int chunked_reader_get_properties(ChunkedReaderHandle handle, CProperties* properties);
int chunked_reader_next_chunk(ChunkedReaderHandle handle, CChunkInfo* chunk_info);
int chunked_reader_has_chunk(ChunkedReaderHandle handle);
void chunked_reader_destroy(ChunkedReaderHandle handle);

// Chunk iterator functions
ChunkIteratorHandle chunk_iterator_create(ChunkedReaderHandle reader_handle);
int chunk_iterator_next_row(ChunkIteratorHandle handle, CRowData* row_data);
int chunk_iterator_has_next(ChunkIteratorHandle handle);
void chunk_iterator_destroy(ChunkIteratorHandle handle);

// Memory management
void free_row_data(CRowData* row_data);
void free_properties(CProperties* properties);

#ifdef __cplusplus
}
#endif

#endif // CPP_SAS7BDAT_C_API_H