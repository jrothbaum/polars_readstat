#ifndef READSTAT_COLUMN_H
#define READSTAT_COLUMN_H

#include <stdint.h>
#include "readstat.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Structure for column data that will be returned to Rust
 */
typedef struct {
    void *data;         /* Typed data array */
    uint8_t *missing;   /* Missing value indicators */
    int type;           /* Column type (matches readstat_type_t) */
    int rows;           /* Number of rows */
    int error_code;     /* Error code (if any) */
} readstat_column_t;

/**
 * Initializes a column storage structure
 * 
 * @param variable ReadStat variable to use for type information
 * @param max_rows Maximum number of rows to allocate (usually your chunk size)
 * @return A newly allocated readstat_column_t structure, or NULL on error
 */
readstat_column_t* readstat_column_init(readstat_variable_t *variable, int max_rows);

/**
 * Sets up a parser to read a specific column into a column storage structure
 * 
 * @param parser ReadStat parser to configure
 * @param variable ReadStat variable to read
 * @param column Column storage structure to fill
 * @return READSTAT_OK on success, or an error code on failure
 */
int readstat_column_read(readstat_parser_t *parser, readstat_variable_t *variable, readstat_column_t *column);

/**
 * Frees all memory associated with a column storage structure
 * 
 * @param column Column structure to free
 */
void readstat_column_free(readstat_column_t *column);

#ifdef __cplusplus
}
#endif

#endif /* READSTAT_COLUMN_H */