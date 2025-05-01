#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include "readstat.h"
#include "readstat_read_column.h"

// Type-specific value handlers
static void handle_string_value(char **data, int index, readstat_value_t value) {
    const char *str_val = readstat_string_value(value);
    if (str_val) {
        data[index] = strdup(str_val);
    }
}

static void handle_int8_value(int8_t *data, int index, readstat_value_t value) {
    data[index] = readstat_int8_value(value);
}

static void handle_int16_value(int16_t *data, int index, readstat_value_t value) {
    data[index] = readstat_int16_value(value);
}

static void handle_int32_value(int32_t *data, int index, readstat_value_t value) {
    data[index] = readstat_int32_value(value);
}

static void handle_float_value(float *data, int index, readstat_value_t value) {
    data[index] = readstat_float_value(value);
}

static void handle_double_value(double *data, int index, readstat_value_t value) {
    data[index] = readstat_double_value(value);
}

// Define function pointer type for value handlers
typedef void (*value_handler_func)(void *data, int index, readstat_value_t value);

// Column context with function pointer
typedef struct {
    int target_column;            // Column index we're reading
    readstat_column_t *result;    // Where to store the result
    value_handler_func handler;   // Type-specific handler function
} column_read_ctx_t;

// Value handler bridge functions that cast to the right type
static void handle_string_bridge(void *data, int index, readstat_value_t value) {
    handle_string_value((char **)data, index, value);
}

static void handle_int8_bridge(void *data, int index, readstat_value_t value) {
    handle_int8_value((int8_t *)data, index, value);
}

static void handle_int16_bridge(void *data, int index, readstat_value_t value) {
    handle_int16_value((int16_t *)data, index, value);
}

static void handle_int32_bridge(void *data, int index, readstat_value_t value) {
    handle_int32_value((int32_t *)data, index, value);
}

static void handle_float_bridge(void *data, int index, readstat_value_t value) {
    handle_float_value((float *)data, index, value);
}

static void handle_double_bridge(void *data, int index, readstat_value_t value) {
    handle_double_value((double *)data, index, value);
}

// Get appropriate handler function based on type
static value_handler_func get_handler_for_type(readstat_type_t type) {
    switch (type) {
        case READSTAT_TYPE_STRING:
            return handle_string_bridge;
        case READSTAT_TYPE_INT8:
            return handle_int8_bridge;
        case READSTAT_TYPE_INT16:
            return handle_int16_bridge;
        case READSTAT_TYPE_INT32:
            return handle_int32_bridge;
        case READSTAT_TYPE_FLOAT:
            return handle_float_bridge;
        case READSTAT_TYPE_DOUBLE:
        default:
            return handle_double_bridge;
    }
}

// Value handler for column reading
static int column_value_handler(int obs_index, readstat_variable_t *variable, 
                               readstat_value_t value, void *ctx) {
    column_read_ctx_t *col_ctx = (column_read_ctx_t *)ctx;
    int var_index = readstat_variable_get_index(variable);
    
    // Skip if not our target column
    if (var_index != col_ctx->target_column) {
        return READSTAT_HANDLER_OK;
    }
    
    // Check bounds
    if (obs_index >= col_ctx->result->rows) {
        return READSTAT_ERROR_ROW_COUNT_MISMATCH;
    }
    
    // Set missing indicator
    uint8_t is_missing = readstat_value_is_system_missing(value) || 
                      readstat_value_is_tagged_missing(value);
    col_ctx->result->missing[obs_index] = is_missing;
    
    // If not missing, use type-specific handler
    if (!is_missing) {
        col_ctx->handler(col_ctx->result->data, obs_index, value);
    }
    
    return READSTAT_HANDLER_OK;
}

// Function to prepare a column reader
readstat_column_t* readstat_column_init(readstat_variable_t *variable, int max_rows) {
    if (!variable || max_rows <= 0) {
        return NULL;
    }
    
    // Create result structure
    readstat_column_t *result = calloc(1, sizeof(readstat_column_t));
    if (!result) return NULL;
    
    result->type = readstat_variable_get_type(variable);
    result->rows = max_rows;
    
    // Allocate missing values array
    result->missing = calloc(result->rows, sizeof(uint8_t));
    if (!result->missing) {
        free(result);
        return NULL;
    }
    
    // Allocate data array based on type
    size_t element_size;
    switch (result->type) {
        case READSTAT_TYPE_STRING:
            element_size = sizeof(char*);
            break;
        case READSTAT_TYPE_INT8:
            element_size = sizeof(int8_t);
            break;
        case READSTAT_TYPE_INT16:
            element_size = sizeof(int16_t);
            break;
        case READSTAT_TYPE_INT32:
            element_size = sizeof(int32_t);
            break;
        case READSTAT_TYPE_FLOAT:
            element_size = sizeof(float);
            break;
        case READSTAT_TYPE_DOUBLE:
        default:
            element_size = sizeof(double);
            break;
    }
    
    result->data = calloc(result->rows, element_size);
    if (!result->data) {
        free(result->missing);
        free(result);
        return NULL;
    }
    
    return result;
}

// Function to setup column reading on a parser
int readstat_column_read(
    readstat_parser_t *parser,
    readstat_variable_t *variable,
    readstat_column_t *column
) {
    if (!parser || !variable || !column) {
        return READSTAT_ERROR_OPEN;
    }
    
    // Get the appropriate handler function
    value_handler_func handler = get_handler_for_type(column->type);
    
    // Set up context for column reading
    column_read_ctx_t *ctx = calloc(1, sizeof(column_read_ctx_t));
    if (!ctx) {
        return READSTAT_ERROR_MALLOC;
    }
    
    ctx->target_column = readstat_variable_get_index(variable);
    ctx->result = column;
    ctx->handler = handler;
    
    // Set up the value handler
    readstat_error_t error = readstat_set_value_handler(parser, column_value_handler);
    if (error != READSTAT_OK) {
        free(ctx);
        return error;
    }
    
    // Save the context in the parser's user_ctx
    readstat_set_user_ctx(parser, ctx);
    
    return READSTAT_OK;
}

// Free column data
void readstat_column_free(readstat_column_t *column) {
    if (!column) return;
    
    // For string type, free each string
    if (column->type == READSTAT_TYPE_STRING) {
        char **strings = (char**)column->data;
        for (int i = 0; i < column->rows; i++) {
            free(strings[i]);
        }
    }
    
    free(column->data);
    free(column->missing);
    free(column);
}