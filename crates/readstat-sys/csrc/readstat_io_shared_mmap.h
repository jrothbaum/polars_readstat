#ifndef READSTAT_IO_SHARED_MMAP_H
#define READSTAT_IO_SHARED_MMAP_H

#include "../vendor/ReadStat/src/readstat.h"
#include <stddef.h>

// Opaque pointer to Rust-managed mmap
typedef struct shared_mmap_s shared_mmap_t;

typedef struct shared_mmap_io_ctx_s {
    shared_mmap_t *shared_mmap;
    size_t         pos;
} shared_mmap_io_ctx_t;

// These will be implemented in Rust and exposed via FFI
shared_mmap_t *create_shared_mmap(const char *file_path);
void retain_shared_mmap(shared_mmap_t *shared_mmap);
void release_shared_mmap(shared_mmap_t *shared_mmap);
size_t get_mmap_size(shared_mmap_t *shared_mmap);
const char *get_mmap_ptr(shared_mmap_t *shared_mmap);

readstat_error_t shared_mmap_io_init(readstat_parser_t *parser, shared_mmap_t *shared_mmap);

#endif