#include "readstat_io_shared_mmap.h"
#include <stdlib.h>
#include <string.h>

// IO handlers that call into Rust-managed mmap
int shared_mmap_open_handler(const char *ignored, void *io_ctx) {
    shared_mmap_io_ctx_t *ctx = (shared_mmap_io_ctx_t*)io_ctx;
    ctx->pos = 0;
    return READSTAT_HANDLER_OK;
}

int shared_mmap_close_handler(void *io_ctx) {
    shared_mmap_io_ctx_t *ctx = (shared_mmap_io_ctx_t*)io_ctx;
    ctx->pos = 0;
    return READSTAT_HANDLER_OK;
}

readstat_off_t shared_mmap_seek_handler(readstat_off_t offset, readstat_io_flags_t whence, void *io_ctx) {
    shared_mmap_io_ctx_t *ctx = (shared_mmap_io_ctx_t*)io_ctx;
    size_t file_size = get_mmap_size(ctx->shared_mmap);
    readstat_off_t new_pos = 0;
    
    switch(whence) {
        case READSTAT_SEEK_SET: new_pos = offset; break;
        case READSTAT_SEEK_CUR: new_pos = ctx->pos + offset; break;
        case READSTAT_SEEK_END: new_pos = file_size + offset; break;
        default: return -1;
    }
    
    if (new_pos < 0 || (size_t)new_pos > file_size) {
        return -1;
    }
    
    ctx->pos = new_pos;
    return ctx->pos;
}

ssize_t shared_mmap_read_handler(void *buf, size_t nbyte, void *io_ctx) {
    shared_mmap_io_ctx_t *ctx = (shared_mmap_io_ctx_t*)io_ctx;
    size_t file_size = get_mmap_size(ctx->shared_mmap);
    const char *mmap_ptr = get_mmap_ptr(ctx->shared_mmap);
    
    if (ctx->pos >= file_size) {
        return 0;
    }
    
    ssize_t available = file_size - ctx->pos;
    if ((ssize_t)nbyte > available) {
        nbyte = available > 0 ? available : 0;
    }
    
    if (nbyte <= 0) {
        return 0;
    }
    
    memcpy(buf, mmap_ptr + ctx->pos, nbyte);
    ctx->pos += nbyte;
    
    return nbyte;
}

readstat_error_t shared_mmap_update_handler(long file_size, readstat_progress_handler progress_handler, void *user_ctx, void *io_ctx) {
    if (!progress_handler) {
        return READSTAT_OK;
    }
    
    shared_mmap_io_ctx_t *ctx = (shared_mmap_io_ctx_t*)io_ctx;
    double progress = (double)ctx->pos / file_size;
    
    if (progress_handler(progress, user_ctx)) {
        return READSTAT_ERROR_USER_ABORT;
    }
    
    return READSTAT_OK;
}

readstat_error_t shared_mmap_io_init(readstat_parser_t *parser, shared_mmap_t *shared_mmap) {
    readstat_error_t retval = READSTAT_OK;
    
    if ((retval = readstat_set_open_handler(parser, shared_mmap_open_handler)) != READSTAT_OK)
        return retval;
    if ((retval = readstat_set_close_handler(parser, shared_mmap_close_handler)) != READSTAT_OK)
        return retval;
    if ((retval = readstat_set_seek_handler(parser, shared_mmap_seek_handler)) != READSTAT_OK)
        return retval;
    if ((retval = readstat_set_read_handler(parser, shared_mmap_read_handler)) != READSTAT_OK)
        return retval;
    if ((retval = readstat_set_update_handler(parser, shared_mmap_update_handler)) != READSTAT_OK)
        return retval;
    
    shared_mmap_io_ctx_t *io_ctx = calloc(1, sizeof(shared_mmap_io_ctx_t));
    if (!io_ctx) {
        return READSTAT_ERROR_MALLOC;
    }
    
    io_ctx->shared_mmap = shared_mmap;
    io_ctx->pos = 0;
    
    retain_shared_mmap(shared_mmap);
    
    retval = readstat_set_io_ctx(parser, (void*)io_ctx);
    parser->io->io_ctx_needs_free = 1;
    
    return retval;
}