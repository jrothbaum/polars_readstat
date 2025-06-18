/**
 *  \file src/exceptions.hpp
 *
 *  \brief List of exceptions
 *
 *  \author Olivia Quinet
 */

#ifndef _CPP_SAS7BDAT_SRC_EXCEPTIONS_HPP_
#define _CPP_SAS7BDAT_SRC_EXCEPTIONS_HPP_

#include <cstddef>

namespace cppsas7bdat {
namespace EXCEPTION {
void cannot_allocate_memory();
void not_a_valid_file(const char *_pcszFileName);
void header_too_short();
void invalid_magic_number();
void cannot_read_page();
void cannot_decompress();
void invalid_buffer_access(const size_t _offset, const size_t _n,
                           const size_t _size);
} // namespace EXCEPTION
} // namespace cppsas7bdat

#endif
