/**
 *  \file src/exceptions.cpp
 *
 *  \brief Exceptions
 *
 *  \author Olivia Quinet
 */

#include "exceptions.hpp"
#include <fmt/core.h>
#include <spdlog/spdlog.h>
#include <stdexcept>

namespace cppsas7bdat {
void raise_exception(const std::string &_msg) {
  spdlog::critical(_msg);
  throw std::runtime_error(_msg);
}

void EXCEPTION::cannot_allocate_memory() {
  raise_exception("cannot_allocate_memory");
}

void EXCEPTION::not_a_valid_file(const char *_pcszFileName) {
  raise_exception(fmt::format("not_a_valid_file: [{}]", _pcszFileName));
}

void EXCEPTION::header_too_short() { raise_exception("header_too_short"); }

void EXCEPTION::invalid_magic_number() {
  raise_exception("invalid_magic_number");
}

void EXCEPTION::cannot_read_page() { raise_exception("cannot_read_page"); }

void EXCEPTION::cannot_decompress() { raise_exception("cannot_decompress"); }

void EXCEPTION::invalid_buffer_access(const size_t _offset, const size_t _n,
                                      const size_t _size) {
  raise_exception(
      fmt::format("invalid_buffer_access: {}+{}>{}", _offset, _n, _size));
}
} // namespace cppsas7bdat
