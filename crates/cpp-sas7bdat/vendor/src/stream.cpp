/**
 *  \file src/stream.cpp
 *
 *  \brief Stream
 *
 *  \author Olivia Quinet
 */

#include "stream.hpp"
#include "debug.hpp"
#include "exceptions.hpp"
#include <fmt/core.h>
#include <spdlog/spdlog.h>

std::ifstream cppsas7bdat::INTERNAL::open_stream(const char *_pcszFileName) {
  D(spdlog::info(fmt::format("open_stream: [{}]", _pcszFileName)));
  std::ifstream is(_pcszFileName, std::ios::binary);
  if (!is.good())
    EXCEPTION::not_a_valid_file(_pcszFileName);
  else {
    D(spdlog::critical(
        fmt::format("open_stream: [{}] is valid", _pcszFileName)));
  }
  return is;
}
