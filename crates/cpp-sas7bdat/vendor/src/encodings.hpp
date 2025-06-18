/**
 *  \file src/encodings.hpp
 *
 *  \brief List of encodings
 *
 *  \author Olivia Quinet
 */

#ifndef _CPP_SAS7BDAT_SRC_ENCODINGS_HPP_
#define _CPP_SAS7BDAT_SRC_ENCODINGS_HPP_

#include <cstdint>
#include <string_view>

namespace cppsas7bdat {
namespace INTERNAL {

std::string_view get_encoding(const uint8_t _e) noexcept;

} // namespace INTERNAL
} // namespace cppsas7bdat

#endif
