/**
 *  \file cppsas7bdat/types.hpp
 *
 *  \brief Types description
 *
 *  \author Olivia Quinet
 */

#ifndef _CPP_SAS7BDAT_TYPES_HPP_
#define _CPP_SAS7BDAT_TYPES_HPP_

#include <cstdint>
#include <iosfwd>
#include <string>
#include <string_view>

#include <boost/date_time/gregorian/gregorian_types.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

namespace cppsas7bdat {

using STRING = std::string;
using SV = std::string_view;
using NUMBER = double;
using INTEGER = int32_t;
using DATETIME = boost::posix_time::ptime;
using DATE = boost::gregorian::date;
using TIME = boost::posix_time::time_duration;

enum class Endian { unknown, little, big };
enum class Format { bit32, bit64 };
enum class Platform { unknown, unix, windows };
enum class Compression { none, RLE, RDC };

std::string_view to_string(const Endian _x);
std::string_view to_string(const Format _x);
std::string_view to_string(const Platform _x);
std::string_view to_string(const Compression _x);

std::string to_string(DATETIME);
std::string to_string(DATE);
std::string to_string(TIME);

std::ostream &operator<<(std::ostream &os, const Endian _x);
std::ostream &operator<<(std::ostream &os, const Format _x);
std::ostream &operator<<(std::ostream &os, const Platform _x);
std::ostream &operator<<(std::ostream &os, const Compression _x);
} // namespace cppsas7bdat

#endif
