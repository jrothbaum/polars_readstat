/**
 *  \file tests/data.hpp
 *
 *  \brief
 *
 *  \author  Olivia Quinet
 */

#include <algorithm>
#include <filesystem>
#include <nlohmann/json.hpp>
#include <iostream>
#include <fstream>

namespace {

inline std::string convert_path(std::string _filename) {
  std::cerr << "convert_path:" << '[' << _filename << ']';
  std::filesystem::path p{_filename};
  _filename = p.make_preferred().string();
  std::cerr << ':' << '[' << _filename << ']' << std::endl;
  return _filename;
}

} // namespace

const std::string invalid_path = convert_path("data/invalid_path.sas7bdat");
const std::string file_too_short = convert_path("data/file_too_short.err");
const std::string invalid_magic_number =
    convert_path("data/invalid_magic_number.err");
const std::string file1 = convert_path("data/file1.sas7bdat");

using json = nlohmann::json;

namespace {
struct FILES {
  explicit FILES(const std::string &_filename) {
    std::ifstream is(_filename.c_str());
    if (!is)
      throw std::runtime_error("Cannot read filename in test");
    is >> j;
  }
  json j;
};

static const FILES &files() {
  static const FILES instance(convert_path("files.json"));
  return instance;
}

using namespace cppsas7bdat;

auto get_string = [](auto j) {
  return j.is_null() ? std::string() : std::string(j);
};
auto get_platform = [](const auto &_platform) {
  if (_platform.is_null())
    return cppsas7bdat::Platform::unknown;
  return _platform == "unix" ? cppsas7bdat::Platform::unix
                             : cppsas7bdat::Platform::windows;
};
auto get_datetime = [](const auto &_x) -> boost::posix_time::ptime {
  using namespace boost::posix_time;
  if (_x.is_null())
    return not_a_date_time;
  std::string date = _x;
  const auto ts = time_from_string(date);
  return ts;
};

auto get_dt =
    [](const boost::posix_time::ptime ts) -> boost::posix_time::ptime {
  using namespace boost::posix_time;
  const auto d = ts.date();
  const auto tod = ts.time_of_day();
  return ptime(d, time_duration(tod.hours(), tod.minutes(), tod.seconds(), 0));
};

auto get_date = [](const auto &_date) -> boost::gregorian::date {
  using namespace boost::gregorian;
  if (_date.is_null())
    return date(not_a_date_time);
  const auto d = from_string(_date);
  return d;
};
auto get_time = [](const auto &_date) -> boost::posix_time::time_duration {
  using namespace boost::posix_time;
  if (_date.is_null())
    return not_a_date_time;
  const auto t = duration_from_string(_date);
  return t;
};
auto get_compression =
    [](const auto &_compression) -> cppsas7bdat::Compression {
  if (_compression.is_null())
    return cppsas7bdat::Compression::none;
  return (_compression == "SASYZCR2"
              ? cppsas7bdat::Compression::RDC
              : _compression == "SASYZCRL" ? cppsas7bdat::Compression::RLE
                                           : cppsas7bdat::Compression::none);
};
auto get_column_type = [](const auto &_type) -> cppsas7bdat::Column::Type {
  if (_type.is_null())
    return cppsas7bdat::Column::Type::unknown;
  return (_type == "string"
              ? cppsas7bdat::Column::Type::string
              : _type == "number" ? cppsas7bdat::Column::Type::number
                                  : cppsas7bdat::Column::Type::unknown);
};
auto get_format = [](const bool _u64) -> cppsas7bdat::Format {
  return _u64 ? cppsas7bdat::Format::bit64 : cppsas7bdat::Format::bit32;
};

auto get_endianness = [](const std::string &_endianness) {
  return _endianness == "little" ? cppsas7bdat::Endian::little
                                 : cppsas7bdat::Endian::big;
};
} // namespace
