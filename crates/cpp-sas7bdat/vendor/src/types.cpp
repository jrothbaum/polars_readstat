/**
 *  \file src/types.cpp
 *
 *  \brief Types file
 *
 *  \author Olivia Quinet
 */

#include <cppsas7bdat/column.hpp>
#include <cppsas7bdat/types.hpp>
#include <fmt/core.h>

namespace cppsas7bdat {

std::string to_string(DATETIME _x) {
  char szBuffer[32];
  size_t size{0};
  const auto d = _x.date();
  const auto t = _x.time_of_day();
  if (t.fractional_seconds()) {
    auto r = fmt::format_to_n(szBuffer, sizeof(szBuffer),
                              "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
                              d.year(), d.month(), d.day(), t.hours(),
                              t.minutes(), t.seconds(), t.fractional_seconds());
    size = r.size;
  } else {
    auto r = fmt::format_to_n(
        szBuffer, sizeof(szBuffer), "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        d.year(), d.month(), d.day(), t.hours(), t.minutes(), t.seconds());
    size = r.size;
  }
  return std::string(szBuffer, size);
}

std::string to_string(DATE _x) {
  char szBuffer[16];
  size_t size{0};
  auto r = fmt::format_to_n(szBuffer, sizeof(szBuffer), "{:04}-{:02}-{:02}",
                            _x.year(), _x.month(), _x.day());
  size = r.size;
  return std::string(szBuffer, size);
}

std::string to_string(TIME _x) {
  char szBuffer[16];
  size_t size{0};
  if (_x.fractional_seconds()) {
    auto r = fmt::format_to_n(
        szBuffer, sizeof(szBuffer), "{:02}:{:02}:{:02}.{:06}", _x.hours(),
        _x.minutes(), _x.seconds(), _x.fractional_seconds());
    size = r.size;
  } else {
    auto r = fmt::format_to_n(szBuffer, sizeof(szBuffer), "{:02}:{:02}:{:02}",
                              _x.hours(), _x.minutes(), _x.seconds());
    size = r.size;
  }
  return std::string(szBuffer, size);
}

std::string_view to_string(const Endian _x) {
  switch (_x) {
  case Endian::little:
    return "little";
  case Endian::big:
    return "big";
  default:
    return "unknown";
  }
}

std::string_view to_string(const Format _x) {
  switch (_x) {
  case Format::bit32:
    return "32bits";
  case Format::bit64:
    return "64bits";
  default:
    return "unknown";
  }
}

std::string_view to_string(const Platform _x) {
  switch (_x) {
  case Platform::unix:
    return "unix";
  case Platform::windows:
    return "windows";
  default:
    return "unknown";
  }
}

std::string_view to_string(const Compression _x) {
  switch (_x) {
  case Compression::none:
    return "none";
  case Compression::RLE:
    return "RLE";
  case Compression::RDC:
    return "RDC";
  default:
    return "unknown";
  }
}

std::string_view to_string(const Column::Type _x) {
  switch (_x) {
  case Column::Type::string:
    return "string";
  case Column::Type::number:
    return "number";
  case Column::Type::integer:
    return "integer";
  case Column::Type::datetime:
    return "datetime";
  case Column::Type::date:
    return "date";
  case Column::Type::time:
    return "time";
  default:
    return "unknown";
  }
}

std::ostream &operator<<(std::ostream &os, const Endian _x) {
  os << to_string(_x);
  return os;
}

std::ostream &operator<<(std::ostream &os, const Format _x) {
  os << to_string(_x);
  return os;
}

std::ostream &operator<<(std::ostream &os, const Platform _x) {
  os << to_string(_x);
  return os;
}

std::ostream &operator<<(std::ostream &os, const Compression _x) {
  os << to_string(_x);
  return os;
}

std::ostream &operator<<(std::ostream &os, const Column::Type _x) {
  os << to_string(_x);
  return os;
}

} // namespace cppsas7bdat
