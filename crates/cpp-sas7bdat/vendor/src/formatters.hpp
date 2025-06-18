/**
 *  \file src/formatters.hpp
 *
 *  \brief Column Value Formatters
 *
 *  \author Olivia Quinet
 */

#ifndef _CPP_SAS7BDAT_SRC_FORMATTERS_HPP_
#define _CPP_SAS7BDAT_SRC_FORMATTERS_HPP_

#include <limits>
#include <string>

namespace cppsas7bdat {
namespace INTERNAL {
namespace FORMATTER {

using Type = Column::Type;

struct IFormatter {
  const size_t offset{0};
  const size_t length{0};
  const Type type{Type::unknown};

  IFormatter(const size_t _offset, const size_t _length, const Type _type)
      : offset(_offset), length(_length), type(_type) {}

  SV get_string([[maybe_unused]] const void *_p) const noexcept { return {}; }
  NUMBER get_number([[maybe_unused]] const void *_p) const noexcept {
    return std::numeric_limits<NUMBER>::quiet_NaN();
  }
  INTEGER get_integer([[maybe_unused]] const void *_p) const noexcept {
    return {};
  }
  DATETIME get_datetime([[maybe_unused]] const void *_p) const noexcept {
    return {};
  }
  DATE get_date([[maybe_unused]] const void *_p) const noexcept { return {}; }
  TIME get_time([[maybe_unused]] const void *_p) const noexcept {
    return TIME(boost::posix_time::not_a_date_time);
  }
  STRING to_string([[maybe_unused]] const void *_p) const { return {}; }

  const uint8_t *data(const void *_p) const noexcept {
    const uint8_t *p = reinterpret_cast<const uint8_t *>(_p) + offset;
    return p;
  }
};

struct NoFormatter : public IFormatter {
  NoFormatter(const size_t _offset, const size_t _length)
      : IFormatter(_offset, _length, Type::unknown) {
    D(spdlog::debug("NoFormatter: {}:{}\n", offset, length));
  }
};

struct StringFormatter : public IFormatter {
  StringFormatter(const size_t _offset, const size_t _length)
      : IFormatter(_offset, _length, Type::string) {
    D(spdlog::debug("StringFormatter\n"));
  }

  SV get_string(const void *_p) const noexcept {
    return INTERNAL::get_string_trim_0(data(_p), length);
  }

  STRING to_string(const void *_p) const { return STRING(get_string(_p)); }
};

struct SmallIntegerFormatter : public IFormatter {
  SmallIntegerFormatter(const size_t _offset, const size_t _length)
      : IFormatter(_offset, _length, Type::integer) {
    D(spdlog::debug("SmallIntegerFormatter\n"));
  }

  INTEGER get_integer(const void *_p) const noexcept { return *data(_p); }

  NUMBER get_number(const void *_p) const noexcept { return get_integer(_p); }

  STRING to_string(const void *_p) const {
    return std::to_string(get_integer(_p));
  }
};

template <Endian _endian, typename _Tp>
struct IntegerFormatter : public IFormatter {
  IntegerFormatter(const size_t _offset, const size_t _length)
      : IFormatter(_offset, _length, Type::integer) {
    D(spdlog::debug("IntegerFormatter\n"));
  }

  INTEGER get_integer(const void *_p) const noexcept {
    return INTERNAL::get_val<_endian, _Tp>(data(_p));
  }

  NUMBER get_number(const void *_p) const noexcept { return get_integer(_p); }

  STRING to_string(const void *_p) const {
    return std::to_string(get_integer(_p));
  }
};

template <Endian _endian> struct DoubleFormatter : public IFormatter {
  DoubleFormatter(const size_t _offset, const size_t _length,
                  const Type _type = Type::number)
      : IFormatter(_offset, _length, _type) {
    D(spdlog::debug("DoubleFormatter\n"));
  }

  NUMBER get_number([[maybe_unused]] const void *_p) const noexcept {
    return INTERNAL::get_double<_endian>(data(_p));
  }

  STRING to_string(const void *_p) const {
    return std::to_string(get_number(_p));
  }
};

template <Endian _endian, int _nbits>
struct IncompleteDoubleFormatter : public IFormatter {
  IncompleteDoubleFormatter(const size_t _offset, const size_t _length)
      : IFormatter(_offset, _length, Type::number) {
    D(spdlog::debug("IncompleteDoubleFormatter\n"));
  }

  NUMBER get_number([[maybe_unused]] const void *_p) const noexcept {
    return INTERNAL::get_incomplete_double<_endian, _nbits>(data(_p));
  }

  STRING to_string(const void *_p) const {
    return std::to_string(get_number(_p));
  }
};

template <Endian _endian>
struct DateTimeFormatter : public DoubleFormatter<_endian> {
  DateTimeFormatter(const size_t _offset, const size_t _length)
      : DoubleFormatter<_endian>(_offset, _length, Type::datetime) {
    D(spdlog::debug("DateTimeFormatter\n"));
  }

  DATETIME get_datetime([[maybe_unused]] const void *_p) const noexcept {
    return INTERNAL::get_datetime_from_epoch(
        DoubleFormatter<_endian>::get_number(_p));
  }

  DATE get_date([[maybe_unused]] const void *_p) const noexcept {
    return get_datetime(_p).date();
  }

  TIME get_time([[maybe_unused]] const void *_p) const noexcept {
    return get_datetime(_p).time_of_day();
  }

  STRING to_string(const void *_p) const {
    return cppsas7bdat::to_string(get_datetime(
        _p)); // boost::posix_time::to_iso_extended_string(get_datetime(_p));
  }
};

template <Endian _endian>
struct DateFormatter : public DoubleFormatter<_endian> {
  DateFormatter(const size_t _offset, const size_t _length)
      : DoubleFormatter<_endian>(_offset, _length, Type::date) {
    D(spdlog::debug("DateTimeFormatter\n"));
  }

  DATE get_date([[maybe_unused]] const void *_p) const noexcept {
    return INTERNAL::get_date_from_epoch(
        DoubleFormatter<_endian>::get_number(_p));
  }

  DATETIME get_datetime([[maybe_unused]] const void *_p) const noexcept {
    return DATETIME(get_date(_p), {});
  }

  TIME get_time([[maybe_unused]] const void *_p) const noexcept { return {}; }

  STRING to_string(const void *_p) const {
    return cppsas7bdat::to_string(get_date(
        _p)); // boost::gregorian::to_iso_extended_string(get_date(_p));
  }
};

template <Endian _endian>
struct TimeFormatter : public DoubleFormatter<_endian> {
  TimeFormatter(const size_t _offset, const size_t _length)
      : DoubleFormatter<_endian>(_offset, _length, Type::time) {
    D(spdlog::debug("DateTimeFormatter\n"));
  }

  TIME get_time([[maybe_unused]] const void *_p) const noexcept {
    return INTERNAL::get_time_from_epoch(
        DoubleFormatter<_endian>::get_number(_p));
  }

  DATETIME get_datetime([[maybe_unused]] const void *_p) const noexcept {
    return DATETIME({}, get_time(_p));
  }

  STRING to_string(const void *_p) const {
    return cppsas7bdat::to_string(
        get_time(_p)); // boost::posix_time::to_simple_string(get_time(_p));
  }
};

} // namespace FORMATTER
} // namespace INTERNAL
} // namespace cppsas7bdat

#endif
