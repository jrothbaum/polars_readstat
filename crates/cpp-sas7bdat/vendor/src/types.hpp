/**
 *  \file src/types.hpp
 *
 *  \brief Managing types
 *
 *  \author Olivia Quinet
 */

#ifndef _CPP_SAS7BDAT_SRC_TYPES_HPP_
#define _CPP_SAS7BDAT_SRC_TYPES_HPP_

#include <boost/endian/conversion.hpp>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <type_traits>
//#include <endian.h>

namespace cppsas7bdat {
namespace INTERNAL {

inline std::string_view get_string(const uint8_t *_buf,
                                   const size_t _length) noexcept {
  return std::string_view(reinterpret_cast<const char *>(_buf), _length);
}

inline std::string_view get_string_trim_0(const uint8_t *_buf,
                                          size_t _length) noexcept {
  while (_length &&
         (_buf[_length - 1] == 0 || std::isspace(_buf[_length - 1]))) {
    --_length;
  }
  return get_string(_buf, _length);
}

inline std::string_view get_string_trim(const uint8_t *_buf,
                                        size_t _length) noexcept {
  size_t offset = 0;
  while (_length && std::isspace(_buf[offset])) {
    ++offset;
    --_length;
  }
  while (_length && (!std::isprint(_buf[offset + _length - 1]) ||
                     std::isspace(_buf[offset + _length - 1]))) {
    --_length;
  }
  return get_string(_buf + offset, _length);
}

using BYTE = uint8_t;
using BYTES = std::basic_string_view<BYTE>;
inline BYTES get_bytes(const uint8_t *_buf, size_t _length) noexcept {
  return BYTES(_buf, _length);
}

//#define USE_INHOUSE
#ifdef USE_INHOUSE

constexpr bool SYSTEM_IS_LITTLE_ENDIAN{__BYTE_ORDER__ ==
                                       __ORDER_LITTLE_ENDIAN__};
constexpr Endian SYSTEM_ENDIANNESS{SYSTEM_IS_LITTLE_ENDIAN ? Endian::little
                                                           : Endian::big};

struct _GET_VAL {
  template <typename _Tp> static inline _Tp get(const uint8_t *_buf) noexcept {
    _Tp x;
    std::memcpy(&x, _buf, sizeof(_Tp));
    return x;
  }

  template <typename _Tp> static inline _Tp swap(const uint8_t *_buf) noexcept {
    auto x = get<_Tp>(_buf);
    return boost::endian::endian_reverse(x);
  }
};

template <Endian _endian_from, Endian _endian_to = SYSTEM_ENDIANNESS>
struct ENDIAN {
  template <typename _Tp>
  static inline _Tp get_val(const uint8_t *_buf) noexcept {
    if constexpr (_endian_from == _endian_to)
      return _GET_VAL::template get<_Tp>(_buf);
    else
      return _GET_VAL::template swap<_Tp>(_buf);
  }
};

#else

template <Endian _endian> struct ENDIAN {
  template <typename _Tp>
  static inline _Tp get_val(const uint8_t *_buf) noexcept {
    // const _Tp* x = reinterpret_cast<const _Tp*>(_buf);  // UB:
    // https://en.cppreference.com/w/cpp/language/reinterpret_cast
    _Tp x;
    std::memcpy(&x, _buf, sizeof(_Tp));
    if constexpr (_endian == Endian::big)
      return boost::endian::big_to_native(x);
    else
      return boost::endian::little_to_native(x);
  }
};

#endif

template <Endian _endian, typename _Tp>
inline _Tp get_val(const uint8_t *_buf) noexcept {
  return ENDIAN<_endian>::template get_val<_Tp>(_buf);
}

template <Endian _endian>
inline double get_double(const uint8_t *_buf) noexcept {
  // return *(reinterpret_cast<ODATA*>(&x)); // UB:
  // https://en.cppreference.com/w/cpp/language/reinterpret_cast
  using IDATA = uint64_t;
  using ODATA = double;
  static_assert(sizeof(IDATA) == sizeof(ODATA));
  const IDATA x = get_val<_endian, IDATA>(_buf);
  ODATA d;
  std::memcpy(&d, &x, sizeof(d));
  return d;
}

template <Endian _endian, int _nbits>
inline double get_incomplete_double(const uint8_t *_buf) noexcept {
  constexpr int n = sizeof(int64_t);
  alignas(n) uint8_t buf[n];
  if constexpr (_endian == Endian::big) {
    for (int i = 0; i < _nbits; ++i)
      buf[i] = _buf[i];
    for (int i = _nbits; i < n; ++i)
      buf[i] = 0;
  } else {
    constexpr int nn = n - _nbits;
    for (int i = 0; i < nn; ++i)
      buf[i] = 0;
    for (int i = nn; i < n; ++i)
      buf[i] = _buf[i - nn];
  }
  return get_double<_endian>(buf);
}

inline DATETIME get_datetime_from_epoch(double _seconds) noexcept {
  using namespace boost::posix_time;
  using namespace boost::gregorian;
  if (std::isnan(_seconds))
    return not_a_date_time;
  ptime start(boost::gregorian::date(1960, 1, 1));
  // Increase accuracy by splitting in days/seconds/microseconds
  constexpr double seconds_in_a_day{24 * 60 * 60};
  const auto _days = std::round(_seconds / seconds_in_a_day);
  _seconds -= _days * seconds_in_a_day;
  const auto _secs = std::round(_seconds);
  const auto _microseconds = std::round((_seconds - _secs) * 1e6);
  return start + days(std::lround(_days)) + seconds(std::lround(_secs)) +
         microseconds(std::lround(_microseconds));
}

inline TIME get_time_from_epoch(const double _seconds) noexcept {
  return get_datetime_from_epoch(_seconds).time_of_day();
}

inline DATE get_date_from_epoch_seconds(const double _seconds) noexcept {
  return get_datetime_from_epoch(_seconds).date();
}

inline DATE get_date_from_epoch_days(const double _days) noexcept {
  using namespace boost::posix_time;
  using namespace boost::gregorian;
  if (std::isnan(_days))
    return DATE(not_a_date_time);
  ptime start(boost::gregorian::date(1960, 1, 1));
  return (start + days(std::lround(_days))).date();
}

inline DATE get_date_from_epoch(const double _t) noexcept {
  const auto x = get_date_from_epoch_days(_t);
  if (x.is_not_a_date())
    return get_date_from_epoch_seconds(_t);
  else
    return x;
}

template <Format _format> struct number_types {};

template <> struct number_types<Format::bit64> {
  using integer_t = int64_t;
  using uinteger_t = uint64_t;
};

template <> struct number_types<Format::bit32> {
  using integer_t = int32_t;
  using uinteger_t = uint32_t;
};

} // namespace INTERNAL
} // namespace cppsas7bdat

#endif
