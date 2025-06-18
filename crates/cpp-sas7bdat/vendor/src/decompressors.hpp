/**
 *  \file src/decompressors.hpp
 *
 *  \brief Decompressors
 *
 *  \author Olivia Quinet
 */

#ifndef _CPP_SAS7BDAT_SRC_DECOMPRESSORS_HPP_
#define _CPP_SAS7BDAT_SRC_DECOMPRESSORS_HPP_

namespace cppsas7bdat {
namespace INTERNAL {
namespace DECOMPRESSOR {

constexpr uint8_t C_NULL = 0x00;  /**< '\@' */
constexpr uint8_t C_SPACE = 0x20; /**< ' ' */
constexpr uint8_t C_AT = 0x40;    /**< '@' */

struct SRC_VALUES {
  const BYTES values;
  const size_t n_src;
  size_t i_src{0};

  explicit SRC_VALUES(const BYTES &_values)
      : values(_values), n_src(values.size()) {}

  auto pop() noexcept { return values[i_src++]; }
  auto pop(const size_t _n) noexcept {
    auto v = values.substr(i_src, _n);
    i_src += _n;
    return v;
  }

  bool check(const size_t _n) const noexcept { return i_src + _n <= n_src; }
  size_t remaining() const noexcept { return n_src - i_src; }
};

struct None {
  template <typename T> T operator()(T &&_values) const noexcept {
    return std::forward<T>(_values);
  }
};

template <Endian _endian, Format _format> struct DST_VALUES {
  BUFFER<_endian, _format> buf;
  const size_t n_dst{0};
  size_t i_dst{0};

  explicit DST_VALUES(const Properties::Metadata *_metadata)
      : DST_VALUES(_metadata->row_length) {}
  explicit DST_VALUES(const size_t _n) : buf(_n), n_dst(_n) {}

  DST_VALUES(const DST_VALUES &) = delete;
  DST_VALUES(DST_VALUES &&) noexcept = default;
  DST_VALUES &operator=(const DST_VALUES &) = delete;
  DST_VALUES &operator=(DST_VALUES &&) noexcept = delete;

  void reset() { i_dst = 0; }

  void fill() {
    const auto n = n_dst - i_dst;
    if (n)
      store_value(C_NULL, n);
  }

  void store_value(const uint8_t _v, const size_t _n) {
    assert_check(_n);
    buf.set(i_dst, _v, _n);
    i_dst += _n;
  }

  bool check() const noexcept { return i_dst < n_dst; }
  bool check(const size_t _n) const noexcept { return i_dst + _n <= n_dst; }
  void assert_check(const size_t _n) const {
    if (!check(_n)) {
      spdlog::critical("Invalid dst length: {}+{}>{}\n", i_dst, _n, n_dst);
      EXCEPTION::cannot_decompress();
    }
  }
};

/// SASYZCR2
template <Endian _endian, Format _format>
struct RDC : public DST_VALUES<_endian, _format> {
  using DST_VALUES<_endian, _format>::buf;
  using DST_VALUES<_endian, _format>::n_dst;
  using DST_VALUES<_endian, _format>::reset;
  using DST_VALUES<_endian, _format>::fill;
  using DST_VALUES<_endian, _format>::check;
  using DST_VALUES<_endian, _format>::assert_check;
  using DST_VALUES<_endian, _format>::store_value;
  using DST_VALUES<_endian, _format>::i_dst;

  explicit RDC(const Properties::Metadata *_metadata)
      : DST_VALUES<_endian, _format>(_metadata) {}

  void store_pattern(const size_t _offset, const size_t _n) {
    assert_check(_n);
    buf.copy(i_dst, i_dst - _offset, _n);
    i_dst += _n;
  }

  BYTES operator()(const BYTES &_values) {
    constexpr uint8_t ONE{1}, FOUR{4}, EIGHT{8};
    constexpr uint8_t THREE{3}, SIXTEEN{16}, NINETEEN{19};

    reset();

    using T = int32_t;
    SRC_VALUES src(_values);
    T ctrl_mask{0};
    T ctrl_bits{0};

    while (src.check(3) && check()) {
      D(spdlog::info("RDC({}/{},{}/{})\n", src.i_src, src.n_src, i_dst, n_dst));
      // get new load of control bits if needed
      ctrl_mask >>= ONE;
      if (ctrl_mask == 0) {
        // The 2 next lines must be performed in that order
        ctrl_bits = (static_cast<T>(src.pop())) << EIGHT;
        ctrl_bits += (static_cast<T>(src.pop()));
        ctrl_mask = 0x8000;
      }
      // just copy this char if control bit is zero
      if ((ctrl_bits & ctrl_mask) == 0) {
        store_value(src.pop(), 1);
      } else {
        // undo the compression code
        const auto val = src.pop();
        const uint8_t cmd = (val >> FOUR) & 0x0F;
        size_t cnt = val & 0x0F;
        if (cmd == 0) { // short rle
          cnt += THREE;
          store_value(src.pop(), cnt);
        } else if (cmd == 1) { // long rle
          cnt += static_cast<size_t>((static_cast<T>(src.pop()) << FOUR) +
                                     NINETEEN);
          store_value(src.pop(), cnt);
        } else if (cmd == 2) { // long pattern
          const size_t ofs =
              cnt + THREE +
              static_cast<size_t>(static_cast<T>(src.pop()) << FOUR);
          cnt = static_cast<size_t>(src.pop() + SIXTEEN);
          store_pattern(ofs, cnt);
        } else if (cmd >= 3 && cmd <= 15) { // short pattern
          const size_t ofs =
              cnt + THREE +
              static_cast<size_t>(static_cast<T>(src.pop()) << FOUR);
          store_pattern(ofs, cmd);
        } else {
          spdlog::critical("unknown marker {:#X} at offset {}\n", val,
                           (src.i_src - 1));
          EXCEPTION::cannot_decompress();
        }
      }
    }
    fill();
    return buf.as_bytes();
  }
};

constexpr uint8_t SAS_RLE_COMMAND_COPY64 = 0x00;
constexpr uint8_t SAS_RLE_COMMAND_INSERT_BYTE18 = 0x04;
constexpr uint8_t SAS_RLE_COMMAND_INSERT_AT17 = 0x05;
constexpr uint8_t SAS_RLE_COMMAND_INSERT_BLANK17 = 0x06;
constexpr uint8_t SAS_RLE_COMMAND_INSERT_ZERO17 = 0x07;
constexpr uint8_t SAS_RLE_COMMAND_COPY1 = 0x08;
constexpr uint8_t SAS_RLE_COMMAND_COPY17 = 0x09;
constexpr uint8_t SAS_RLE_COMMAND_COPY33 = 0x0A;
constexpr uint8_t SAS_RLE_COMMAND_COPY49 = 0x0B;
constexpr uint8_t SAS_RLE_COMMAND_INSERT_BYTE3 = 0x0C;
constexpr uint8_t SAS_RLE_COMMAND_INSERT_AT2 = 0x0D;
constexpr uint8_t SAS_RLE_COMMAND_INSERT_BLANK2 = 0x0E;
constexpr uint8_t SAS_RLE_COMMAND_INSERT_ZERO2 = 0X0F;

/// SASYZCRL
/**
 *  From https://github.com/WizardMac/ReadStat
 */
template <Endian _endian, Format _format>
struct RLE : public DST_VALUES<_endian, _format> {
  using DST_VALUES<_endian, _format>::buf;
  using DST_VALUES<_endian, _format>::n_dst;
  using DST_VALUES<_endian, _format>::reset;
  using DST_VALUES<_endian, _format>::fill;
  using DST_VALUES<_endian, _format>::check;
  using DST_VALUES<_endian, _format>::assert_check;
  using DST_VALUES<_endian, _format>::store_value;
  using DST_VALUES<_endian, _format>::i_dst;

  explicit RLE(const Properties::Metadata *_metadata)
      : DST_VALUES<_endian, _format>(_metadata) {}

  BYTES operator()(const BYTES &_values) {
    constexpr uint8_t FOUR{4}, EIGHT{8};

    reset();
    SRC_VALUES src(_values);
    auto store_values = [&](size_t n) {
      n = std::min(n, src.remaining());
      assert_check(n);
      buf.copy(i_dst, src.pop(n));
      i_dst += n;
    };

    while (src.check(2) && check()) {
      const auto val = src.pop();
      const uint8_t command = static_cast<uint8_t>(val >> FOUR);
      const size_t end_of_first_byte = static_cast<size_t>(val & 0x0F);
      D(spdlog::info("RLE:({}, {}: {:#X})\n", src.i_src, i_dst, command));
      switch (command) {
        break;
      case SAS_RLE_COMMAND_COPY64: {
        const size_t n = (end_of_first_byte << EIGHT) + src.pop() + 64;
        store_values(n);
      } break;
      case SAS_RLE_COMMAND_INSERT_BYTE18: {
        const size_t n = (end_of_first_byte << FOUR) + src.pop() + 18;
        store_value(src.pop(), n);
      } break;
      case SAS_RLE_COMMAND_INSERT_AT17: {
        const size_t n = (end_of_first_byte << EIGHT) + src.pop() + 17;
        store_value(C_AT, n);
      } break;
      case SAS_RLE_COMMAND_INSERT_BLANK17: {
        const size_t n = (end_of_first_byte << EIGHT) + src.pop() + 17;
        store_value(C_SPACE, n);
      } break;
      case SAS_RLE_COMMAND_INSERT_ZERO17: {
        const size_t n = (end_of_first_byte << EIGHT) + src.pop() + 17;
        store_value(C_NULL, n);
      } break;
      case SAS_RLE_COMMAND_COPY1: {
        store_values(end_of_first_byte + 1);
      } break;
      case SAS_RLE_COMMAND_COPY17: {
        store_values(end_of_first_byte + 17);
      } break;
      case SAS_RLE_COMMAND_COPY33: {
        store_values(end_of_first_byte + 33);
      } break;
      case SAS_RLE_COMMAND_COPY49: {
        store_values(end_of_first_byte + 49);
      } break;
      case SAS_RLE_COMMAND_INSERT_BYTE3: {
        store_value(src.pop(), end_of_first_byte + 3);
      } break;
      case SAS_RLE_COMMAND_INSERT_AT2: {
        store_value(C_AT, end_of_first_byte + 2);
      } break;
      case SAS_RLE_COMMAND_INSERT_BLANK2: {
        store_value(C_SPACE, end_of_first_byte + 2);
      } break;
      case SAS_RLE_COMMAND_INSERT_ZERO2: {
        store_value(C_NULL, end_of_first_byte + 2);
      } break;
      default: {
        spdlog::critical("Invalid command: {:#X} at offset {}\n", command,
                         (src.i_src - 1));
        EXCEPTION::cannot_decompress();
      }
      }
    }
    fill();
    return buf.as_bytes();
  }
};

} // namespace DECOMPRESSOR
} // namespace INTERNAL
} // namespace cppsas7bdat

#endif
