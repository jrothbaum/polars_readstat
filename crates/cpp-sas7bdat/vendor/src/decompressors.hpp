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

constexpr uint8_t C_NULL = 0x00;  /**< '\0' */
constexpr uint8_t C_SPACE = 0x20; /**< ' ' */
constexpr uint8_t C_AT = 0x40;    /**< '@' */

// ReadStat RLE command constants
#define SAS_RLE_COMMAND_COPY64         0x00
#define SAS_RLE_COMMAND_INSERT_BYTE18  0x04
#define SAS_RLE_COMMAND_INSERT_AT17    0x05
#define SAS_RLE_COMMAND_INSERT_BLANK17 0x06
#define SAS_RLE_COMMAND_INSERT_ZERO17  0x07
#define SAS_RLE_COMMAND_COPY1          0x08
#define SAS_RLE_COMMAND_COPY17         0x09
#define SAS_RLE_COMMAND_COPY33         0x0A
#define SAS_RLE_COMMAND_COPY49         0x0B
#define SAS_RLE_COMMAND_INSERT_BYTE3   0x0C
#define SAS_RLE_COMMAND_INSERT_AT2     0x0D
#define SAS_RLE_COMMAND_INSERT_BLANK2  0x0E
#define SAS_RLE_COMMAND_INSERT_ZERO2   0x0F

struct SRC_VALUES {
  const BYTES values;
  const size_t n_src;
  size_t i_src{0};

  explicit SRC_VALUES(const BYTES &_values)
      : values(_values), n_src(values.size()) {}

  auto pop() noexcept { 
    if (i_src >= n_src) {
      return uint8_t(0);
    }
    return values[i_src++]; 
  }
  
  auto pop(const size_t _n) noexcept {
    if (i_src + _n > n_src) {
      return values.substr(i_src, n_src - i_src);
    }
    auto v = values.substr(i_src, _n);
    i_src += _n;
    return v;
  }

  bool check(const size_t _n) const noexcept { return i_src + _n <= n_src; }
  size_t remaining() const noexcept { return n_src - i_src; }
  bool has_bytes(size_t n) const noexcept { return i_src + n <= n_src; }
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
      spdlog::critical("Invalid dst length: {}+{}>{}", i_dst, _n, n_dst);
      EXCEPTION::cannot_decompress();
    }
  }
};

/// SASYZCR2 - Fixed RDC implementation
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
    if (i_dst < _offset) {
      EXCEPTION::cannot_decompress();
    }
    buf.copy(i_dst, i_dst - _offset, _n);
    i_dst += _n;
  }

  BYTES operator()(const BYTES &_values) {
    reset();
    SRC_VALUES src(_values);
    
    uint32_t ctrl_bits = 0;
    uint32_t ctrl_mask = 0;

    while (src.has_bytes(1) && check()) {
      // Check if we need more control bits
      if (ctrl_mask == 0) {
        if (!src.has_bytes(2)) {
          break;
        }
        uint8_t byte1 = src.pop();
        uint8_t byte2 = src.pop();
        ctrl_bits = (static_cast<uint32_t>(byte1) << 8) | byte2;
        ctrl_mask = 0x8000;
      }
      
      if ((ctrl_bits & ctrl_mask) == 0) {
        // Copy literal byte
        if (!src.has_bytes(1)) break;
        uint8_t literal = src.pop();
        store_value(literal, 1);
      } else {
        // Compressed data
        if (!src.has_bytes(1)) break;
        uint8_t command_byte = src.pop();
        uint8_t cmd = (command_byte >> 4) & 0x0F;
        uint8_t cnt = command_byte & 0x0F;
        
        if (cmd == 0) { // short RLE
          if (!src.has_bytes(1)) break;
          uint8_t repeat_byte = src.pop();
          size_t count = cnt + 3;
          store_value(repeat_byte, count);
        } else if (cmd == 1) { // long RLE  
          if (!src.has_bytes(2)) break;
          uint8_t extra = src.pop();
          uint8_t repeat_byte = src.pop();
          size_t count = cnt + ((static_cast<size_t>(extra) << 4) + 19);
          store_value(repeat_byte, count);
        } else if (cmd == 2) { // long pattern
          if (!src.has_bytes(2)) break;
          uint8_t extra = src.pop();
          uint8_t count_byte = src.pop();
          size_t offset = cnt + 3 + (static_cast<size_t>(extra) << 4);
          size_t count = count_byte + 16;
          store_pattern(offset, count);
        } else if (cmd >= 3 && cmd <= 15) { // short pattern
          if (!src.has_bytes(1)) break;
          uint8_t extra = src.pop();
          size_t offset = cnt + 3 + (static_cast<size_t>(extra) << 4);
          size_t count = cmd;
          store_pattern(offset, count);
        } else {
          spdlog::critical("RDC: Invalid command {}", cmd);
          EXCEPTION::cannot_decompress();
        }
      }
      
      // Shift control mask for next iteration
      ctrl_mask >>= 1;
    }
    
    fill();
    return buf.as_bytes();
  }
};

/// SASYZCRL - ReadStat-based RLE implementation
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

  // ReadStat-style copy function
  void copy_bytes(SRC_VALUES& src, size_t count) {
    count = std::min(count, src.remaining());
    if (count == 0) return;
    
    assert_check(count);
    auto data = src.pop(count);
    buf.copy(i_dst, data);
    i_dst += count;
  }

  BYTES operator()(const BYTES &_values) {
    reset();
    SRC_VALUES src(_values);
    
    while (src.has_bytes(1) && check()) {
      if (!src.has_bytes(1)) break;
      
      const uint8_t control_byte = src.pop();
      const uint8_t command = (control_byte >> 4) & 0x0F;
      const uint8_t end_of_first_byte = control_byte & 0x0F;

      switch (command) {
        case SAS_RLE_COMMAND_COPY64: {
          if (!src.has_bytes(1)) goto done;
          size_t count = (static_cast<size_t>(end_of_first_byte) << 8) + src.pop() + 64;
          copy_bytes(src, count);
          break;
        }
        case SAS_RLE_COMMAND_INSERT_BYTE18: {
          if (!src.has_bytes(2)) goto done;
          size_t count = (static_cast<size_t>(end_of_first_byte) << 4) + src.pop() + 18;
          uint8_t byte_to_insert = src.pop();
          store_value(byte_to_insert, count);
          break;
        }
        case SAS_RLE_COMMAND_INSERT_AT17: {
          if (!src.has_bytes(1)) goto done;
          size_t count = (static_cast<size_t>(end_of_first_byte) << 8) + src.pop() + 17;
          store_value(C_AT, count);
          break;
        }
        case SAS_RLE_COMMAND_INSERT_BLANK17: {
          if (!src.has_bytes(1)) goto done;
          size_t count = (static_cast<size_t>(end_of_first_byte) << 8) + src.pop() + 17;
          store_value(C_SPACE, count);
          break;
        }
        case SAS_RLE_COMMAND_INSERT_ZERO17: {
          if (!src.has_bytes(1)) goto done;
          size_t count = (static_cast<size_t>(end_of_first_byte) << 8) + src.pop() + 17;
          store_value(C_NULL, count);
          break;
        }
        case SAS_RLE_COMMAND_COPY1: {
          size_t count = end_of_first_byte + 1;
          copy_bytes(src, count);
          break;
        }
        case SAS_RLE_COMMAND_COPY17: {
          size_t count = end_of_first_byte + 17;
          copy_bytes(src, count);
          break;
        }
        case SAS_RLE_COMMAND_COPY33: {
          size_t count = end_of_first_byte + 33;
          copy_bytes(src, count);
          break;
        }
        case SAS_RLE_COMMAND_COPY49: {
          size_t count = end_of_first_byte + 49;
          copy_bytes(src, count);
          break;
        }
        case SAS_RLE_COMMAND_INSERT_BYTE3: {
          if (!src.has_bytes(1)) goto done;
          uint8_t byte_to_insert = src.pop();
          size_t count = end_of_first_byte + 3;
          store_value(byte_to_insert, count);
          break;
        }
        case SAS_RLE_COMMAND_INSERT_AT2: {
          size_t count = end_of_first_byte + 2;
          store_value(C_AT, count);
          break;
        }
        case SAS_RLE_COMMAND_INSERT_BLANK2: {
          size_t count = end_of_first_byte + 2;
          store_value(C_SPACE, count);
          break;
        }
        case SAS_RLE_COMMAND_INSERT_ZERO2: {
          size_t count = end_of_first_byte + 2;
          store_value(C_NULL, count);
          break;
        }
        default: {
          spdlog::critical("RLE: Invalid command 0x{:X} at src offset {}", command, src.i_src - 1);
          EXCEPTION::cannot_decompress();
          break;
        }
      }
    }

done:
    fill();
    return buf.as_bytes();
  }
};

} // namespace DECOMPRESSOR
} // namespace INTERNAL
} // namespace cppsas7bdat

#endif