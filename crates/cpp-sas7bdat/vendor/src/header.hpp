/**
 *  \file src/header.hpp
 *
 *  \brief Header reading
 *
 *  \author Olivia Quinet
 */

#ifndef _CPP_SAS7BDAT_SRC_HEADER_HPP_
#define _CPP_SAS7BDAT_SRC_HEADER_HPP_

#include "buffer.hpp"
#include "encodings.hpp"

namespace cppsas7bdat {
namespace INTERNAL {

struct HEADER_CONSTANTS {
  static constexpr size_t HEADER_SIZE{288};
  static constexpr BYTE MAGIC_NUMBER[] = {
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0xc2, 0xea, 0x81, 0x60, 0xb3, 0x14, 0x11, 0xcf, 0xbd, 0x92,
      0x08, 0x00, 0x09, 0xc7, 0x31, 0x8c, 0x18, 0x1f, 0x10, 0x11};
};

template <typename _DataSource> struct CHECK_HEADER : public HEADER_CONSTANTS {
  _DataSource is;
  INTERNAL::MBUFFER buf;
  size_t align1{0};
  size_t align2{0};
  size_t total_align{0};
  bool is_big_endian{false};
  bool is_64bit{false};

  explicit CHECK_HEADER(_DataSource &&_is) : is(std::move(_is)) {
    D(spdlog::info("Reading header ...\n"));
    if (!buf.read_stream(is, HEADER_SIZE))
      EXCEPTION::header_too_short();
  }

  void check_magic_number() const {
    D(spdlog::info("Checking magic number ...\n"));
    if (!_check_magic_number())
      EXCEPTION::invalid_magic_number();
  }

  bool _check_magic_number() const noexcept {
    return std::memcmp(buf.data(0, sizeof(MAGIC_NUMBER)), MAGIC_NUMBER,
                       sizeof(MAGIC_NUMBER)) == 0;
  }

  void set_aligns_endianness(Properties::Header *_header) {
    if (buf[32] == '3') {
      align2 = 4;
      _header->format = Format::bit64;
      is_64bit = true;
    }
    if (buf[35] == '3') {
      align1 = 4;
    }
    total_align = align1 + align2;
    _header->endianness = (buf[37] == 0x01 ? Endian::little : Endian::big);
    is_big_endian = _header->endianness == Endian::big;
    D(spdlog::info("Setting aligns ... {}, {}, {}, {}, {}\n", align1, align2,
                   total_align, _header->format, _header->endianness));
  }
};

template <typename _DataSource, Endian _endian, Format _format>
struct READ_HEADER {
  using DataSource = _DataSource;
  constexpr static auto endian = _endian;
  constexpr static auto format = _format;

  _DataSource is;
  INTERNAL::BUFFER<_endian, _format> buf;
  const size_t align1;      /**< 0 or 4 */
  const size_t total_align; /**< 0, 4, or 8 */

  READ_HEADER(CHECK_HEADER<_DataSource> &&_ch)
      : is(std::move(_ch.is)), buf(std::move(_ch.buf)), align1(_ch.align1),
        total_align(_ch.total_align) {
    FMT_ASSERT(is, "DataSource is not valid");
  }

  void set_header_length_and_read(Properties::Header *_header) {
    _header->header_length = buf.get_uint32(196 + align1); // 196-200 + align1
    /*if (_header->format == Format::bit64 &&
        _header->header_length != 8192) {
      spdlog::info("Expected header length of 8192 but got {}\n",
                   _header->header_length);
    }*/

    // Read the rest of the header
    FMT_ASSERT(_header->header_length >= HEADER_CONSTANTS::HEADER_SIZE,
               "Header is too big in set_header_length_and_read");
    if (!buf.read_stream(is,
                         _header->header_length - HEADER_CONSTANTS::HEADER_SIZE,
                         HEADER_CONSTANTS::HEADER_SIZE))
      EXCEPTION::header_too_short();
    D(spdlog::info("Set header length and read ... {}\n",
                   _header->header_length));
  }

  void set_header(Properties::Header *_header) const {
    FMT_ASSERT(buf.size() >= 288 + total_align,
               "Buffer is too small in set_header");

    _header->platform = (buf[39] == '1' ? Platform::unix
                                        : buf[39] == '2' ? Platform::windows
                                                         : Platform::unknown);
    _header->encoding = get_encoding(buf[70]);      // 70
    _header->dataset_name = buf.get_string(92, 64); // 92-156
    _header->file_type = buf.get_string(156, 8);    // 156-164

    _header->date_created = get_datetime_from_epoch(
        buf.get_double(164 + align1)); // 164-172 + align1
    _header->date_modified = get_datetime_from_epoch(
        buf.get_double(172 + align1)); // 172-180 + align1

    _header->page_length = buf.get_uint32(200 + align1); // 200-204 + align1
    _header->page_count = buf.get_uint32(204 + align1);  // 204-208 + align1

    _header->sas_release =
        buf.get_string(216 + total_align, 8); // 216-224 + total_align
    _header->sas_server_type =
        buf.get_string(224 + total_align, 16); // 224-240 + total_align
    _header->os_type = buf.get_string(240 + total_align, 16); // 240-256
    _header->os_name =
        (buf[272 + total_align]
             ? buf.get_string(272 + total_align, 16)   // 272+288 + total_align
             : buf.get_string(256 + total_align, 16)); // 256-272 + total_align

    D(spdlog::info(
        "Setting header ... {}, {}; {}; {}, {}; {}, {}; {}, {}, {}, {}; {}, "
        "{}\n",
        to_string(_header->format), to_string(_header->platform),
        _header->encoding, _header->dataset_name, _header->file_type,
        cppsas7bdat::to_string(
            _header
                ->date_created), // boost::posix_time::to_iso_extended_string(_header->date_created),
        cppsas7bdat::to_string(
            _header
                ->date_modified), // boost::posix_time::to_iso_extended_string(_header->date_modified),
        _header->sas_release, _header->sas_server_type, _header->os_type,
        _header->os_name, _header->page_length, _header->page_count));
  }
};

} // namespace INTERNAL
} // namespace cppsas7bdat

#endif
