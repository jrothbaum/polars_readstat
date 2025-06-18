/**
 *  \file src/sas7bdat-impl.hpp
 *
 *  \brief Main file
 *
 *  \author Olivia Quinet
 */

#ifndef _CPP_SAS7BDAT_SRC_SAS7BDAT_IMPL_HPP_
#define _CPP_SAS7BDAT_SRC_SAS7BDAT_IMPL_HPP_

// clang-format off
#include <spdlog/spdlog.h>
#include <variant>
#include <cppsas7bdat/reader.hpp>

#include "debug.hpp"
#include "exceptions.hpp"
#include "types.hpp"
#include "stream.hpp"
#include "header.hpp"
#include "metadata.hpp"
#include "data.hpp"
#include "decompressors.hpp"
// clang-format on

/**
 *  Reading a SAS7BDAT is done in several consecutive steps:
 *  - checking the header
 *  - reading the header
 *  - reading the metadata page
 *  - reading the data pages
 *
 *  Each step depends on the completion of the previous step.  In the
 *  following code, the result of the previous step is moved to
 *  construct the next step.
 */
namespace cppsas7bdat {
namespace INTERNAL {

using DATASOURCE = Reader::PSOURCE;

inline CHECK_HEADER<DATASOURCE> check_header(DATASOURCE &&_source,
                                             Properties::Header *_header) {
  INTERNAL::CHECK_HEADER<DATASOURCE> ch(std::move(_source));
  ch.check_magic_number();
  ch.set_aligns_endianness(_header);
  return ch;
}

template <Endian _endian, Format _format>
inline READ_HEADER<DATASOURCE, _endian, _format>
_read_header(CHECK_HEADER<DATASOURCE> &&ch, Properties::Header *_header) {
  READ_HEADER<DATASOURCE, _endian, _format> rh(std::move(ch));
  rh.set_header_length_and_read(_header);
  rh.set_header(_header);
  return rh;
}

template <Endian _endian, Format _format>
inline READ_METADATA<DATASOURCE, _endian, _format>
_read_metadata(READ_HEADER<DATASOURCE, _endian, _format> &&rh,
               const Properties::Header *_header,
               Properties::Metadata *_metadata,
               const Reader::PFILTER &_filter) {
  READ_METADATA<DATASOURCE, _endian, _format> rm(std::move(rh), _header);
  rm.set_metadata(_metadata, _filter);
  return rm;
}

template <Endian _endian, Format _format, typename _Decompressor>
inline READ_DATA<DATASOURCE, _endian, _format, _Decompressor>
_read_data(READ_METADATA<DATASOURCE, _endian, _format> &&rm,
           _Decompressor &&_decompressor,
           const Properties::Metadata *_metadata) {
  READ_DATA<DATASOURCE, _endian, _format, _Decompressor> rd(
      std::move(rm), std::forward<_Decompressor>(_decompressor), _metadata);
  return rd;
}

using RH = std::variant<READ_HEADER<DATASOURCE, Endian::big, Format::bit64>,
                        READ_HEADER<DATASOURCE, Endian::big, Format::bit32>,
                        READ_HEADER<DATASOURCE, Endian::little, Format::bit64>,
                        READ_HEADER<DATASOURCE, Endian::little, Format::bit32>>;
using RM =
    std::variant<READ_METADATA<DATASOURCE, Endian::big, Format::bit64>,
                 READ_METADATA<DATASOURCE, Endian::big, Format::bit32>,
                 READ_METADATA<DATASOURCE, Endian::little, Format::bit64>,
                 READ_METADATA<DATASOURCE, Endian::little, Format::bit32>>;
using RD = std::variant<
    READ_DATA<DATASOURCE, Endian::big, Format::bit64, DECOMPRESSOR::None>,
    READ_DATA<DATASOURCE, Endian::big, Format::bit64,
              DECOMPRESSOR::RDC<Endian::big, Format::bit64>>,
    READ_DATA<DATASOURCE, Endian::big, Format::bit64,
              DECOMPRESSOR::RLE<Endian::big, Format::bit64>>,
    READ_DATA<DATASOURCE, Endian::big, Format::bit32, DECOMPRESSOR::None>,
    READ_DATA<DATASOURCE, Endian::big, Format::bit32,
              DECOMPRESSOR::RDC<Endian::big, Format::bit32>>,
    READ_DATA<DATASOURCE, Endian::big, Format::bit32,
              DECOMPRESSOR::RLE<Endian::big, Format::bit32>>,
    READ_DATA<DATASOURCE, Endian::little, Format::bit64, DECOMPRESSOR::None>,
    READ_DATA<DATASOURCE, Endian::little, Format::bit64,
              DECOMPRESSOR::RDC<Endian::little, Format::bit64>>,
    READ_DATA<DATASOURCE, Endian::little, Format::bit64,
              DECOMPRESSOR::RLE<Endian::little, Format::bit64>>,
    READ_DATA<DATASOURCE, Endian::little, Format::bit32, DECOMPRESSOR::None>,
    READ_DATA<DATASOURCE, Endian::little, Format::bit32,
              DECOMPRESSOR::RDC<Endian::little, Format::bit32>>,
    READ_DATA<DATASOURCE, Endian::little, Format::bit32,
              DECOMPRESSOR::RLE<Endian::little, Format::bit32>>>;

inline RH read_header(CHECK_HEADER<DATASOURCE> &&ch,
                      Properties::Header *_header) {
  if (ch.is_big_endian) {
    if (ch.is_64bit)
      return _read_header<Endian::big, Format::bit64>(std::move(ch), _header);
    else
      return _read_header<Endian::big, Format::bit32>(std::move(ch), _header);
  } else {
    if (ch.is_64bit)
      return _read_header<Endian::little, Format::bit64>(std::move(ch),
                                                         _header);
    else
      return _read_header<Endian::little, Format::bit32>(std::move(ch),
                                                         _header);
  }
}

inline RM read_metadata(RH &&rh, const Properties::Header *_header,
                        Properties::Metadata *_metadata,
                        const Reader::PFILTER &_filter) {
  return std::visit(
      [&](auto &&arg) -> RM {
        using T = std::decay_t<decltype(arg)>;
        return _read_metadata<T::endian, T::format>(
            std::forward<T>(arg), _header, _metadata, _filter);
      },
      std::move(rh));
}

inline RD read_data(RM &&rm, const Properties::Metadata *_metadata) {
  return std::visit(
      [&](auto &&arg) -> RD {
        using T = std::decay_t<decltype(arg)>;
        switch (_metadata->compression) {
        case Compression::RDC:
          return _read_data<T::endian, T::format>(
              std::forward<T>(arg),
              DECOMPRESSOR::RDC<T::endian, T::format>(_metadata), _metadata);
        case Compression::RLE:
          return _read_data<T::endian, T::format>(
              std::forward<T>(arg),
              DECOMPRESSOR::RLE<T::endian, T::format>(_metadata), _metadata);
        default:
          return _read_data<T::endian, T::format>(
              std::forward<T>(arg), DECOMPRESSOR::None(), _metadata);
        }
      },
      std::move(rm));
}

inline auto read_line(RD &rd) {
  return std::visit([&](auto &&arg) { return arg.read_line(); }, rd);
}

inline auto current_row_index(const RD &rd) {
  return std::visit([&](auto &&arg) { return arg.current_row; }, rd);
}

} // namespace INTERNAL

namespace READ {

inline INTERNAL::RH header(INTERNAL::DATASOURCE &&_source,
                           Properties::Header *_header) {
  return INTERNAL::read_header(
      INTERNAL::check_header(std::move(_source), _header), _header);
}

inline INTERNAL::RM metadata(INTERNAL::DATASOURCE &&_source,
                             Properties::Header *_header,
                             Properties::Metadata *_metadata,
                             const Reader::PFILTER &_filter) {
  return INTERNAL::read_metadata(READ::header(std::move(_source), _header),
                                 _header, _metadata, _filter);
}

inline INTERNAL::RD data(INTERNAL::DATASOURCE &&_source,
                         Properties::Header *_header,
                         Properties::Metadata *_metadata,
                         const Reader::PFILTER &_filter) {
  return INTERNAL::read_data(
      READ::metadata(std::move(_source), _header, _metadata, _filter),
      _metadata);
}

} // namespace READ

} // namespace cppsas7bdat

#endif
