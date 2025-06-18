/**
 *  \file src/metadata.hpp
 *
 *  \brief Metadata reading
 *
 *  \author Olivia Quinet
 */

#ifndef _CPP_SAS7BDAT_SRC_METADATA_HPP_
#define _CPP_SAS7BDAT_SRC_METADATA_HPP_

#include "formatters.hpp"
#include "page.hpp"

namespace cppsas7bdat {
namespace INTERNAL {

constexpr const std::string_view COLUMN_DATETIME_FORMAT[] = {
    "DATETIME", "DTWKDATX", "B8601DN",  "B8601DT", "B8601DX",
    "B8601DZ",  "B8601LX",  "E8601DN",  "E8601DT", "E8601DX",
    "E8601DZ",  "E8601LX",  "DATEAMPM", "DTDATE",  "DTMONYY",
    "DTMONYY",  "DTWKDATX", "DTYEAR",   "TOD",     "MDYAMPM"};

constexpr const std::string_view COLUMN_DATE_FORMAT[] = {
    "DATE",     "DAY",      "DDMMYY",   "DOWNAME", "JULDAY",  "JULIAN",
    "MMDDYY",   "MMYY",     "MMYYC",    "MMYYD",   "MMYYP",   "MMYYS",
    "MMYYN",    "MONNAME",  "MONTH",    "MONYY",   "QTR",     "QTRR",
    "NENGO",    "WEEKDATE", "WEEKDATX", "WEEKDAY", "WEEKV",   "WORDDATE",
    "WORDDATX", "YEAR",     "YYMM",     "YYMMC",   "YYMMD",   "YYMMP",
    "YYMMS",    "YYMMN",    "YYMON",    "YYMMDD",  "YYQ",     "YYQC",
    "YYQD",     "YYQP",     "YYQS",     "YYQN",    "YYQR",    "YYQRC",
    "YYQRD",    "YYQRP",    "YYQRS",    "YYQRN",   "YYMMDDP", "YYMMDDC",
    "E8601DA",  "YYMMDDN",  "MMDDYYC",  "MMDDYYS", "MMDDYYD", "YYMMDDS",
    "B8601DA",  "DDMMYYN",  "YYMMDDD",  "DDMMYYB", "DDMMYYP", "MMDDYYP",
    "YYMMDDB",  "MMDDYYN",  "DDMMYYC",  "DDMMYYD", "DDMMYYS", "MINGUO"};

constexpr const std::string_view COLUMN_TIME_FORMAT[] = {"TIME"};

template <Format _format> struct METADATA_CONSTANT;

template <> struct METADATA_CONSTANT<Format::bit64> {
  static constexpr const size_t lcs_offset = 682;
  static constexpr const size_t lcp_offset = 706;
  static constexpr const size_t compression_offset = 20;

  static constexpr BYTE ROW_SIZE_SUBHEADER[][8] = {
      {0x00, 0x00, 0x00, 0x00, 0xF7, 0xF7, 0xF7, 0xF7},
      {0xF7, 0xF7, 0xF7, 0xF7, 0x00, 0x00, 0x00, 0x00}};
  static constexpr BYTE COLUMN_SIZE_SUBHEADER[][8] = {
      {0x00, 0x00, 0x00, 0x00, 0xF6, 0xF6, 0xF6, 0xF6},
      {0xF6, 0xF6, 0xF6, 0xF6, 0x00, 0x00, 0x00, 0x00}};
  static constexpr BYTE SUBHEADER_COUNTS_SUBHEADER[][8] = {
      {0x00, 0xFC, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
      {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFC, 0x00}};
  static constexpr BYTE COLUMN_TEXT_SUBHEADER[][8] = {
      {0xFD, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
      {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFD}};
  static constexpr BYTE COLUMN_NAME_SUBHEADER[][8] = {
      {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}};
  static constexpr BYTE COLUMN_ATTRIBUTES_SUBHEADER[][8] = {
      {0xFC, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
      {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFC}};
  static constexpr BYTE FORMAT_AND_LABEL_SUBHEADER[][8] = {
      {0xFE, 0xFB, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
      {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFB, 0xFE}};
  static constexpr BYTE COLUMN_LIST_SUBHEADER[][8] = {
      {0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
      {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE}};
};

template <> struct METADATA_CONSTANT<Format::bit32> {
  static constexpr const size_t lcs_offset = 354;
  static constexpr const size_t lcp_offset = 378;
  static constexpr const size_t compression_offset = 16;

  static constexpr BYTE ROW_SIZE_SUBHEADER[][4] = {{0xF7, 0xF7, 0xF7, 0xF7}};
  static constexpr BYTE COLUMN_SIZE_SUBHEADER[][4] = {{0xF6, 0xF6, 0xF6, 0xF6}};
  static constexpr BYTE SUBHEADER_COUNTS_SUBHEADER[][4] = {
      {0x00, 0xFC, 0xFF, 0xFF}, {0xFF, 0xFF, 0xFC, 0x00}};
  static constexpr BYTE COLUMN_TEXT_SUBHEADER[][4] = {{0xFD, 0xFF, 0xFF, 0xFF},
                                                      {0xFF, 0xFF, 0xFF, 0xFD}};
  static constexpr BYTE COLUMN_NAME_SUBHEADER[][4] = {{0xFF, 0xFF, 0xFF, 0xFF}};
  static constexpr BYTE COLUMN_ATTRIBUTES_SUBHEADER[][4] = {
      {0xFC, 0xFF, 0xFF, 0xFF}, {0xFF, 0xFF, 0xFF, 0xFC}};
  static constexpr BYTE FORMAT_AND_LABEL_SUBHEADER[][4] = {
      {0xFE, 0xFB, 0xFF, 0xFF}, {0xFF, 0xFF, 0xFB, 0xFE}};
  static constexpr BYTE COLUMN_LIST_SUBHEADER[][4] = {{0xFE, 0xFF, 0xFF, 0xFF},
                                                      {0xFF, 0xFF, 0xFF, 0xFE}};
};

template <typename _DataSource, Endian _endian, Format _format>
struct READ_METADATA : public READ_PAGE<_DataSource, _endian, _format>,
                       public METADATA_CONSTANT<_format> {
  using DataSource = _DataSource;
  constexpr static auto endian = _endian;
  constexpr static auto format = _format;

  using CBUFFER = INTERNAL::BUFFER<_endian, _format>;
  constexpr static size_t integer_size = CBUFFER::integer_size;
  using METADATA_CONSTANT<_format>::lcs_offset;
  using METADATA_CONSTANT<_format>::lcp_offset;
  using METADATA_CONSTANT<_format>::compression_offset;

  using METADATA_CONSTANT<_format>::ROW_SIZE_SUBHEADER;
  using METADATA_CONSTANT<_format>::COLUMN_SIZE_SUBHEADER;
  using METADATA_CONSTANT<_format>::SUBHEADER_COUNTS_SUBHEADER;
  using METADATA_CONSTANT<_format>::COLUMN_TEXT_SUBHEADER;
  using METADATA_CONSTANT<_format>::COLUMN_NAME_SUBHEADER;
  using METADATA_CONSTANT<_format>::COLUMN_ATTRIBUTES_SUBHEADER;
  using METADATA_CONSTANT<_format>::FORMAT_AND_LABEL_SUBHEADER;
  using METADATA_CONSTANT<_format>::COLUMN_LIST_SUBHEADER;

  static constexpr const char RLE_COMPRESSION[] = "SASYZCRL";
  static constexpr const char RDC_COMPRESSION[] = "SASYZCR2";

  DATASUBHEADERS data_subheaders;
  std::vector<std::string> column_texts;
  std::vector<std::string> column_names;
  std::vector<std::string> column_formats;
  std::vector<std::string> column_labels;
  std::vector<size_t> column_data_offsets;
  std::vector<size_t> column_data_lengths;
  std::vector<Column::Type> column_data_types;

  using READ_PAGE<_DataSource, _endian, _format>::read_page;
  using READ_PAGE<_DataSource, _endian, _format>::process_page_subheaders;
  using READ_PAGE<_DataSource, _endian, _format>::current_page_header;
  using READ_PAGE<_DataSource, _endian, _format>::buf;

  READ_METADATA(READ_HEADER<_DataSource, _endian, _format> &&_rh,
                const Properties::Header *_header)
      : READ_PAGE<_DataSource, _endian, _format>(std::move(_rh.is),
                                                 std::move(_rh.buf), _header) {}

  void set_metadata(Properties::Metadata *_metadata,
                    const Reader::PFILTER &_filter) {
    while (read_page()) {
      if (process_page(_metadata))
        break;
    }
    create_columns(_metadata, _filter);
  }

  bool process_page(Properties::Metadata *_metadata) {
    D(spdlog::info("process_page: type={}\n", current_page_header.type));
    bool data_reached = false;
    if (is_page_meta_mix_amd(current_page_header.type))
      data_reached = process_page_metadata(_metadata);
    return is_page_data_mix(current_page_header.type) || data_reached;
  }

  bool process_page_metadata(Properties::Metadata *_metadata) {
    bool data_reached = false;
    auto psh = [&](const PAGE_SUBHEADER &_psh) {
      if (process_subheader(_psh, _metadata))
        data_reached = true;
    };
    process_page_subheaders(psh);
    return data_reached;
  }

  bool process_subheader(const PAGE_SUBHEADER &_subheader,
                         Properties::Metadata *_metadata) {
    // Match the subheader signature to the correct processing function
    const auto subheader_signature =
        buf.get_bytes(_subheader.offset, integer_size);
    if (match_signature(ROW_SIZE_SUBHEADER, subheader_signature)) {
      process_ROW_SIZE_SUBHEADER(_subheader, _metadata);
      return false;
    }
    if (match_signature(COLUMN_SIZE_SUBHEADER, subheader_signature)) {
      process_COLUMN_SIZE_SUBHEADER(_subheader, _metadata);
      return false;
    }
    if (match_signature(SUBHEADER_COUNTS_SUBHEADER, subheader_signature)) {
      process_SUBHEADER_COUNTS_SUBHEADER(_subheader, _metadata);
      return false;
    }
    if (match_signature(COLUMN_TEXT_SUBHEADER, subheader_signature)) {
      process_COLUMN_TEXT_SUBHEADER(_subheader, _metadata);
      return false;
    }
    if (match_signature(COLUMN_NAME_SUBHEADER, subheader_signature)) {
      process_COLUMN_NAME_SUBHEADER(_subheader, _metadata);
      return false;
    }
    if (match_signature(COLUMN_ATTRIBUTES_SUBHEADER, subheader_signature)) {
      process_COLUMN_ATTRIBUTES_SUBHEADER(_subheader, _metadata);
      return false;
    }
    if (match_signature(FORMAT_AND_LABEL_SUBHEADER, subheader_signature)) {
      process_FORMAT_AND_LABEL_SUBHEADER(_subheader, _metadata);
      return false;
    }
    if (match_signature(COLUMN_LIST_SUBHEADER, subheader_signature)) {
      process_COLUMN_LIST_SUBHEADER(_subheader, _metadata);
      return false;
    }

    if (DataSubHeader::check(_metadata, _subheader)) {
      process_DATA_SUBHEADER(_subheader);
      return true;
    }

    // Unknown subheader...
    spdlog::warn("Unknown subheader signature!");
    return false;
  }

  template <size_t n, size_t m>
  static bool match_signature(const BYTE (&_s)[n][m],
                              const BYTES _signature) noexcept {
    return std::any_of(std::begin(_s), std::end(_s),
                       [sig = _signature.data()](auto s) {
                         return std::memcmp(s, sig, m) == 0;
                       });
  }

  void
  process_ROW_SIZE_SUBHEADER([[maybe_unused]] const PAGE_SUBHEADER &_subheader,
                             Properties::Metadata *_metadata) const noexcept {
    D(spdlog::info("READ_METADATA::process_ROW_SIZE_SUBHEADER: {}, {} : ",
                   _subheader.offset, _subheader.length));
    buf.assert_check(_subheader.offset + lcp_offset, 4);
    _metadata->lcs = buf.get_uint16(_subheader.offset + lcs_offset);
    _metadata->lcp = buf.get_uint16(_subheader.offset + lcp_offset);
    _metadata->row_length =
        buf.get_uinteger(_subheader.offset + 5 * integer_size);
    _metadata->row_count =
        buf.get_uinteger(_subheader.offset + 6 * integer_size);
    _metadata->col_count_p1 =
        buf.get_uinteger(_subheader.offset + 9 * integer_size);
    _metadata->col_count_p2 =
        buf.get_uinteger(_subheader.offset + 10 * integer_size);
    _metadata->mix_page_row_count =
        buf.get_uinteger(_subheader.offset + 15 * integer_size);
    D(spdlog::info("lcs={}, lcp={}, row_length={}, row_count={}, "
                   "col_count_p1={}, col_count_p2={}, mix_page_row_count={}\n",
                   _metadata->lcs, _metadata->lcp, _metadata->row_length,
                   _metadata->row_count, _metadata->col_count_p1,
                   _metadata->col_count_p2, _metadata->mix_page_row_count));
  }

  void process_COLUMN_SIZE_SUBHEADER(
      [[maybe_unused]] const PAGE_SUBHEADER &_subheader,
      Properties::Metadata *_metadata) const noexcept {
    D(spdlog::info("READ_METADATA::process_COLUMN_SIZE_SUBHEADER: {}, {} : ",
                   _subheader.offset, _subheader.length));
    _metadata->column_count = buf.template get_uinteger<ASSERT::YES>(
        _subheader.offset + integer_size);
    D(spdlog::info("column_count={}\n", _metadata->column_count));
    if (_metadata->col_count_p1 + _metadata->col_count_p2 !=
        _metadata->column_count)
      spdlog::warn("Column count mismatch\n");
  }

  void process_SUBHEADER_COUNTS_SUBHEADER(
      [[maybe_unused]] const PAGE_SUBHEADER &_subheader,
      [[maybe_unused]] Properties::Metadata *_metadata) const noexcept {
    D(spdlog::info(
        "READ_METADATA::process_SUBHEADER_COUNTS_SUBHEADER: {}, {}\n",
        _subheader.offset, _subheader.length));
    // spdlog::info("READ_METADATA::process_SUBHEADER_COUNTS_SUBHEADER: {}\n",
    // buf.get_string_untrim(_subheader.offset, _subheader.length));
  }

  void process_COLUMN_TEXT_SUBHEADER(
      [[maybe_unused]] const PAGE_SUBHEADER &_subheader,
      Properties::Metadata *_metadata) noexcept {
    D(spdlog::info("READ_METADATA::process_COLUMN_TEXT_SUBHEADER: {}, {}\n",
                   _subheader.offset, _subheader.length));
    const size_t length =
        buf.template get_uint16<ASSERT::YES>(_subheader.offset + integer_size);
    const auto text = buf.template get_string_untrim<ASSERT::YES>(
        _subheader.offset + integer_size, length);
    D(spdlog::info("text: {}\n", text));
    column_texts.emplace_back(text);
    if (column_texts.size() == 1) {
      if (text.find(RLE_COMPRESSION) != text.npos)
        _metadata->compression = Compression::RLE;
      else if (text.find(RDC_COMPRESSION) != text.npos)
        _metadata->compression = Compression::RDC;
      const auto compression = buf.template get_string<ASSERT::YES>(
          _subheader.offset + compression_offset, 8);
      if (compression == "") {
        _metadata->lcs = 0;
        _metadata->creator_proc = buf.template get_string<ASSERT::YES>(
            _subheader.offset + compression_offset + 16, _metadata->lcp);
      } else if (compression == RLE_COMPRESSION) {
        _metadata->creator_proc = buf.template get_string<ASSERT::YES>(
            _subheader.offset + compression_offset + 24, _metadata->lcp);
      } else if (_metadata->lcs > 0) {
        _metadata->lcp = 0;
        _metadata->creator = buf.template get_string<ASSERT::YES>(
            _subheader.offset + compression_offset, _metadata->lcs);
      }
      D(spdlog::info("compression: {}, creator: {}, creator_proc: {}\n",
                     _metadata->compression, _metadata->creator,
                     _metadata->creator_proc));
    }
  }

  std::string get_column_text_substr(const size_t _idx, size_t _offset,
                                     size_t _length) const noexcept {
    if (_idx < column_texts.size()) {
      const auto ct = column_texts[std::min(_idx, column_texts.size() - 1)];
      const auto n = ct.size();
      _offset = std::min(_offset, n);
      _length = std::min(_length, n - _offset);
      if (_length && std::isprint(ct[_offset])) {
        while (_length && std::isspace(ct[_offset])) {
          ++_offset;
          --_length;
        }
        while (_length && (!std::isprint(ct[_offset + _length - 1]) ||
                           std::isspace(ct[_offset + _length - 1]))) {
          --_length;
        }
        return ct.substr(_offset, _length);
      }
    }
    return {};
  }

  void process_COLUMN_NAME_SUBHEADER(
      [[maybe_unused]] const PAGE_SUBHEADER &_subheader,
      [[maybe_unused]] Properties::Metadata *_metadata) noexcept {
    D(spdlog::info("READ_METADATA::process_COLUMN_NAME_SUBHEADER: {}, {}\n",
                   _subheader.offset, _subheader.length));
    const size_t offset_max =
        _subheader.offset + _subheader.length - 12 - integer_size;
    for (size_t offset = _subheader.offset + integer_size + 8;
         offset <= offset_max; offset += 8) {
      const size_t idx = buf.get_uint16(offset + 0);
      const size_t name_offset = buf.get_uint16(offset + 2);
      const size_t name_length = buf.get_uint16(offset + 4);
      const auto column_name =
          get_column_text_substr(idx, name_offset, name_length);
      column_names.emplace_back(column_name);
      D(spdlog::info("column name: {},{},{}: {}\n", idx, name_offset,
                     name_length, column_name));
    }
  }

  void process_COLUMN_ATTRIBUTES_SUBHEADER(
      [[maybe_unused]] const PAGE_SUBHEADER &_subheader,
      [[maybe_unused]] Properties::Metadata *_metadata) noexcept {
    D(spdlog::info(
        "READ_METADATA::process_COLUMN_ATTRIBUTES_SUBHEADER: {}, {}\n",
        _subheader.offset, _subheader.length));
    const size_t offset_max =
        _subheader.offset + _subheader.length - 12 - integer_size;
    for (size_t offset = _subheader.offset + integer_size + 8;
         offset <= offset_max; offset += integer_size + 8) {
      const size_t column_data_offset = buf.get_uinteger(offset + 0);
      const size_t column_data_length = buf.get_uint32(offset + integer_size);
      const uint8_t column_data_type = buf.get_byte(offset + integer_size + 6);
      column_data_offsets.emplace_back(column_data_offset);
      column_data_lengths.emplace_back(column_data_length);
      column_data_types.emplace_back(
          column_data_type == 1 ? Column::Type::number : Column::Type::string);
      D(spdlog::info("column attribute: {}, {}, {}\n", column_data_offset,
                     column_data_length, column_data_type));
    }
  }

  void process_FORMAT_AND_LABEL_SUBHEADER(
      [[maybe_unused]] const PAGE_SUBHEADER &_subheader,
      [[maybe_unused]] Properties::Metadata *_metadata) noexcept {
    D(spdlog::info(
        "READ_METADATA::process_FORMAT_AND_LABEL_SUBHEADER: {}, {}\n",
        _subheader.offset, _subheader.length));
    const size_t offset = _subheader.offset + 3 * integer_size;
    buf.assert_check(offset + 32, 2);
    const size_t format_idx = buf.get_uint16(offset + 22);
    const size_t format_offset = buf.get_uint16(offset + 24);
    const size_t format_length = buf.get_uint16(offset + 26);
    const size_t label_idx = buf.get_uint16(offset + 28);
    const size_t label_offset = buf.get_uint16(offset + 30);
    const size_t label_length = buf.get_uint16(offset + 32);
    D(spdlog::info("column format: {}, {}, {}: ", format_idx, format_offset,
                   format_length));
    const auto column_format =
        get_column_text_substr(format_idx, format_offset, format_length);
    D(spdlog::info("{}\n", column_format));
    D(spdlog::info("column_label: {}, {}, {}: ", label_idx, label_offset,
                   label_length));
    const auto column_label =
        get_column_text_substr(label_idx, label_offset, label_length);
    D(spdlog::info("{}\n", column_label));
    column_formats.emplace_back(column_format);
    column_labels.emplace_back(column_label);
  }

  void process_COLUMN_LIST_SUBHEADER(
      [[maybe_unused]] const PAGE_SUBHEADER &_subheader,
      [[maybe_unused]] Properties::Metadata *_metadata) const noexcept {
    D(spdlog::info("READ_METADATA::process_COLUMN_LIST_SUBHEADER: {}, {}\n",
                   _subheader.offset, _subheader.length));
    // spdlog::info("READ_METADATA::process_COLUMN_LIST_SUBHEADER: {}\n",
    // buf.get_string_untrim(_subheader.offset, _subheader.length));
  }

  void process_DATA_SUBHEADER([
      [maybe_unused]] const PAGE_SUBHEADER &_subheader) noexcept {
    D(spdlog::info("READ_METADATA::process_DATA_SUBHEADER: {}, {}\n",
                   _subheader.offset, _subheader.length));
    data_subheaders.emplace_back(_subheader);
  }

  void create_columns(Properties::Metadata *_metadata,
                      const Reader::PFILTER &_filter) {
    const size_t ncols = _metadata->column_count;
    _metadata->columns.reserve(ncols);

    auto get_value = [&](auto arg, auto vals, const size_t icol) {
      const auto n = std::min(ncols, vals.size());
      if (n != ncols)
        spdlog::info("Mismatch between the expected number of columns ({}) and "
                     "the number ({}) of '{}' values!\n",
                     ncols, vals.size(), arg);
      return vals.at(icol);
    };

    using Type = Column::Type;

    for (size_t icol = 0; icol < ncols; ++icol) {
      D(spdlog::info("READ_METADATA::create_columns: {}/{}\n", icol, ncols));
      const auto column_name = get_value("name", column_names, icol);
      const auto column_label = get_value("label", column_labels, icol);
      const auto column_format = get_value("format", column_formats, icol);
      const auto column_offset =
          get_value("data_offset", column_data_offsets, icol);
      const auto column_length =
          get_value("data_length", column_data_lengths, icol);
      const auto column_type = get_value("data_type", column_data_types, icol);

      bool column_type_not_supported = true;
      auto add_column = [&](auto &&formatter) {
        D(spdlog::info("add_column: {}, {}, {}, {}, {}, {}\n", column_name,
                       column_label, column_format, column_offset,
                       column_length, column_type));
        column_type_not_supported = false;

        Column column(column_name, column_label, column_format,
                      std::forward<decltype(formatter)>(formatter));
        if (!_filter || _filter->accept(column))
          _metadata->columns.emplace_back(
              std::move(column)); // column_name, column_label, column_format,
                                  // std::move(formatter));
      };

      if (column_type == Type::string)
        add_column(FORMATTER::StringFormatter(column_offset, column_length));
      else if (column_type == Type::number) {
        if (column_length == 1)
          add_column(
              FORMATTER::SmallIntegerFormatter(column_offset, column_length));
        else if (column_length == 2)
          add_column(FORMATTER::IntegerFormatter<_endian, int16_t>(
              column_offset, column_length));
        else if (is_datetime_format(column_format))
          add_column(FORMATTER::DateTimeFormatter<_endian>(column_offset,
                                                           column_length));
        else if (is_date_format(column_format))
          add_column(
              FORMATTER::DateFormatter<_endian>(column_offset, column_length));
        else if (is_time_format(column_format))
          add_column(
              FORMATTER::TimeFormatter<_endian>(column_offset, column_length));
        else {
          if (column_length == 8)
            add_column(FORMATTER::DoubleFormatter<_endian>(column_offset,
                                                           column_length));
          else if (column_length == 3)
            add_column(FORMATTER::IncompleteDoubleFormatter<_endian, 3>(
                column_offset, column_length));
          else if (column_length == 4)
            add_column(FORMATTER::IncompleteDoubleFormatter<_endian, 4>(
                column_offset, column_length));
          else if (column_length == 5)
            add_column(FORMATTER::IncompleteDoubleFormatter<_endian, 5>(
                column_offset, column_length));
          else if (column_length == 6)
            add_column(FORMATTER::IncompleteDoubleFormatter<_endian, 6>(
                column_offset, column_length));
          else if (column_length == 7)
            add_column(FORMATTER::IncompleteDoubleFormatter<_endian, 7>(
                column_offset, column_length));
        }
      }
      if (column_type_not_supported) {
        D(spdlog::warn("NoFormatter: {}, {}: ", column_type, column_format));
        add_column(FORMATTER::NoFormatter(column_offset, column_length));
      }
    }
  }

  template <size_t n>
  static bool match_column_format(const std::string_view (&_list)[n],
                                  const std::string_view &_f) noexcept {
    return std::any_of(std::begin(_list), std::end(_list),
                       [_f](auto _g) { return _f == _g; });
  }

  static bool is_datetime_format(const std::string_view &_f) noexcept {
    return match_column_format(COLUMN_DATETIME_FORMAT, _f);
  }

  static bool is_date_format(const std::string_view &_f) noexcept {
    return match_column_format(COLUMN_DATE_FORMAT, _f);
  }

  static bool is_time_format(const std::string_view &_f) noexcept {
    return match_column_format(COLUMN_TIME_FORMAT, _f);
  }
};

} // namespace INTERNAL
} // namespace cppsas7bdat

#endif
