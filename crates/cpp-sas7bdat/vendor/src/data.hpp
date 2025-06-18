/**
 *  \file src/data.hpp
 *
 *  \brief Data reading
 *
 *  \author Olivia Quinet
 */

#ifndef _CPP_SAS7BDAT_SRC_DATA_HPP_
#define _CPP_SAS7BDAT_SRC_DATA_HPP_

#include "page.hpp"
#include <optional>

namespace cppsas7bdat {
namespace INTERNAL {

namespace PAGE {
struct base {
  using OFFSET_LENGTH = std::pair<size_t, size_t>;
  size_t current_row_on_page{0};

  virtual ~base() {}

  virtual bool next() const noexcept = 0;
  virtual OFFSET_LENGTH read_line() noexcept = 0;

  void inc_row_on_page() noexcept { ++current_row_on_page; }
};

struct meta_type : public base {
  DATASUBHEADERS data_subheaders;

  meta_type(DATASUBHEADERS &&_data_subheaders)
      : data_subheaders(std::move(_data_subheaders)) {
    D(spdlog::info("PAGE::meta_type: {}\n", data_subheaders.size()));
  }

  bool next() const noexcept final {
    return current_row_on_page == data_subheaders.size();
  }
  OFFSET_LENGTH read_line() noexcept final {
    const auto &data_subheader = data_subheaders[current_row_on_page];
    return std::make_pair(data_subheader.offset, data_subheader.length);
  }
};

struct data_type : public base {
  const size_t block_count;
  const size_t offset;
  const size_t row_length;

  data_type(const size_t _block_count, const size_t _offset,
            const size_t _row_length)
      : block_count(_block_count), offset(_offset), row_length(_row_length) {
    D(spdlog::info("PAGE::data_type: {}, {}, {}\n", block_count, offset,
                   row_length));
  }

  bool next() const noexcept final {
    return current_row_on_page == block_count;
  }
  OFFSET_LENGTH read_line() noexcept final {
    return std::make_pair(offset + row_length * current_row_on_page,
                          row_length);
  }
};

struct mix_type : public base {
  const size_t row_count;
  const size_t offset;
  const size_t row_length;

  mix_type(const Properties::Metadata *metadata, const size_t _offset)
      : row_count(std::min(metadata->row_count, metadata->mix_page_row_count)),
        offset(_offset), row_length(metadata->row_length) {
    D(spdlog::info("PAGE::mix_type: {}, {}, {}\n", row_count, offset,
                   row_length));
  }

  bool next() const noexcept final { return current_row_on_page == row_count; }
  OFFSET_LENGTH read_line() noexcept final {
    return std::make_pair(offset + row_length * current_row_on_page,
                          row_length);
  }
};
} // namespace PAGE

template <typename _DataSource, Endian _endian, Format _format,
          typename _Decompressor>
struct READ_DATA : public READ_PAGE<_DataSource, _endian, _format> {
  using DataSource = _DataSource;
  constexpr static auto endian = _endian;
  constexpr static auto format = _format;
  using Decompressor = _Decompressor;

  using CBUFFER = INTERNAL::BUFFER<_endian, _format>;
  constexpr static size_t integer_size = CBUFFER::integer_size;

  mutable _Decompressor decompressor;
  const Properties::Metadata *metadata{nullptr};
  size_t current_row{0};
  using PPAGE = std::unique_ptr<PAGE::base>;
  PPAGE page;

  using READ_PAGE<_DataSource, _endian, _format>::header;
  using READ_PAGE<_DataSource, _endian, _format>::read_page;
  using READ_PAGE<_DataSource, _endian, _format>::process_page_subheaders;
  using READ_PAGE<_DataSource, _endian, _format>::current_page_header;
  using READ_PAGE<_DataSource, _endian, _format>::buf;

  using PAGE_CONSTANT<_format>::page_bit_offset;

  static READ_DATA build(READ_METADATA<_DataSource, _endian, _format> &&_rm,
                         _Decompressor &&_decompressor,
                         const Properties::Metadata *_metadata) {
    PPAGE page;
    // Check if we already have data_subheaders from the metadata page...
    if (_rm.data_subheaders.size()) {
      // Yes => build the corresponding meta page by moving the data_subheaders
      page = std::make_unique<PAGE::meta_type>(std::move(_rm.data_subheaders));
    }
    return READ_DATA(std::move(_rm), std::forward<_Decompressor>(_decompressor),
                     _metadata, std::move(page));
  }

  READ_DATA(READ_METADATA<_DataSource, _endian, _format> &&_rm,
            _Decompressor &&_decompressor,
            const Properties::Metadata *_metadata)
      : READ_DATA(build(std::move(_rm),
                        std::forward<_Decompressor>(_decompressor),
                        _metadata)) {}

  READ_DATA(READ_METADATA<_DataSource, _endian, _format> &&_rm,
            _Decompressor &&_decompressor,
            const Properties::Metadata *_metadata, PPAGE &&_page)
      : READ_PAGE<_DataSource, _endian, _format>(std::move(_rm)),
        decompressor(std::move(_decompressor)), metadata(_metadata),
        page(std::move(_page)) {
    // Check if we need to build the page, i.e. not page is provided...
    if (!page) {
      // No => build the corresponding page
      build_page();
    }
  }

  void set_pmetadata(const Properties::Metadata *_metadata) {
    metadata = _metadata;
  }

  bool next() {
    if (current_row == metadata->row_count)
      return false;
    if (!page)
      return false;
    // Do we need to read a new page from the file?
    if (page->next()) {
      if (!read_next_page())
        return false;
    }
    return true;
  }

  auto _read_line() {
    const auto r = page->read_line();
    ++current_row;
    page->inc_row_on_page();
    return r;
  }

  bool skip(size_t _nrows) {
    while (_nrows) {
      if (!next())
        return false;
      _read_line();
      --_nrows;
    }
    return true;
  }

  std::optional<BYTES> read_line() {
    if (!next())
      return {};
    const auto [offset, length] = _read_line();
    return extract_row_values(offset, length);
  }

  BYTES extract_row_values(const size_t _offset, const size_t _length) const {
    D(spdlog::info("READ_DATA::extract_row_values({}, {})\n", _offset,
                   _length));
    FMT_ASSERT(_offset + _length <= header->page_length,
               "Accessing data outside the page");
    const auto values = buf.get_bytes(_offset, _length);
    if (_length < metadata->row_length) {
      return decompressor(values);
    } else {
      return values;
    }
  }

  bool build_page() {
    if (current_page_header.type == PAGE_META_TYPE) {
      D(spdlog::info("READ_DATA::read_next_page::PAGE_META_TYPE\n"));
      DATASUBHEADERS data_subheaders;
      process_page_metadata(data_subheaders);
      if (!data_subheaders.empty()) {
        page.reset(new PAGE::meta_type(std::move(data_subheaders)));
        return true;
      }
    } else if (current_page_header.type == PAGE_DATA_TYPE) {
      D(spdlog::info("READ_DATA::read_next_page::PAGE_DATA_TYPE\n"));
      page.reset(new PAGE::data_type(current_page_header.block_count,
                                     page_bit_offset + 8,
                                     metadata->row_length));
      return true;
    } else if (is_page_mix(current_page_header.type)) {
      D(spdlog::info("READ_DATA::read_next_page::PAGE_MIX_TYPE\n"));
      constexpr const size_t subheader_size = 3 * integer_size; // 12 or 24
      const size_t offset =
          page_bit_offset + 8 +
          current_page_header.subheaders_count * subheader_size;
      const size_t align_correction = offset % 8;
      page.reset(new PAGE::mix_type(metadata, offset + align_correction));
      return true;
    } else {
      D(spdlog::warn("Unknown page type: {}\n", current_page_header.type));
    }
    return false;
  }

  bool read_next_page() {
    D(spdlog::info("read_next_page\n"));

    page.reset();
    while (true) {
      if (!read_page())
        return false;
      if (build_page())
        return true;
    }
  }

  void process_page_metadata(DATASUBHEADERS &_data_subheaders) {
    auto psh = [&](const PAGE_SUBHEADER &_subheader) {
      if (DataSubHeader::check(metadata, _subheader))
        process_DATA_SUBHEADER(_subheader, _data_subheaders);
    };
    process_page_subheaders(psh);
  }

  void process_DATA_SUBHEADER(const PAGE_SUBHEADER &_subheader,
                              DATASUBHEADERS &_data_subheaders) {
    D(spdlog::info("READ_DATA::process_DATA_SUBHEADER: {}, {}\n",
                   _subheader.offset, _subheader.length));
    _data_subheaders.emplace_back(_subheader);
  }
};

} // namespace INTERNAL
} // namespace cppsas7bdat

#endif
