/**
 *  \file src/reader-impl.hpp
 *
 *  \brief Main file
 *
 *  \author Olivia Quinet
 */

#ifndef _CPP_SAS7BDAT_SRC_READER_IMPL_HPP_
#define _CPP_SAS7BDAT_SRC_READER_IMPL_HPP_

// clang-format off
#include <cppsas7bdat/reader.hpp>
#include "sas7bdat-impl.hpp"
// clang-format on

namespace cppsas7bdat {

class Reader::impl : public boost::noncopyable {
public:
  static PIMPL build(PSOURCE &&_source, PSINK &&_sink, PFILTER &&_filter);

  explicit impl(PSINK &&_sink, Properties &&_properties)
      : m_sink(std::move(_sink)), m_properties(std::move(_properties)) {
    m_sink->set_properties(properties());
  }
  virtual ~impl() {}

  const Properties &properties() const noexcept { return m_properties; }

  virtual size_t current_row_index() const noexcept = 0;

  void push_row(const size_t _row_index, Column::PBUF _p) {
    m_sink->push_row(_row_index, _p);
  }

  void end_of_data() { m_sink->end_of_data(); }

  virtual Column::PBUF read_row_no_sink() = 0;

  virtual bool skip(const size_t _nrows) = 0;

  virtual bool read_row() = 0;

  bool read_rows(size_t _chunk_size) {
    while (_chunk_size && read_row())
      _chunk_size--;
    return _chunk_size == 0;
  }

  void read_all() {
    while (read_row())
      ;
  }

private:
  PSINK m_sink;
  Properties m_properties;
};

namespace INTERNAL {

template <typename _RD> class ReaderImpl : public Reader::impl {
public:
  explicit ReaderImpl(_RD &&_rd, Reader::PSINK &&_sink,
                      Properties &&_properties)
      : Reader::impl(std::move(_sink), std::move(_properties)),
        m_read_data(std::forward<_RD>(_rd)) {
    // Dirty hack to make sure to use the object's properties.
    m_read_data.set_pheader(&properties());
    m_read_data.set_pmetadata(&properties());
  }

  size_t current_row_index() const noexcept final {
    return m_read_data.current_row;
  }

  bool skip(const size_t _nrows) final {
    const auto r = m_read_data.skip(_nrows);
    if (!r)
      end_of_data();
    return r;
  }

  Column::PBUF read_row_no_sink() final {
    auto vals = m_read_data.read_line();
    return vals ? vals->data() : nullptr;
  }

  bool read_row() final {
    const size_t row_index = current_row_index();
    auto vals = m_read_data.read_line();
    if (!vals) {
      end_of_data();
      return false;
    }
    push_row(row_index, vals->data());
    return true;
  }

private:
  _RD m_read_data;
};

} // namespace INTERNAL

} // namespace cppsas7bdat

#endif
