/**
 *  \file cppsas7bdat/reader.hpp
 *
 *  \brief Main file
 *
 *  \author Olivia Quinet
 */

#ifndef _CPP_SAS7BDAT_READER_HPP_
#define _CPP_SAS7BDAT_READER_HPP_

#include <cppsas7bdat/filter/column.hpp>
#include <cppsas7bdat/column.hpp>
#include <cppsas7bdat/properties.hpp>
#include <cppsas7bdat/version.hpp>
#include <memory>

namespace cppsas7bdat {

class Reader {
private:
  struct DataSourceConcept {
    virtual ~DataSourceConcept();

    virtual bool eof() = 0;
    virtual bool read_bytes(void *_p, const size_t _length) = 0;
  };

  template <typename _Source>
  struct DataSourceModel : public DataSourceConcept {
    template <typename _Tp>
    DataSourceModel(_Tp &&_source) : source(std::forward<_Tp>(_source)) {}

    bool eof() final { return source.eof(); }

    bool read_bytes(void *_p, const size_t _length) final {
      return source.read_bytes(_p, _length);
    }

    _Source source;
  };

  struct DatasetSinkConcept {
    virtual ~DatasetSinkConcept();

    virtual void set_properties(const Properties &_properties) = 0;
    virtual void push_row(const size_t _row_index, Column::PBUF _p) = 0;
    virtual void end_of_data() = 0;
  };

  template <typename _Sink>
  struct DatasetSinkModel : public DatasetSinkConcept {
    template <typename _Tp>
    DatasetSinkModel(_Tp &&_dataset) : dataset(std::forward<_Tp>(_dataset)) {}

    void set_properties(const Properties &_properties) final {
      dataset.set_properties(_properties);
    }

    void push_row(const size_t _row_index, Column::PBUF _p) final {
      dataset.push_row(_row_index, _p);
    }

    void end_of_data() final { dataset.end_of_data(); }

  private:
    _Sink dataset;
  };

  struct FilterConcept {
    virtual ~FilterConcept();

    virtual bool accept(const Column &_column) const = 0;
  };

  template <typename _Filter> struct FilterModel : public FilterConcept {
    template <typename _Tp>
    FilterModel(_Tp &&_filter) : filter(std::forward<_Tp>(_filter)) {}

    bool accept(const Column &_column) const final {
      return filter.accept(_column);
    }

  private:
    _Filter filter;
  };

public:
  class impl;
  using PIMPL = std::unique_ptr<impl>;
  using PSOURCE = std::unique_ptr<DataSourceConcept>;
  using PSINK = std::unique_ptr<DatasetSinkConcept>;
  using PFILTER = std::unique_ptr<FilterConcept>;

  template <typename _Source> static PSOURCE build_source(_Source &&_source) {
    return std::make_unique<DataSourceModel<_Source>>(
        std::forward<_Source>(_source));
  }
  template <typename _Sink> static PSINK build_sink(_Sink &&_sink) {
    return std::make_unique<DatasetSinkModel<_Sink>>(
        std::forward<_Sink>(_sink));
  }
  template <typename _Filter> static PFILTER build_filter(_Filter &&_filter) {
    return std::make_unique<FilterModel<_Filter>>(
        std::forward<_Filter>(_filter));
  }

private:
  PIMPL m_pimpl;

protected:
  Reader(PSOURCE &&_source, PSINK &&_sink, PFILTER &&_filter);

public:
  template <typename _Source, typename _Sink,
            typename _Filter = ColumnFilter::AcceptAll>
  explicit Reader(_Source &&_source, _Sink &&_sink, _Filter &&_filter = {})
      : Reader(build_source(std::forward<_Source>(_source)),
               build_sink(std::forward<_Sink>(_sink)),
               build_filter(std::forward<_Filter>(_filter))) {}

  Reader() noexcept;
  Reader(Reader &&) noexcept;
  Reader &operator=(Reader &&) noexcept;
  ~Reader();

  const Properties &properties() const noexcept;

  void end_of_data();

  bool skip(const size_t _nrows);

  void read_all();
  bool read_row();
  bool read_rows(const size_t _chunk_size);

  Column::PBUF read_row_no_sink();

  size_t current_row_index() const noexcept;
};

static_assert(!std::is_copy_constructible_v<Reader> &&
                  !std::is_copy_assignable_v<Reader>,
              "Reader is copyable");
static_assert(std::is_nothrow_move_constructible_v<Reader> &&
                  std::is_nothrow_move_assignable_v<Reader>,
              "Reader is not movable");
} // namespace cppsas7bdat

#endif
