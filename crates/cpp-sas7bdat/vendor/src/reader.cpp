/**
 *  \file src/reader.cpp
 *
 *  \brief Main file
 *
 *  \author Olivia Quinet
 */

#include "reader-impl.hpp"

namespace cppsas7bdat {
Reader::Reader() noexcept = default;
Reader::Reader(Reader &&) noexcept = default;
Reader &Reader::operator=(Reader &&) noexcept = default;

Reader::~Reader() = default;

Reader::DataSourceConcept::~DataSourceConcept() = default;
Reader::DatasetSinkConcept::~DatasetSinkConcept() = default;
Reader::FilterConcept::~FilterConcept() = default;

Reader::Reader(PSOURCE &&_source, PSINK &&_sink, PFILTER &&_filter)
    : m_pimpl(impl::build(std::move(_source), std::move(_sink),
                          std::move(_filter))) {}

Reader::PIMPL Reader::impl::build(PSOURCE &&_source, PSINK &&_sink,
                                  PFILTER &&_filter) {
  Properties properties;
  auto rd = READ::data(std::move(_source), &properties /*.header*/,
                       &properties /*.metadata*/, _filter);
  return std::visit(
      [&](auto &&arg) -> Reader::PIMPL {
        using T = std::decay_t<decltype(arg)>;
        using RI = INTERNAL::ReaderImpl<T>;
        return std::make_unique<RI>(std::move(arg), std::move(_sink),
                                    std::move(properties));
      },
      std::move(rd));
}

const Properties &Reader::properties() const noexcept {
  static const Properties empty;
  return m_pimpl ? m_pimpl->properties() : empty;
}

void Reader::end_of_data() {
  if (m_pimpl)
    m_pimpl->end_of_data();
}

bool Reader::skip(const size_t _nrows) {
  return m_pimpl ? m_pimpl->skip(_nrows) : false;
}

void Reader::read_all() {
  if (m_pimpl)
    m_pimpl->read_all();
}

bool Reader::read_row() { return m_pimpl ? m_pimpl->read_row() : false; }

bool Reader::read_rows(const size_t _chunk_size) {
  return m_pimpl ? m_pimpl->read_rows(_chunk_size) : false;
}

size_t Reader::current_row_index() const noexcept {
  return m_pimpl ? m_pimpl->current_row_index() : 0;
}

Column::PBUF Reader::read_row_no_sink() {
  return m_pimpl ? m_pimpl->read_row_no_sink() : Column::PBUF{};
}
} // namespace cppsas7bdat
