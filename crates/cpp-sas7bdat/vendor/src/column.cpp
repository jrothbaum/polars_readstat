/**
 *  \file src/column.cpp
 *
 *  \brief Column file
 *
 *  \author Olivia Quinet
 */

#include <cppsas7bdat/filter/column.hpp>
#include <cppsas7bdat/column.hpp>

namespace cppsas7bdat {

Column::FormatterConcept::~FormatterConcept() {}

COLUMNS get_columns(const COLUMNS &_columns, Column::Type _type) {
  COLUMNS columns;
  for (const auto &column : _columns) {
    if (column.type == _type)
      columns.emplace_back(column);
  }
  return columns;
}

Columns::Columns(const COLUMNS &_columns)
    : strings(get_columns(_columns, Column::Type::string)),
      numbers(get_columns(_columns, Column::Type::number)),
      integers(get_columns(_columns, Column::Type::integer)),
      datetimes(get_columns(_columns, Column::Type::datetime)),
      dates(get_columns(_columns, Column::Type::date)),
      times(get_columns(_columns, Column::Type::time)) {}
} // namespace cppsas7bdat
