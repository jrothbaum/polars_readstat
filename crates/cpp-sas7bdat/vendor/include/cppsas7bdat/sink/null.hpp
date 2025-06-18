/**
 *  \file cppsas7bdat/sink/null.hpp
 *
 *  \brief NULL datasink
 *
 *  \author Olivia Quinet
 */

#ifndef _CPP_SAS7BDAT_SINK_NULL_HPP_
#define _CPP_SAS7BDAT_SINK_NULL_HPP_

#include <cppsas7bdat/column.hpp>
#include <cppsas7bdat/properties.hpp>

namespace cppsas7bdat {
namespace datasink {
struct null {
  COLUMNS columns;

  void set_properties([[maybe_unused]] const Properties &_properties) {
    columns = COLUMNS(_properties /*.metadata*/.columns);
  }

  void push_row([[maybe_unused]] const size_t _irow,
                [[maybe_unused]] Column::PBUF _p) {
    for (const auto &column : columns) {
      switch (column.type) {
      case cppsas7bdat::Column::Type::string:
        process(column.get_string(_p));
        break;
      case cppsas7bdat::Column::Type::integer:
        process(column.get_integer(_p));
        break;
      case cppsas7bdat::Column::Type::number:
        process(column.get_number(_p));
        break;
      case cppsas7bdat::Column::Type::datetime:
        process(column.get_datetime(_p));
        break;
      case cppsas7bdat::Column::Type::date:
        process(column.get_date(_p));
        break;
      case cppsas7bdat::Column::Type::time:
        process(column.get_time(_p));
        break;
      }
    }
  }

  template <typename _Tp>
  void process([[maybe_unused]] const _Tp _x) const noexcept {}

  void end_of_data() const noexcept {}
};
} // namespace datasink
} // namespace cppsas7bdat

#endif
