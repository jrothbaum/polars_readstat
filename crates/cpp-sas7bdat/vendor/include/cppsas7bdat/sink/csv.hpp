/**
 *  \file cppsas7bdat/sink/csv.hpp
 *
 *  \brief CSV datasink
 *
 *  \author Olivia Quinet
 */

#ifndef _CPP_SAS7BDAT_SINK_CSV_HPP_
#define _CPP_SAS7BDAT_SINK_CSV_HPP_

#include <cppsas7bdat/column.hpp>
#include <fmt/core.h>
#include <fmt/ostream.h>
#include <fstream>
#include <iostream>

namespace cppsas7bdat {
namespace datasink {
namespace detail {
struct csv {
  std::ostream &os;

  explicit csv(std::ostream &_os) noexcept : os(_os) {}

  COLUMNS columns;

  void set_properties(const Properties &_properties) {
    columns = COLUMNS(_properties /*.metadata*/.columns);
    bool first = true;
    for (const auto &column : columns) {
      if (first)
        first = false;
      else
        fmt::print(os, ",");
      print(column.name);
    }
    fmt::print(os, "\n");
  }

  void push_row([[maybe_unused]] const size_t _irow, Column::PBUF _p) {
    bool first = true;
    for (const auto &column : columns) {
      if (first)
        first = false;
      else
        fmt::print(os, ",");
      switch (column.type) {
      case cppsas7bdat::Column::Type::string:
        print(column.get_string(_p));
        break;
      case cppsas7bdat::Column::Type::integer:
        print(column.get_integer(_p));
        break;
      case cppsas7bdat::Column::Type::number:
        print(column.get_number(_p));
        break;
      case cppsas7bdat::Column::Type::datetime:
        print(column.get_datetime(_p));
        break;
      case cppsas7bdat::Column::Type::date:
        print(column.get_date(_p));
        break;
      case cppsas7bdat::Column::Type::time:
        print(column.get_time(_p));
        break;
      }
    }
    fmt::print(os, "\n");
  }

  void end_of_data() const noexcept {}

  void print(const SV &_x) { fmt::print(os, "\"{}\"", _x); }
  void print(const NUMBER _x) {
    if (!std::isnan(_x))
      fmt::print(os, "{}", _x);
  }
  void print(const INTEGER _x) { fmt::print(os, "{}", _x); }
  void print(const DATETIME _x) { fmt::print(os, "{}", _x); }
  void print(const DATE _x) { fmt::print(os, "{}", _x); }
  void print(const TIME _x) { fmt::print(os, "{}", _x); }
};

struct _ofstream {
  std::ofstream ofs;

  explicit _ofstream(const char *_pcszfilename) : ofs(_pcszfilename) {}
};

struct csv_ofstream : public _ofstream, public csv {
  explicit csv_ofstream(const char *_pcszfilename)
      : _ofstream(_pcszfilename), csv(ofs) {}
};

} // namespace detail

struct _csv_factory {
  auto operator()(std::ostream &_os) const noexcept { return detail::csv(_os); }
  auto operator()(const char *_pcszfilename) const {
    return detail::csv_ofstream(_pcszfilename);
  }
} csv;

} // namespace datasink
} // namespace cppsas7bdat

#endif
