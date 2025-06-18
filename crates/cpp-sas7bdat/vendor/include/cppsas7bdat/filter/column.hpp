/**
 *  \file cppsas7bdat/filter/column.hpp
 *
 *  \brief Column filter description
 *
 *  \author Olivia Quinet
 */

#ifndef _CPP_SAS7BDAT_FILTER_COLUMN_HPP_
#define _CPP_SAS7BDAT_FILTER_COLUMN_HPP_

#include <set>
#include <string>

#include <cppsas7bdat/column.hpp>

namespace cppsas7bdat {

namespace ColumnFilter {

struct AcceptAll {
  bool accept([[maybe_unused]] const Column &_column) const noexcept {
    return true;
  }
};

struct Include {
  using NAMES = std::set<std::string>;

  NAMES included;

  bool accept(const Column &_column) const noexcept {
    return is_accepted(_column.name);
  }

  bool is_accepted(const std::string &_name) const noexcept {
    return is_included(_name);
  }

  bool is_included(const std::string &_name) const noexcept {
    return included.find(_name) != included.end();
  }
};

struct Exclude {
  using NAMES = std::set<std::string>;

  NAMES excluded;

  bool accept(const Column &_column) const noexcept {
    return is_accepted(_column.name);
  }

  bool is_accepted(const std::string &_name) const noexcept {
    return !is_excluded(_name);
  }

  bool is_excluded(const std::string &_name) const noexcept {
    return excluded.find(_name) != excluded.end();
  }
};

struct IncludeExclude : public Include, public Exclude {
  bool accept(const Column &_column) const noexcept {
    return is_accepted(_column.name);
  }

  bool is_accepted(const std::string &_name) const noexcept {
    if (!included.empty())
      return is_included(_name);
    if (!excluded.empty())
      return !is_excluded(_name);
    return true;
  }
};

} // namespace ColumnFilter

} // namespace cppsas7bdat

#endif
