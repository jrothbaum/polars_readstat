/**
 *  \file tests/tests_column-filter.cpp
 *
 *  \brief
 *
 *  \author  Olivia Quinet
 */

#include <cppsas7bdat/filter/column.hpp>
#include <cppsas7bdat/sink/null.hpp>
#include <cppsas7bdat/source/ifstream.hpp>
#include <cppsas7bdat/reader.hpp>

#include "data.hpp"

#include <catch2/catch_test_macros.hpp>
#include <fmt/core.h>

SCENARIO("The Include/Excluded column filter can be used to filter columns") {
  GIVEN("An empty Include filter") {
    cppsas7bdat::ColumnFilter::Include filter;
    WHEN("A filter a column based on its name") {
      CHECK(filter.is_accepted("col1") == false);
      CHECK(filter.is_accepted("col2") == false);
    }
  }
  GIVEN("An Include filter") {
    cppsas7bdat::ColumnFilter::Include filter{{"col1"}};
    WHEN("A filter a column based on its name") {
      CHECK(filter.is_accepted("col1") == true);
      CHECK(filter.is_accepted("col2") == false);
    }
  }
  GIVEN("An empty Exclude filter") {
    cppsas7bdat::ColumnFilter::Exclude filter;
    WHEN("A filter a column based on its name") {
      CHECK(filter.is_accepted("col1") == true);
      CHECK(filter.is_accepted("col2") == true);
    }
  }
  GIVEN("An Exclude filter") {
    cppsas7bdat::ColumnFilter::Exclude filter{{"col1"}};
    WHEN("A filter a column based on its name") {
      CHECK(filter.is_accepted("col1") == false);
      CHECK(filter.is_accepted("col2") == true);
    }
  }
  GIVEN("An empty IncludeExclude filter") {
    cppsas7bdat::ColumnFilter::IncludeExclude filter;
    WHEN("A filter a column based on its name") {
      CHECK(filter.is_accepted("col1") == true);
      CHECK(filter.is_accepted("col2") == true);
    }
  }
  GIVEN("An IncludeExclude filter, include filled") {
    cppsas7bdat::ColumnFilter::IncludeExclude filter{
        cppsas7bdat::ColumnFilter::Include{{"col1"}},
        cppsas7bdat::ColumnFilter::Exclude{}};
    WHEN("A filter a column based on its name") {
      CHECK(filter.is_accepted("col1") == true);
      CHECK(filter.is_accepted("col2") == false);
    }
  }
  GIVEN("An IncludeExclude filter, exclude filled") {
    cppsas7bdat::ColumnFilter::IncludeExclude filter{
        cppsas7bdat::ColumnFilter::Include{},
        cppsas7bdat::ColumnFilter::Exclude{{"col1"}}};
    WHEN("A filter a column based on its name") {
      CHECK(filter.is_accepted("col1") == false);
      CHECK(filter.is_accepted("col2") == true);
    }
  }
}

namespace {
template <typename _DataSink, typename _Filter>
auto get_reader(const std::string &_filename, _DataSink &&_datasink,
                _Filter &&_filter) {
  return cppsas7bdat::Reader(
      cppsas7bdat::datasource::ifstream(convert_path(_filename).c_str()),
      std::forward<_DataSink>(_datasink), std::forward<_Filter>(_filter));
}
} // namespace

SCENARIO("The Include/Exclude column filter can be used to filter columns to "
         "be read") {
  const std::string filename = "data/file2.sas7bdat";
  GIVEN(fmt::format("A file {},", filename)) {
    GIVEN("An include filter") {
      cppsas7bdat::ColumnFilter::Include filter{{"c1"}};
      auto reader = get_reader(filename, cppsas7bdat::datasink::null(), filter);
      const auto &columns = reader.properties() /*.metadata*/.columns;
      THEN("The columns list is consistent with the filter") {
        REQUIRE(columns.size() == 1);
        CHECK(columns[0].name == "c1");
      }
    }
    GIVEN("An exclude filter") {
      cppsas7bdat::ColumnFilter::Exclude filter{{"c1"}};
      auto reader = get_reader(filename, cppsas7bdat::datasink::null(), filter);
      const auto &columns = reader.properties() /*.metadata*/.columns;
      THEN("The columns list is consistent with the filter") {
        REQUIRE(columns.size() == 3);
        CHECK(columns[0].name == "q1");
        CHECK(columns[1].name == "c2");
        CHECK(columns[2].name == "q2");
      }
    }
  }
}
