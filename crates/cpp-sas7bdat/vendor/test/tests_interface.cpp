/**
 *  \file tests/tests_interface.cpp
 *
 *  \brief
 *
 *  \author  Olivia Quinet
 */

#include "../include/cppsas7bdat/sink/null.hpp"
#include "../include/cppsas7bdat/source/ifstream.hpp"
#include "../include/cppsas7bdat/reader.hpp"

#include "data.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators_all.hpp>
#include <fmt/core.h>
#include <fmt/ostream.h>

using namespace cppsas7bdat;

SCENARIO("I can translate to string the Endian enum") {
  auto [e, s] = GENERATE(std::make_pair(Endian::little, "little"),
                         std::make_pair(Endian::big, "big"),
                         std::make_pair(Endian::unknown, "unknown"),
                         std::make_pair(Endian(-1), "unknown"));
  GIVEN("A value") {
    THEN("It is correctly translated") { CHECK(to_string(e) == s); }
  }
}

SCENARIO("I can translate to string the Format enum") {
  auto [e, s] = GENERATE(std::make_pair(Format::bit32, "32bits"),
                         std::make_pair(Format::bit64, "64bits"),
                         std::make_pair(Format(-1), "unknown"));
  GIVEN("A value") {
    THEN("It is correctly translated") { CHECK(to_string(e) == s); }
  }
}

SCENARIO("I can translate to string the Platform enum") {
  auto [e, s] = GENERATE(std::make_pair(Platform::unix, "unix"),
                         std::make_pair(Platform::windows, "windows"),
                         std::make_pair(Platform(-1), "unknown"));
  GIVEN("A value") {
    THEN("It is correctly translated") { CHECK(to_string(e) == s); }
  }
}

SCENARIO("I can translate to string the Compression enum") {
  auto [e, s] = GENERATE(std::make_pair(Compression::none, "none"),
                         std::make_pair(Compression::RLE, "RLE"),
                         std::make_pair(Compression::RDC, "RDC"),
                         std::make_pair(Compression(-1), "unknown"));
  GIVEN("A value") {
    THEN("It is correctly translated") { CHECK(to_string(e) == s); }
  }
}

SCENARIO("I can translate to string the Column type enum") {
  auto [e, s] = GENERATE(std::make_pair(Column::Type::string, "string"),
                         std::make_pair(Column::Type::number, "number"),
                         std::make_pair(Column::Type::integer, "integer"),
                         std::make_pair(Column::Type::datetime, "datetime"),
                         std::make_pair(Column::Type::date, "date"),
                         std::make_pair(Column::Type::time, "time"),
                         std::make_pair(Column::Type(-1), "unknown"));
  GIVEN("A value") {
    THEN("It is correctly translated") { CHECK(to_string(e) == s); }
  }
}

namespace {
template <typename _DataSink>
auto get_reader(const std::string &_filename, _DataSink &&_datasink) {
  return cppsas7bdat::Reader(
      cppsas7bdat::datasource::ifstream(convert_path(_filename).c_str()),
      std::forward<_DataSink>(_datasink));
}
auto get_reader(const std::string &_filename) {
  return get_reader(_filename, cppsas7bdat::datasink::null());
}
} // namespace

SCENARIO("When I try to read a non existing file with the public interface, an "
         "exception is thrown",
         "[interface][not_a_valid_file]") {
  GIVEN("An invalid path") {
    THEN("An exception is thrown") {
      // CHECK_THROWS_WITH(get_reader(invalid_path), "not_a_valid_file");
      CHECK_THROWS(get_reader(invalid_path));
    }
  }
}

SCENARIO("When I try to read a file too short with the public interface, an "
         "exception is thrown",
         "[interface][file too short]") {
  GIVEN("A path to a too short file") {
    THEN("an exception is thrown") {
      // CHECK_THROWS_WITH(get_reader(file_too_short), "header_too_short");
      CHECK_THROWS(get_reader(file_too_short));
    }
  }
}

SCENARIO("When I try to read a file with an invalid magic number with the "
         "public interface, an exception is thrown",
         "[interface][header_too_short]") {
  GIVEN("A path to a file with an invalid magic number") {
    THEN("an exception is thrown") {
      // CHECK_THROWS_WITH(get_reader(invalid_magic_number),
      // "invalid_magic_number");
      CHECK_THROWS(get_reader(invalid_magic_number));
    }
  }
}

SCENARIO("When I try to read a valid file with the public interface, no "
         "exception is thrown",
         "[interface][valid_file]") {
  GIVEN("A path to a valid file") {
    THEN("No exception is thrown") { CHECK_NOTHROW(get_reader(file1)); }
  }
}

SCENARIO("When the end_of_data signal is sent, the sink get it",
         "[interface][end_of_data]")
{
  struct DataSinkFoo {
    bool* got_end_of_data{nullptr};

    DataSinkFoo(bool* _got_end_of_data) : got_end_of_data(_got_end_of_data) {}

    void set_properties([[maybe_unused]] const Properties &_properties) {}
    void push_row([[maybe_unused]] const size_t _irow,
                  [[maybe_unused]] Column::PBUF _p) {}
    void end_of_data() { *got_end_of_data = true; }
  };

  GIVEN("A reader and a valid file") {
    bool got_end_of_data = false;
    auto test = get_reader(file1, DataSinkFoo(&got_end_of_data));
    WHEN("The end_of_data signal is sent") {
      test.end_of_data();
      THEN("The sink get it") {
        CHECK(got_end_of_data == true);
      }
    }
  }
}

#include <charconv>
#include <type_traits>

namespace {
struct MyTestDataSink {
  COLUMNS columns;
  json ref_header;
  json ref_columns;
  using iterator = decltype(std::declval<const json>()[""].items().begin());
  iterator ref_data_iter;
  iterator ref_data_iter_end;
  size_t ref_irow{0};
  size_t row_read{0};
  size_t ncols{0};

  template <typename _iter>
  MyTestDataSink(const json &_header, const json &_columns,
                 _iter &&_ref_data_iter, _iter &&_ref_data_iter_end)
      : ref_header(_header), ref_columns(_columns),
        ref_data_iter(std::forward<_iter>(_ref_data_iter)),
        ref_data_iter_end(std::forward<_iter>(_ref_data_iter_end)) {
    get_ref_irow();
  }

  void get_ref_irow() {
    const std::string line = ref_data_iter.key();
    std::from_chars(line.data(), line.data() + line.size(), ref_irow);
  };

  void set_properties([[maybe_unused]] const Properties &_properties) {
    columns = COLUMNS(_properties /*.metadata*/.columns);
    ncols = columns.size();

    CHECK(_properties.platform == get_platform(ref_header["platform"]));
    // CHECK(_properties.encoding == ref_header["encoding"]);
    CHECK(_properties.dataset_name == ref_header["name"]);
    CHECK(_properties.file_type == ref_header["file_type"]);
    CHECK(get_dt(_properties.date_created) ==
          get_dt(get_datetime(ref_header["date_created"])));
    CHECK(get_dt(_properties.date_modified) ==
          get_dt(get_datetime(ref_header["date_modified"])));
    CHECK(_properties.sas_release == ref_header["sas_release"]);
    CHECK(_properties.sas_server_type == ref_header["server_type"]);
    CHECK(_properties.os_type == ref_header["os_type"]);
    CHECK(_properties.os_name == ref_header["os_name"]);
    CHECK(_properties.header_length == ref_header["header_length"]);
    CHECK(_properties.page_length == ref_header["page_length"]);
    CHECK(_properties.page_count == ref_header["page_count"]);

    CHECK(_properties.creator == get_string(ref_header["creator"]));
    CHECK(_properties.creator_proc == get_string(ref_header["creator_proc"]));
    CHECK(_properties.row_length == ref_header["row_length"]);
    CHECK(_properties.row_count == ref_header["row_count"]);
    CHECK(_properties.col_count_p1 == ref_header["col_count_p1"]);
    CHECK(_properties.col_count_p2 == ref_header["col_count_p2"]);
    CHECK(_properties.mix_page_row_count == ref_header["mix_page_row_count"]);
    CHECK(_properties.lcs == ref_header["lcs"]);
    CHECK(_properties.lcp == ref_header["lcp"]);
    CHECK(_properties.compression ==
          get_compression(ref_header["compression"]));
    REQUIRE(_properties.columns.size() == ref_columns.size());
    for (size_t icol = 0; icol < _properties.columns.size(); ++icol) {
      const auto &column = _properties.columns[icol];
      const auto &ref_column = ref_columns[icol];
      INFO("Col#" << icol << " name=" << ref_column["name"]);
      CHECK(column.name == get_string(ref_column["name"]));
      // CHECK(column.label == get_string(ref_column["label"]));
      CHECK(column.format == get_string(ref_column["format"]));
      // CHECK(column.data_offset() == ref_column["offset"]);
      // CHECK(column.data_length() == ref_column["length"]);
      if (get_column_type(ref_column["type"]) ==
          cppsas7bdat::Column::Type::string) {
        CHECK(column.type == cppsas7bdat::Column::Type::string);
      } else {
        CHECK(column.type >= cppsas7bdat::Column::Type::number);
      }
    }
  }

  void push_row([[maybe_unused]] const size_t _irow,
                [[maybe_unused]] Column::PBUF _p) {
    if (_irow == ref_irow) {
      const auto values = ref_data_iter.value();
      for (size_t icol = 0; icol < ncols; ++icol) {
        const auto &column = columns[icol];
        const auto refval = values[icol];
        INFO("Colname=" << column.name << '[' << icol << "] row=" << ref_irow);
        switch (column.type) {
        case cppsas7bdat::Column::Type::string:
          CHECK(std::string(column.get_string(_p)) == refval);
          break;
        case cppsas7bdat::Column::Type::integer:
          CHECK(column.get_integer(_p) == refval);
          break;
        case cppsas7bdat::Column::Type::number: {
          auto x = column.get_number(_p);
          if (std::isnan(x)) {
            CHECK(refval.is_null());
          } else {
            CHECK(x == refval);
          }
        } break;
        case cppsas7bdat::Column::Type::datetime:
          CHECK(column.get_datetime(_p) == get_datetime(refval));
          break;
        case cppsas7bdat::Column::Type::date:
          CHECK(column.get_date(_p) == get_date(refval));
          break;
        case cppsas7bdat::Column::Type::time:
          CHECK(column.get_time(_p) == get_time(refval));
          break;
        default:
          CHECK(false);
        }
      }
      ++ref_data_iter;
      if (ref_data_iter != ref_data_iter_end)
        get_ref_irow();
    }
    ++row_read;
  }

  void end_of_data() const noexcept {}
};
} // namespace

SCENARIO("When I read a file with the public interface, the properties and "
         "data are read properly",
         "[inteface][read_data]") {
  const auto data =
      GENERATE(from_range(files().j.items().begin(), files().j.items().end()));

  const std::string filename = data.key();
  const auto ref_header = data.value()["Header"];
  const auto ref_columns = data.value()["Columns"];
  auto ref_data = data.value()["Data"].items();

  GIVEN(fmt::format("A file {},", filename)) {
    // Skip big5 files
    if (filename.find("big5") != filename.npos)
      return;
    WHEN("The data is read") {
      auto reader = get_reader(filename, MyTestDataSink(ref_header, ref_columns,
                                                        ref_data.begin(),
                                                        ref_data.end()));
      THEN("The data values are correct - read_all") {
        CHECK(reader.current_row_index() == 0);
        reader.read_all();
        CHECK(reader.current_row_index() ==
              reader.properties() /*.metadata*/.row_count);
      }
      THEN("The data values are correct - read_row") {
        size_t irow{0};
        CHECK(reader.current_row_index() == irow);
        while (reader.read_row()) {
          irow++;
          CHECK(reader.current_row_index() == irow);
        }
        CHECK(reader.current_row_index() ==
              reader.properties() /*.metadata*/.row_count);
      }
      THEN("The data values are correct - read_rows") {
        size_t irow{0};
        CHECK(reader.current_row_index() == irow);
        while (reader.read_rows(50)) {
          irow += 50;
          CHECK(reader.current_row_index() == irow);
        }
        CHECK(reader.current_row_index() ==
              reader.properties() /*.metadata*/.row_count);
      }
      THEN("The data values are correct - read_row_no_sink") {
        MyTestDataSink sink(ref_header, ref_columns, ref_data.begin(),
                            ref_data.end());
        sink.set_properties(reader.properties());
        size_t irow{0};
        CHECK(reader.current_row_index() == irow);
        while (auto p = reader.read_row_no_sink()) {
          sink.push_row(irow, p);
          irow++;
          CHECK(reader.current_row_index() == irow);
        }
        CHECK(reader.current_row_index() ==
              reader.properties() /*.metadata*/.row_count);
      }
      THEN("I can skip rows") {
        size_t irow{0}, ref_irow{0};
        for (auto ref_row : ref_data) {
          {
            const std::string line = ref_row.key();
            std::from_chars(line.data(), line.data() + line.size(), ref_irow);
          }
          CHECK(reader.current_row_index() == irow);
          CHECK(reader.skip(ref_irow - irow) == true);
          CHECK(reader.read_row() == true);
          irow = ref_irow + 1;
        }
        CHECK(reader.current_row_index() ==
              reader.properties() /*.metadata*/.row_count);
      }
    }
  }
}
