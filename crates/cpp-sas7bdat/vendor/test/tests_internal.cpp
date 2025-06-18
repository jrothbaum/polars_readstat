/**
 *  \file tests/tests_internal.cpp
 *
 *  \brief
 *
 *  \author  Olivia Quinet
 */

#include "../include/cppsas7bdat/source/ifstream.hpp"
#include "../src/sas7bdat-impl.hpp"
#include "data.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators_all.hpp>
#include <exception>
#include <iostream>
#include <string>
#include <vector>

namespace {
cppsas7bdat::INTERNAL::DATASOURCE open_file(const std::string &_filename) {
  return cppsas7bdat::Reader::build_source(
      cppsas7bdat::datasource::ifstream(convert_path(_filename).c_str()));
}
} // namespace

SCENARIO("When I try to read a non existing file, an exception is thrown",
         "[internal][not_a_valid_file]") {
  GIVEN("An invalid path") {
    THEN("An exception is thrown") {
      // CHECK_THROWS_WITH(cppsas7bdat::INTERNAL::open_stream(invalid_path),
      // "not_a_valid_file");
      CHECK_THROWS(cppsas7bdat::INTERNAL::open_stream(invalid_path.c_str()));
    }
  }
}

SCENARIO("When I try to read anexisting file, no exception is thrown",
         "[internal][not_a_valid_file]") {
  GIVEN("A valid path") {
    THEN("No exception is thrown") {
      CHECK_NOTHROW(cppsas7bdat::INTERNAL::open_stream(file_too_short.c_str()));
    }
  }
}

SCENARIO("When I try to read a file too short, an exception is thrown",
         "[internal][file too short]") {
  GIVEN("A path to a too short file") {
    THEN("an exception is thrown") {
      // CHECK_THROWS_WITH(cppsas7bdat::INTERNAL::CHECK_HEADER(open_file(file_too_short)),
      // "header_too_short");
      CHECK_THROWS(
          cppsas7bdat::INTERNAL::CHECK_HEADER(open_file(file_too_short)));
    }
  }
}

SCENARIO(
    "When I try to read a file that's not too short, no exception is thrown",
    "[internal][file too short]") {
  GIVEN("A path to a file with the minimum size") {
    THEN("No exception is thrown") {
      CHECK_NOTHROW(
          cppsas7bdat::INTERNAL::CHECK_HEADER(open_file(invalid_magic_number)));
    }
  }
}

SCENARIO("When I try to read a file with an invalid magic number, an exception "
         "is thrown",
         "[internal][header_too_short]") {
  // cppsas7bdat::Properties::Header header;
  GIVEN("A path to a file with an invalid magic number") {
    auto ch =
        cppsas7bdat::INTERNAL::CHECK_HEADER(open_file(invalid_magic_number));
    WHEN("I check the magic number") {
      THEN("an exception is thrown") {
        // CHECK_THROWS_WITH(ch.check_magic_number(), "invalid_magic_number");
        CHECK_THROWS(ch.check_magic_number());
      }
    }
  }
}

SCENARIO("When I try to read a file with an valid magic number, no exception "
         "is thrown",
         "[internal][header_too_short]") {
  GIVEN("A path to a valid file") {
    auto ch = cppsas7bdat::INTERNAL::CHECK_HEADER(open_file(file1));
    WHEN("I check the magic number") {
      THEN("No exception is thrown") { CHECK_NOTHROW(ch.check_magic_number()); }
    }
  }
}

SCENARIO("When I read a file, the align/endianness values are set properly",
         "[internal][set_align_endianness]") {
  const auto data =
      GENERATE(from_range(files().j.items().begin(), files().j.items().end()));

  const std::string filename = data.key();
  const auto ref_header = data.value()["Header"];
  GIVEN(fmt::format("A file {},", filename)) {
    cppsas7bdat::Properties::Header header;
    auto ch = cppsas7bdat::INTERNAL::check_header(open_file(filename), &header);
    WHEN("The align values are set") {
      THEN("The align values are correct") {
        CHECK(header.format == get_format(ref_header["u64"]));
        CHECK(header.endianness == get_endianness(ref_header["endianess"]));
      }
    }
  }
}

SCENARIO("When I read a file, the header is set properly",
         "[internal][read_header]") {
  const auto data =
      GENERATE(from_range(files().j.items().begin(), files().j.items().end()));

  const std::string filename = data.key();
  const auto ref_header = data.value()["Header"];
  // std::cerr << "File=" << filename << std::endl;
  GIVEN(fmt::format("A file {},", filename)) {
    cppsas7bdat::Properties::Header header;
    WHEN("The header is read") {
      cppsas7bdat::READ::header(open_file(filename), &header);
      THEN("The header values are correct") {
        CHECK(header.platform == get_platform(ref_header["platform"]));
        // CHECK(header.encoding == ref_header["encoding"]);
        CHECK(header.dataset_name == ref_header["name"]);
        CHECK(header.file_type == ref_header["file_type"]);
        CHECK(get_dt(header.date_created) ==
              get_dt(get_datetime(ref_header["date_created"])));
        CHECK(get_dt(header.date_modified) ==
              get_dt(get_datetime(ref_header["date_modified"])));
        CHECK(header.sas_release == ref_header["sas_release"]);
        CHECK(header.sas_server_type == ref_header["server_type"]);
        CHECK(header.os_type == ref_header["os_type"]);
        CHECK(header.os_name == ref_header["os_name"]);
        CHECK(header.header_length == ref_header["header_length"]);
        CHECK(header.page_length == ref_header["page_length"]);
        CHECK(header.page_count == ref_header["page_count"]);
      }
    }
  }
}

SCENARIO("When I read a file, the metadata is set properly",
         "[internal][read_metadata]") {
  const auto data =
      GENERATE(from_range(files().j.items().begin(), files().j.items().end()));

  const std::string filename = data.key();
  const auto ref_header = data.value()["Header"];
  const auto ref_columns = data.value()["Columns"];
  GIVEN(fmt::format("A file {},", filename)) {
    cppsas7bdat::Properties::Header header;
    cppsas7bdat::Properties::Metadata metadata;
    WHEN("The metadata is read") {
      cppsas7bdat::READ::metadata(open_file(filename), &header, &metadata, {});
      THEN("The metadata values are correct") {
        CHECK(metadata.creator == get_string(ref_header["creator"]));
        CHECK(metadata.creator_proc == get_string(ref_header["creator_proc"]));
        CHECK(metadata.row_length == ref_header["row_length"]);
        CHECK(metadata.row_count == ref_header["row_count"]);
        CHECK(metadata.col_count_p1 == ref_header["col_count_p1"]);
        CHECK(metadata.col_count_p2 == ref_header["col_count_p2"]);
        CHECK(metadata.mix_page_row_count == ref_header["mix_page_row_count"]);
        CHECK(metadata.lcs == ref_header["lcs"]);
        CHECK(metadata.lcp == ref_header["lcp"]);
        CHECK(metadata.compression ==
              get_compression(ref_header["compression"]));
        REQUIRE(metadata.columns.size() == ref_columns.size());
        for (size_t icol = 0; icol < metadata.columns.size(); ++icol) {
          const auto &column = metadata.columns[icol];
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
    }
  }
}

#include <charconv>

SCENARIO("When I read a file, the data are read properly",
         "[internal][read_data]") {
  const auto data =
      GENERATE(from_range(files().j.items().begin(), files().j.items().end()));

  const std::string filename = data.key();
  const auto ref_header = data.value()["Header"];
  const auto ref_columns = data.value()["Columns"];
  auto ref_data = data.value()["Data"].items();
  auto ref_data_iter = ref_data.begin();
  const size_t ncols = ref_columns.size();

  size_t ref_irow;
  auto get_ref_irow = [&]() {
    const std::string line = ref_data_iter.key();
    std::from_chars(line.data(), line.data() + line.size(), ref_irow);
  };
  get_ref_irow();

  GIVEN(fmt::format("A file {},", filename)) {
    // Skip big5 files
    if (filename.find("big5") != filename.npos)
      return;
    cppsas7bdat::Properties::Header header;
    cppsas7bdat::Properties::Metadata metadata;
    WHEN("The data is read") {
      auto rd =
          cppsas7bdat::READ::data(open_file(filename), &header, &metadata, {});
      THEN("The data values are correct") {
        size_t row_read = 0;

        while (auto vals = cppsas7bdat::INTERNAL::read_line(rd)) {
          if (row_read == ref_irow) {
            const auto values = ref_data_iter.value();

            const cppsas7bdat::INTERNAL::BYTES sv = *vals;
            const void *p = sv.data();
            for (size_t icol = 0; icol < ncols; ++icol) {
              const auto &column = metadata.columns[icol];
              const auto refval = values[icol];
              INFO("Colname=" << column.name << '[' << icol
                              << "] row=" << ref_irow);
              switch (column.type) {
              case cppsas7bdat::Column::Type::string:
                CHECK(std::string(column.get_string(p)) == refval);
                break;
              case cppsas7bdat::Column::Type::integer:
                CHECK(column.get_integer(p) == refval);
                break;
              case cppsas7bdat::Column::Type::number: {
                auto x = column.get_number(p);
                if (std::isnan(x)) {
                  CHECK(refval.is_null());
                } else {
                  CHECK(x == refval);
                }
              } break;
              case cppsas7bdat::Column::Type::datetime:
                CHECK(column.get_datetime(p) == get_datetime(refval));
                break;
              case cppsas7bdat::Column::Type::date:
                CHECK(column.get_date(p) == get_date(refval));
                break;
              case cppsas7bdat::Column::Type::time:
                CHECK(column.get_time(p) == get_time(refval));
                break;
              default:
                CHECK(false);
              }
            }
            ++ref_data_iter;
            if (ref_data_iter != ref_data.end())
              get_ref_irow();
          }
          ++row_read;
        }
        CHECK(row_read == ref_header["row_count"]);
        CHECK(ref_data_iter == ref_data.end());
      }
    }
  }
}
