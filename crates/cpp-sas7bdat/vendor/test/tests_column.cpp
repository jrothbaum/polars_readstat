/**
 *  \file tests/tests_column.cpp
 *
 *  \brief
 *
 *  \author  Olivia Quinet
 */

#include "../src/sas7bdat-impl.hpp"
#include <catch2/catch_test_macros.hpp>

using namespace cppsas7bdat;

namespace {
struct TestFormatter {
  const Column::Type type{Column::Type::unknown};
  const size_t length{2};

  explicit TestFormatter(const Column::Type _type = Column::Type::unknown)
      : type(_type) {}

  SV get_string(Column::PBUF) const { return "hello, world!"; }
  NUMBER get_number(Column::PBUF) const { return 3.1415; }
  INTEGER get_integer(Column::PBUF) const { return 42; }
  DATETIME get_datetime(Column::PBUF) const {
    return boost::posix_time::from_iso_extended_string(
        "1976-06-30T00:01:02.345");
  }
  DATE get_date(Column::PBUF) const {
    return boost::gregorian::from_string("1976-06-30");
  }
  TIME get_time(Column::PBUF) const {
    return boost::posix_time::duration_from_string("00:01:02.345");
  }
  STRING to_string(Column::PBUF) const { return "Hello, World!"; }
};
} // namespace

SCENARIO("The Column accepts a formatter and uses correctly") {
  GIVEN("A column with the test formatter") {
    const auto test = Column("name", "label", "format", TestFormatter{});
    THEN("It is correctly initialized") {
      CHECK(test.name == "name");
      CHECK(test.label == "label");
      CHECK(test.format == "format");
      CHECK(test.type == Column::Type::unknown);
      CHECK(test.length() == 2);
    }
    THEN("The formatting interface is working") {
      Column::PBUF p = nullptr;
      CHECK(test.get_string(p) == "hello, world!");
      CHECK(test.get_number(p) == 3.1415);
      CHECK(test.get_integer(p) == 42);
      CHECK(test.get_datetime(p) ==
            DATETIME(DATE(1976, 6, 30),
                     TIME(0, 1, 2) + boost::posix_time::milliseconds(345)));
      CHECK(test.get_date(p) == DATE(1976, 6, 30));
      CHECK(test.get_time(p) ==
            TIME(0, 1, 2) + boost::posix_time::milliseconds(345));
      CHECK(test.to_string(p) == "Hello, World!");
    }
  }
}

SCENARIO("A vector of columns is splitted in different column types") {
  GIVEN("A list of columns") {
    COLUMNS columns{
        Column("name1", "", "", TestFormatter(Column::Type::string)),
        Column("name2", "", "", TestFormatter(Column::Type::number)),
        Column("name3", "", "", TestFormatter(Column::Type::integer)),
        Column("name4", "", "", TestFormatter(Column::Type::datetime)),
        Column("name5", "", "", TestFormatter(Column::Type::date)),
        Column("name6", "", "", TestFormatter(Column::Type::time)),
    };
    WHEN("It is split") {
      Columns test(columns);
      THEN("I find all the columns in the different categories") {
        REQUIRE(test.strings.size() == 1);
        CHECK(test.strings[0].name == "name1");
        REQUIRE(test.numbers.size() == 1);
        CHECK(test.numbers[0].name == "name2");
        REQUIRE(test.integers.size() == 1);
        CHECK(test.integers[0].name == "name3");
        REQUIRE(test.datetimes.size() == 1);
        CHECK(test.datetimes[0].name == "name4");
        REQUIRE(test.dates.size() == 1);
        CHECK(test.dates[0].name == "name5");
        REQUIRE(test.times.size() == 1);
        CHECK(test.times[0].name == "name6");
      }
    }
  }
}
