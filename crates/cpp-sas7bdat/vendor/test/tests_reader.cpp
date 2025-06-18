/**
 *  \file tests/tests_reader.cpp
 *
 *  \brief
 *
 *  \author  Olivia Quinet
 */

//#include <catch2/catch.hpp>
#include <catch2/catch_test_macros.hpp>
#include "../include/cppsas7bdat/reader.hpp"

using namespace cppsas7bdat;

SCENARIO("I can create a Reader instance", "[interface][reader]")
{
  GIVEN("The default constructor") {
    Reader test;

    WHEN("I access the properties") {
      THEN("I get empty properties") {
        CHECK(test.properties().filename.empty());
      }
    }

    WHEN("I call the different methods") {
      THEN("I don't get an exception") {
        CHECK_NOTHROW(test.end_of_data());
        CHECK_NOTHROW(test.skip(0));
        CHECK_NOTHROW(test.read_all());
        CHECK_NOTHROW(test.read_row());
        CHECK_NOTHROW(test.read_rows(0));
        CHECK_NOTHROW(test.read_row_no_sink());
        CHECK_NOTHROW(test.current_row_index());
      }
    }
  }
}

SCENARIO("The Reader instance can be moved", "[interface][reader]")
{
  GIVEN("The default constructor") {
    Reader test;
    WHEN("I try to move it - operator=") {
      THEN("it works") {
        CHECK_NOTHROW(test = std::move(test));
      }
    }
    WHEN("I try to move it - Reader(&&)") {
      THEN("it works") {
        CHECK_NOTHROW(Reader(std::move(test)));
      }
    }
  }
}
