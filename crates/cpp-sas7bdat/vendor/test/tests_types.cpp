/**
 *  \file tests/tests_types.cpp
 *
 *  \brief
 *
 *  \author  Olivia Quinet
 */

#include <catch2/catch_test_macros.hpp>
#include <cppsas7bdat/types.hpp>

using namespace cppsas7bdat;

SCENARIO("I can convert datetime to string") {
  CHECK_THROWS(cppsas7bdat::to_string(DATETIME()) == "");
  CHECK(cppsas7bdat::to_string(DATETIME(DATE(1976, 6, 30))) ==
        "1976-06-30 00:00:00");
  CHECK(cppsas7bdat::to_string(DATETIME(DATE(1976, 6, 30), TIME(1, 2, 3))) ==
        "1976-06-30 01:02:03");
  CHECK(cppsas7bdat::to_string(
            DATETIME(DATE(1976, 6, 30), TIME(1, 2, 3, 123456))) ==
        "1976-06-30 01:02:03.123456");
}

SCENARIO("I can convert date to string") {
  CHECK_THROWS(cppsas7bdat::to_string(DATE()) == "");
  CHECK(cppsas7bdat::to_string(DATE(1976, 6, 30)) == "1976-06-30");
}

SCENARIO("I can convert time to string") {
  CHECK(cppsas7bdat::to_string(TIME(1, 2, 3)) == "01:02:03");
  CHECK(cppsas7bdat::to_string(TIME(1, 2, 3, 123456)) == "01:02:03.123456");
}
