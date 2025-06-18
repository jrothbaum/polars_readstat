/**
 *  \file tests/tests_version.cpp
 *
 *  \brief
 *
 *  \author  Olivia Quinet
 */

#include <catch2/catch_test_macros.hpp>
#include <cppsas7bdat/version.hpp>

SCENARIO("I can get the version of the library") {
  CHECK(cppsas7bdat::getVersion() == "1.0.1");
}
