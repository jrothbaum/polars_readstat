/**
 *  \file tests/tests_decompressors.cpp
 *
 *  \brief
 *
 *  \author  Olivia Quinet
 */

#include "../src/sas7bdat-impl.hpp"
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators_all.hpp>
#include <catch2/matchers/catch_matchers_all.hpp>

using namespace cppsas7bdat;
using namespace cppsas7bdat::INTERNAL;
using namespace cppsas7bdat::INTERNAL::DECOMPRESSOR;

namespace {
const BYTES operator"" _b(const char *_p, size_t _n) {
  auto p = reinterpret_cast<const unsigned char *>(_p);
  return BYTES(p, _n);
}
const auto source1{"a"_b};
const auto source2{"abcdefghijklmnopqrstuvwxyz"_b};
} // namespace

SCENARIO("When I pop values from a source, I get the expected values",
         "[internal][decompressor][SRC_VALUES][pop]") {
  const auto source = GENERATE(source1, source2);

  GIVEN("A source") {
    auto src = SRC_VALUES(source);
    THEN("The remaining is the length of the source") {
      CHECK(src.check(1) == true);
      CHECK(src.remaining() == source.size());
    }
    WHEN("I pop the first value") {
      auto test = src.pop();
      THEN("I get then expected value") {
        CHECK(test == source[0]);
        CHECK(src.remaining() == source.size() - 1);
      }
    }
    WHEN("I pop all the values in one go") {
      auto test = src.pop(source.size());
      THEN("I get then expected value") {
        CHECK(test == source);
        CHECK(src.remaining() == 0);
      }
    }
  }
}

SCENARIO("The None decompressor just does a perfect forward of the data",
         "[internal][decompressor][None]") {
  const auto source = GENERATE(source1, source2);

  GIVEN("The none decompressor and a source") {
    const auto decompressor = DECOMPRESSOR::None();
    WHEN("The source is passed to the decompressor") {
      auto test = decompressor(source);
      THEN("I get the source back") { CHECK(test == source); }
    }
  }
}

SCENARIO("I can store values in the destination",
         "[internal][decompressor][DST_VALUES]") {
  GIVEN("A destination of one character") {
    const auto dst = DST_VALUES<Endian::little, Format::bit32>((size_t)1);
    WHEN("I check if one value can be added") {
      THEN("It is accepted") {
        CHECK(dst.check() == true);
        CHECK(dst.check(1) == true);
        CHECK_NOTHROW(dst.assert_check(1));
      }
    }
    WHEN("I check if more than one value can be added") {
      THEN("It is rejected") {
        CHECK(dst.check(2) == false);
        CHECK_THROWS(dst.assert_check(2));
      }
    }
  }
}
