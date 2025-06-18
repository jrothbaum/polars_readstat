/**
 *  \file tests/tests_encodings.cpp
 *
 *  \brief
 *
 *  \author  Olivia Quinet
 */

#include "../src/encodings.hpp"
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators_all.hpp>
#include <fmt/core.h>
#include <vector>

SCENARIO("I can get string representation of the encoding", "[encoding]") {
  static std::vector<bool> passed(256, false);
  static constexpr const char *SAS_DEFAULT_STRING_ENCODING = "WINDOWS-1252";

  GIVEN("A list of encodings") {
    auto encoding = GENERATE(
        std::make_pair(0, SAS_DEFAULT_STRING_ENCODING),
        std::make_pair(20, "UTF-8"), std::make_pair(28, "US-ASCII"),
        std::make_pair(29, "ISO-8859-1"), std::make_pair(30, "ISO-8859-2"),
        std::make_pair(31, "ISO-8859-3"), std::make_pair(32, "ISO-8859-4"),
        std::make_pair(33, "ISO-8859-5"), std::make_pair(34, "ISO-8859-6"),
        std::make_pair(35, "ISO-8859-7"), std::make_pair(36, "ISO-8859-8"),
        std::make_pair(37, "ISO-8859-9"), std::make_pair(39, "ISO-8859-11"),
        std::make_pair(40, "ISO-8859-15"), std::make_pair(41, "CP437"),
        std::make_pair(42, "CP850"), std::make_pair(43, "CP852"),
        std::make_pair(44, "CP857"), std::make_pair(45, "CP858"),
        std::make_pair(46, "CP862"), std::make_pair(47, "CP864"),
        std::make_pair(48, "CP865"), std::make_pair(49, "CP866"),
        std::make_pair(50, "CP869"), std::make_pair(51, "CP874"),
        std::make_pair(52, "CP921"), std::make_pair(53, "CP922"),
        std::make_pair(54, "CP1129"), std::make_pair(55, "CP720"),
        std::make_pair(56, "CP737"), std::make_pair(57, "CP775"),
        std::make_pair(58, "CP860"), std::make_pair(59, "CP863"),
        std::make_pair(60, "WINDOWS-1250"), std::make_pair(61, "WINDOWS-1251"),
        std::make_pair(62, "WINDOWS-1252"), std::make_pair(63, "WINDOWS-1253"),
        std::make_pair(64, "WINDOWS-1254"), std::make_pair(65, "WINDOWS-1255"),
        std::make_pair(66, "WINDOWS-1256"), std::make_pair(67, "WINDOWS-1257"),
        std::make_pair(68, "WINDOWS-1258"), std::make_pair(69, "MACROMAN"),
        std::make_pair(70, "MACARABIC"), std::make_pair(71, "MACHEBREW"),
        std::make_pair(72, "MACGREEK"), std::make_pair(73, "MACTHAI"),
        std::make_pair(75, "MACTURKISH"), std::make_pair(76, "MACUKRAINE"),
        std::make_pair(118, "CP950"), std::make_pair(119, "EUC-TW"),
        std::make_pair(123, "BIG5-HKSCS"), std::make_pair(125, "GB18030"),
        std::make_pair(126, "CP936"), std::make_pair(128, "CP1381"),
        std::make_pair(134, "EUC-JP"), std::make_pair(136, "CP949"),
        std::make_pair(137, "CP942"), std::make_pair(138, "CP932"),
        std::make_pair(140, "EUC-KR"), std::make_pair(141, "CP949"),
        std::make_pair(142, "CP949"), std::make_pair(163, "MACICELAND"),
        std::make_pair(167, "ISO-2022-JP"), std::make_pair(168, "ISO-2022-KR"),
        std::make_pair(169, "ISO-2022-CN"),
        std::make_pair(172, "ISO-2022-CN-EXT"),
        std::make_pair(204, SAS_DEFAULT_STRING_ENCODING),
        std::make_pair(205, "GB18030"), std::make_pair(227, "ISO-8859-14"),
        std::make_pair(242, "ISO-8859-13"), std::make_pair(245, "MACCROATIAN"),
        std::make_pair(246, "MACCYRILLIC"), std::make_pair(247, "MACROMANIA"),
        std::make_pair(248, "SHIFT_JISX0213"));

    GIVEN(fmt::format("An integer [{}]", encoding.first)) {
      THEN(fmt::format("I get the correct string [{}]", encoding.second)) {
        CHECK(cppsas7bdat::INTERNAL::get_encoding(encoding.first) ==
              encoding.second);
        passed[encoding.first] = true;
      }
    }
  }

  GIVEN("The unassigned encodings") {
    for (size_t i = 0; i < 256; ++i) {
      if (!passed[i]) {
        INFO(fmt::format("An integer: [{}]", i));
        CHECK(cppsas7bdat::INTERNAL::get_encoding(i) ==
              SAS_DEFAULT_STRING_ENCODING);
      }
    }
  }
}
