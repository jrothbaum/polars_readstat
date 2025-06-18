/**
 *  \file tests/tests_formatters.cpp
 *
 *  \brief
 *
 *  \author  Olivia Quinet
 */

#include "../src/sas7bdat-impl.hpp"
#include <catch2/catch_test_macros.hpp>

using namespace cppsas7bdat;
using namespace cppsas7bdat::INTERNAL::FORMATTER;

SCENARIO("The IFormatter returns missing values") {
  GIVEN("An instance of the formatter") {
    const auto instance = IFormatter(1, 2, Type::string);
    THEN("It is correctly initialized") {
      CHECK(instance.offset == 1);
      CHECK(instance.length == 2);
      CHECK(instance.type == Type::string);
    }
    WHEN("Values are formatted") {
      THEN("Missing values are returned") {
        CHECK(instance.get_string(nullptr) == std::string());
        CHECK(std::isnan(instance.get_number(nullptr)) == true);
        CHECK(instance.get_integer(nullptr) == 0);
        CHECK(instance.get_datetime(nullptr).is_not_a_date_time() == true);
        CHECK(instance.get_date(nullptr).is_not_a_date() == true);
        CHECK(instance.get_time(nullptr).is_not_a_date_time() == true);
        CHECK(instance.to_string(nullptr) == std::string());
      }
    }
  }
}

SCENARIO("The NoFormatter creates a IFormatter with the unknown type") {
  GIVEN("An instance of the formatter") {
    const auto instance = NoFormatter(1, 2);
    THEN("It is correctly initialized") {
      CHECK(instance.offset == 1);
      CHECK(instance.length == 2);
      CHECK(instance.type == Type::unknown);
    }
  }
}

SCENARIO("The StringFormatter creates a IFormatter with the string type") {
  GIVEN("An instance of the formatter") {
    const auto instance = StringFormatter(1, 2);
    THEN("It is correctly initialized") {
      CHECK(instance.offset == 1);
      CHECK(instance.length == 2);
      CHECK(instance.type == Type::string);
    }
    WHEN("Values are formatted") {
      THEN("Correct values are returned") {
        constexpr auto *p = "abcdef";
        CHECK(instance.get_string(p) == "bc");
        CHECK(instance.to_string(p) == "bc");
      }
    }
  }
}

SCENARIO(
    "The SmallIntegerFormatter creates a IFormatter with the integer type") {
  GIVEN("An instance of the formatter") {
    const auto instance = SmallIntegerFormatter(1, 2);
    THEN("It is correctly initialized") {
      CHECK(instance.offset == 1);
      CHECK(instance.length == 2);
      CHECK(instance.type == Type::integer);
    }
    WHEN("Values are formatted") {
      THEN("Correct values are returned") {
        constexpr char p[2] = {0x20, 0x40};
        CHECK(instance.get_integer(p) == 64);
        CHECK(instance.get_number(p) == 64);
        CHECK(instance.to_string(p) == "64");
      }
    }
  }
}

SCENARIO("The IntegerFormatter creates a IFormatter with the integer type") {
  GIVEN("An instance of the formatter(Endian::little)") {
    const auto instance = IntegerFormatter<Endian::little, int16_t>(1, 2);
    THEN("It is correctly initialized") {
      CHECK(instance.offset == 1);
      CHECK(instance.length == 2);
      CHECK(instance.type == Type::integer);
    }
    WHEN("Values are formatted") {
      THEN("Correct values are returned") {
        constexpr char p[3] = {0x20, 0x40, 0x00};
        CHECK(instance.get_integer(p) == 64);
        CHECK(instance.get_number(p) == 64);
        CHECK(instance.to_string(p) == "64");
      }
    }
  }
  GIVEN("An instance of the formatter(Endian::big)") {
    const auto instance = IntegerFormatter<Endian::big, int16_t>(1, 2);
    THEN("It is correctly initialized") {
      CHECK(instance.offset == 1);
      CHECK(instance.length == 2);
      CHECK(instance.type == Type::integer);
    }
    WHEN("Values are formatted") {
      THEN("Correct values are returned") {
        constexpr char p[3] = {0x20, 0x00, 0x40};
        CHECK(instance.get_integer(p) == 64);
        CHECK(instance.get_number(p) == 64);
        CHECK(instance.to_string(p) == "64");
      }
    }
  }
}

SCENARIO("The DoubleFormatter creates a IFormatter with the number type") {
  GIVEN("An instance of the formatter(Endian::little)") {
    const auto instance = DoubleFormatter<Endian::little>(0, 8);
    THEN("It is correctly initialized") {
      CHECK(instance.offset == 0);
      CHECK(instance.length == 8);
      CHECK(instance.type == Type::number);
    }
    WHEN("Values are formatted") {
      THEN("Correct values are returned") {
        constexpr char p[8] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
        CHECK(instance.get_number(p) == 0);
        CHECK(instance.get_integer(p) == 0);
        CHECK(instance.to_string(p) == "0.000000");
      }
    }
  }
  GIVEN("An instance of the formatter(Endian::big)") {
    const auto instance = DoubleFormatter<Endian::big>(0, 8);
    THEN("It is correctly initialized") {
      CHECK(instance.offset == 0);
      CHECK(instance.length == 8);
      CHECK(instance.type == Type::number);
    }
    WHEN("Values are formatted") {
      THEN("Correct values are returned") {
        constexpr char p[8] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
        CHECK(instance.get_number(p) == 0);
        CHECK(instance.get_integer(p) == 0);
        CHECK(instance.to_string(p) == "0.000000");
      }
    }
  }
}

SCENARIO(
    "The IncompleteDoubleFormatter creates a IFormatter with the number type") {
  GIVEN("An instance of the formatter(Endian::little)") {
    const auto instance = IncompleteDoubleFormatter<Endian::little, 6>(0, 6);
    THEN("It is correctly initialized") {
      CHECK(instance.offset == 0);
      CHECK(instance.length == 6);
      CHECK(instance.type == Type::number);
    }
    WHEN("Values are formatted") {
      THEN("Correct values are returned") {
        constexpr char p[6] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
        CHECK(instance.get_number(p) == 0);
        CHECK(instance.get_integer(p) == 0);
        CHECK(instance.to_string(p) == "0.000000");
      }
    }
  }
  GIVEN("An instance of the formatter(Endian::big)") {
    const auto instance = IncompleteDoubleFormatter<Endian::big, 6>(0, 6);
    THEN("It is correctly initialized") {
      CHECK(instance.offset == 0);
      CHECK(instance.length == 6);
      CHECK(instance.type == Type::number);
    }
    WHEN("Values are formatted") {
      THEN("Correct values are returned") {
        constexpr char p[6] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
        CHECK(instance.get_number(p) == 0);
        CHECK(instance.get_integer(p) == 0);
        CHECK(instance.to_string(p) == "0.000000");
      }
    }
  }
}

SCENARIO("The DateTimeFormatter creates a IFormatter with the datetime type") {
  GIVEN("An instance of the formatter(Endian::little)") {
    const auto instance = DateTimeFormatter<Endian::little>(0, 8);
    THEN("It is correctly initialized") {
      CHECK(instance.offset == 0);
      CHECK(instance.length == 8);
      CHECK(instance.type == Type::datetime);
    }
    WHEN("Values are formatted") {
      THEN("Correct values are returned") {
        constexpr char p[8] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
        CHECK(instance.get_datetime(p) ==
              DATETIME(DATE(1960, 1, 1), TIME(0, 0, 0)));
        CHECK(instance.get_date(p) == DATE(1960, 1, 1));
        CHECK(instance.get_time(p) == TIME(0, 0, 0));
        CHECK(instance.to_string(p) == "1960-01-01 00:00:00");
      }
    }
  }
  GIVEN("An instance of the formatter(Endian::big)") {
    const auto instance = DateTimeFormatter<Endian::big>(0, 8);
    THEN("It is correctly initialized") {
      CHECK(instance.offset == 0);
      CHECK(instance.length == 8);
      CHECK(instance.type == Type::datetime);
    }
    WHEN("Values are formatted") {
      THEN("Correct values are returned") {
        constexpr char p[8] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
        CHECK(instance.get_datetime(p) ==
              DATETIME(DATE(1960, 1, 1), TIME(0, 0, 0)));
        CHECK(instance.get_date(p) == DATE(1960, 1, 1));
        CHECK(instance.get_time(p) == TIME(0, 0, 0));
        CHECK(instance.to_string(p) == "1960-01-01 00:00:00");
      }
    }
  }
}

SCENARIO("The DateFormatter creates a IFormatter with the date type") {
  GIVEN("An instance of the formatter(Endian::little)") {
    const auto instance = DateFormatter<Endian::little>(0, 8);
    THEN("It is correctly initialized") {
      CHECK(instance.offset == 0);
      CHECK(instance.length == 8);
      CHECK(instance.type == Type::date);
    }
    WHEN("Values are formatted") {
      THEN("Correct values are returned") {
        constexpr char p[8] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
        CHECK(instance.get_datetime(p) ==
              DATETIME(DATE(1960, 1, 1), TIME(0, 0, 0)));
        CHECK(instance.get_date(p) == DATE(1960, 1, 1));
        CHECK(instance.get_time(p) == TIME(0, 0, 0));
        CHECK(instance.to_string(p) == "1960-01-01");
      }
    }
  }
  GIVEN("An instance of the formatter(Endian::big)") {
    const auto instance = DateFormatter<Endian::big>(0, 8);
    THEN("It is correctly initialized") {
      CHECK(instance.offset == 0);
      CHECK(instance.length == 8);
      CHECK(instance.type == Type::date);
    }
    WHEN("Values are formatted") {
      THEN("Correct values are returned") {
        constexpr char p[8] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
        CHECK(instance.get_datetime(p) ==
              DATETIME(DATE(1960, 1, 1), TIME(0, 0, 0)));
        CHECK(instance.get_date(p) == DATE(1960, 1, 1));
        CHECK(instance.get_time(p) == TIME(0, 0, 0));
        CHECK(instance.to_string(p) == "1960-01-01");
      }
    }
  }
}

SCENARIO("The TimeFormatter creates a IFormatter with the time type") {
  GIVEN("An instance of the formatter(Endian::little)") {
    const auto instance = TimeFormatter<Endian::little>(0, 8);
    THEN("It is correctly initialized") {
      CHECK(instance.offset == 0);
      CHECK(instance.length == 8);
      CHECK(instance.type == Type::time);
    }
    WHEN("Values are formatted") {
      THEN("Correct values are returned") {
        constexpr char p[8] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
        CHECK(instance.get_datetime(p).is_not_a_date_time());
        CHECK(instance.get_date(p).is_not_a_date());
        CHECK(instance.get_time(p) == TIME(0, 0, 0));
        CHECK(instance.to_string(p) == "00:00:00");
      }
    }
  }
  GIVEN("An instance of the formatter(Endian::big)") {
    const auto instance = TimeFormatter<Endian::big>(0, 8);
    THEN("It is correctly initialized") {
      CHECK(instance.offset == 0);
      CHECK(instance.length == 8);
      CHECK(instance.type == Type::time);
    }
    WHEN("Values are formatted") {
      THEN("Correct values are returned") {
        constexpr char p[8] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
        CHECK(instance.get_datetime(p).is_not_a_date_time());
        CHECK(instance.get_date(p).is_not_a_date());
        CHECK(instance.get_time(p) == TIME(0, 0, 0));
        CHECK(instance.to_string(p) == "00:00:00");
      }
    }
  }
}
