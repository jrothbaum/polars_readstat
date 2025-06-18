/**
 *  \file tests/tests_memory.cpp
 *
 *  \brief
 *
 *  \author  Olivia Quinet
 */

#include "../src/memory.hpp"
#include <catch2/catch_test_macros.hpp>
#include <iostream>

using namespace cppsas7bdat;
using cppsas7bdat::INTERNAL::MEMORY::PALIGNEDMEM;

PALIGNEDMEM alloc_and_init(const size_t _size) {
  PALIGNEDMEM p(INTERNAL::MEMORY::aligned_alloc(_size));
  std::cerr << "alloc_and_init(" << _size << ")=" << (void *)(p.get())
            << std::endl;
  std::memset(p.get(), 0, _size);
  return p;
}

SCENARIO("If the system can allocate memory, no exception is thrown",
         "[memory]") {
  CHECK_NOTHROW(alloc_and_init(1));
  CHECK_NOTHROW(alloc_and_init(1024));
  CHECK_NOTHROW(alloc_and_init(1024 * 1024));
}

SCENARIO("If the system cannot allocate memory, an exception is thrown",
         "[memory]") {
  CHECK_THROWS(alloc_and_init(std::numeric_limits<size_t>::max() / 2));
}
