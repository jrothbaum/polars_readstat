/**
 *  \file src/datasource_ifstream.cpp
 *
 *  \brief DataSource ifstream
 *
 *  \author Olivia Quinet
 */

#include "exceptions.hpp"
#include "stream.hpp"
#include <cppsas7bdat/source/ifstream.hpp>

namespace cppsas7bdat::datasource {

ifstream::ifstream(const char *_pcszfilename)
    : is(INTERNAL::open_stream(_pcszfilename)) {}
} // namespace cppsas7bdat::datasource
