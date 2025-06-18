/**
 *  \file src/stream.hpp
 *
 *  \brief Stream management
 *
 *  \author Olivia Quinet
 */

#ifndef _CPP_SAS7BDAT_SRC_STREAM_HPP_
#define _CPP_SAS7BDAT_SRC_STREAM_HPP_

#include <fstream>
#include <iostream>

namespace cppsas7bdat {
namespace INTERNAL {

std::ifstream open_stream(const char *_pcszFileName);

}
} // namespace cppsas7bdat

#endif
