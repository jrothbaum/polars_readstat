/**
 *  \file cppsas7bdat/source/ifstream.hpp
 *
 *  \brief std::ifstream datasource
 *
 *  \author Olivia Quinet
 */

#ifndef _CPP_SAS7BDAT_SOURCE_IFSTREAM_HPP_
#define _CPP_SAS7BDAT_SOURCE_IFSTREAM_HPP_

#include <fstream>

namespace cppsas7bdat {
namespace datasource {

struct ifstream {
  std::ifstream is;

  explicit ifstream(const char *_pcszfilename);

  bool eof() { return is.eof(); }
  bool read_bytes(void *_p, const size_t _length) {
    is.read(reinterpret_cast<char *>(_p),
            static_cast<std::streamsize>(_length));
    // Did we manage to read the requested data?
    return is.good();
  }
};

} // namespace datasource
} // namespace cppsas7bdat

#endif
