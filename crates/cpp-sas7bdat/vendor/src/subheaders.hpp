/**
 *  \file src/subheaders.hpp
 *
 *  \brief Definition of subheaders
 *
 *  \author Olivia Quinet
 */

#ifndef _CPP_SAS7BDAT_SRC_SUBHEADERS_HPP_
#define _CPP_SAS7BDAT_SRC_SUBHEADERS_HPP_

#include <cppsas7bdat/properties.hpp>
#include <vector>

namespace cppsas7bdat {
namespace INTERNAL {

struct PAGE_SUBHEADER {
  size_t offset{0};
  size_t length{0};
  uint8_t compression{0};
  uint8_t type{0};
};

constexpr const uint8_t TRUNCATED_SUBHEADER_ID{1};
constexpr const uint8_t COMPRESSED_SUBHEADER_ID{4};
constexpr const uint8_t COMPRESSED_SUBHEADER_TYPE{1};

struct DataSubHeader {
  const size_t offset{0};
  const size_t length{0};

  DataSubHeader(const PAGE_SUBHEADER &_subheader)
      : offset(_subheader.offset), length(_subheader.length) {}

  // Is it a data subheader?
  static bool check(const Properties::Metadata *_metadata,
                    const PAGE_SUBHEADER &_subheader) noexcept {
    return ((_metadata->compression != Compression::none) &&
            (_subheader.compression == COMPRESSED_SUBHEADER_ID ||
             _subheader.compression == 0) &&
            (_subheader.type == COMPRESSED_SUBHEADER_TYPE));
  };
};

using DATASUBHEADERS = std::vector<DataSubHeader>;

} // namespace INTERNAL
} // namespace cppsas7bdat

#endif
