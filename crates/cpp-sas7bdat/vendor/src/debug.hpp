/**
 *  \file src/debug.hpp
 *
 *  \brief Metadata reading
 *
 *  \author Olivia Quinet
 */

#ifndef _CPP_SAS7BDAT_SRC_DEBUG_HPP_
#define _CPP_SAS7BDAT_SRC_DEBUG_HPP_

#ifdef DEBUG
#define D(x) x
#else
#define D(x)
#endif

#endif
