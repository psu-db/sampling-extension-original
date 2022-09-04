/*
 * types.hpp 
 * Douglas Rumbaugh
 *
 * A centralized header file for various datatypes used throughout the
 * code base. There are a few very specific types, such as header formats,
 * that are defined within the header files that make direct use of them,
 * but all generally usable, simple types are defined here.
 */
#ifndef TYPES_H
#define TYPES_H

#include <cstdlib>
#include <cstdint>
#include <cstddef>
#include <string>

#include "util/base.h"


namespace lsm {

using std::byte;

// Represents a page offset within a specific file (physical or virtual)
typedef uint32_t PageNum;

// Byte offset within a page. Also used for lengths of records, etc.,
// within the codebase. size_t isn't necessary, as the maximum offset
// is only parm::PAGE_SIZE 
typedef uint16_t PageOffset;

// A unique identifier for a frame within a buffer or cache.
typedef int32_t FrameId;

// A unique timestamp for use in MVCC concurrency control. Currently stored in
// record headers, but not used by anything.
typedef uint32_t Timestamp;
const Timestamp TIMESTAMP_MIN = 0;
const Timestamp TIMESTAMP_MAX = UINT32_MAX;

// Invalid values for various IDs. Used throughout the code base to indicate
// uninitialized values and error conditions.
const PageNum INVALID_PNUM = 0;
const FrameId INVALID_FRID = -1;

}
#endif
