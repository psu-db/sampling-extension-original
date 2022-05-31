#ifndef BASE_H
#define BASE_H

#include <cstdlib>
#include <cstdint>
#include <cstddef>
#include <memory>
#include "util/types.hpp"

namespace lsm {

// Namespace for various numerical constants used throughout the
// codebase.
namespace parm {
// The correct quantity for use in alignment of buffers to be
// compatible with O_DIRECT
const size_t SECTOR_SIZE = 512;

// The standard sized block of data (in bytes) for use in IO 
// operations.
const size_t PAGE_SIZE = 4096;

// The size of a cacheline, for alignment purposes.
const size_t CACHELINE_SIZE = 64;

// The largest representable PageNum. A given file cannot
// have more pages than this.
const size_t MAX_PAGE_COUNT = UINT32_MAX;

// The largest representable FileId. The file manager cannot
// manage more files than this.
const size_t MAX_FILE_COUNT = UINT32_MAX;

// The largest representable FrameId. No buffer can be defined with
// more frames than this.
const size_t MAX_FRAME_COUNT = UINT32_MAX;
}

// The number of bytes of zeroes available in ZEROBUF. Will be
// a multiple of the parm::PAGE_SIZE.
constexpr size_t ZEROBUF_SIZE = 8 * parm::PAGE_SIZE;

// A large, preallocated, buffer of zeroes used for pre-allocation
// of pages in a file.
alignas(parm::SECTOR_SIZE) const char ZEROBUF[ZEROBUF_SIZE] = {0};


// alignment code taken from TacoDB (file: tdb_base.h)
template<class T>
constexpr T
TYPEALIGN(uint64_t ALIGNVAL, T LEN) {
    return (((uint64_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uint64_t) ((ALIGNVAL) - 1)));
}

#define SHORTALIGN(LEN)         TYPEALIGN(2, (LEN))
#define INTALIGN(LEN)           TYPEALIGN(4, (LEN))
#define LONGALIGN(LEN)          TYPEALIGN(8, (LEN))
#define DOUBLEALIGN(LEN)        TYPEALIGN(8, (LEN))
#define MAXALIGN(LEN)           TYPEALIGN(8, (LEN))
#define CACHELINEALIGN(LEN)     TYPEALIGN(parm::CACHELINE_SIZE, (LEN))
#define MAXALIGN_OF             8


namespace mem {

inline byte* page_alloc(PageNum page_cnt=1) {
    return (byte *) std::aligned_alloc(parm::SECTOR_SIZE, page_cnt*parm::PAGE_SIZE);
}

inline std::unique_ptr<byte> page_alloc_unique(PageNum page_cnt=1) {
    return std::unique_ptr<byte>(page_alloc(page_cnt));
}

}}

#endif
