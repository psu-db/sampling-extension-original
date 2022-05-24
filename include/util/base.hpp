#ifndef BASE_H
#define BASE_H

#include <cstdlib>
#include <cstdint>
#include <cstddef>

namespace lsm {

namespace parm {
    const size_t SECTOR_SIZE = 512;
    const size_t PAGE_SIZE = 4096;
    const size_t CACHELINE_SIZE = 64;
    const size_t MAX_PAGE_COUNT = UINT32_MAX;
    const size_t MAX_FILE_COUNT = UINT32_MAX;
    const size_t MAX_FRAME_COUNT = UINT32_MAX;
}

constexpr size_t ZEROBUF_SIZE = 8 * parm::PAGE_SIZE;
const char ZEROBUF[ZEROBUF_SIZE] = {0};


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

}

#endif
