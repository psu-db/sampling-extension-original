/*
 *
 */

#ifndef H_MEM
#define H_MEM

#include <memory>

#include "util/types.hpp"

namespace lsm { namespace mem {

typedef std::unique_ptr<byte, decltype(&free)> aligned_buffer;

inline byte *page_alloc_raw(PageNum page_cnt=1) {
    return (byte *) std::aligned_alloc(parm::SECTOR_SIZE, page_cnt*parm::PAGE_SIZE);
}

inline aligned_buffer page_alloc(PageNum page_cnt=1) {
    return std::unique_ptr<byte, decltype(&free)>(page_alloc_raw(page_cnt), &free);
}


inline aligned_buffer wrap_aligned_buffer(byte * ptr) {
    return std::unique_ptr<byte, decltype(&free)> (ptr, &free);
}


inline aligned_buffer create_aligned_buffer(size_t size, size_t alignment=parm::SECTOR_SIZE) {
    return wrap_aligned_buffer((byte *) std::aligned_alloc(alignment, size));
}

}}

#endif
