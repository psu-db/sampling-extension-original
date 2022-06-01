/*
 *
 */

#ifndef H_MEM
#define H_MEM

#include <memory>

#include "util/types.hpp"

namespace lsm { namespace mem {

inline byte* page_alloc(PageNum page_cnt=1) {
    return (std::byte *) std::aligned_alloc(parm::SECTOR_SIZE, page_cnt*parm::PAGE_SIZE);
}

inline std::unique_ptr<std::byte> page_alloc_unique(PageNum page_cnt=1) {
    return std::unique_ptr<std::byte>(page_alloc(page_cnt));
}

}}

#endif
