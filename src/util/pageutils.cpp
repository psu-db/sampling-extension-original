/*
 *
 */

#include "util/pageutils.hpp"

namespace lsm { namespace io {

io::Page *wrap_page_raw(byte *page_ptr) 
{
    auto header = (PageHeaderData *) page_ptr;

    if (header->fixed_record_length) {
        return new FixedlenDataPage(page_ptr);
    }

    // TODO: varlen data page, if necessary
    return nullptr;
}

std::unique_ptr<io::Page> wrap_page(byte *page_ptr) 
{
    return std::unique_ptr<Page>(wrap_page_raw(page_ptr));
}


}}
