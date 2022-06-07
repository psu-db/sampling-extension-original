/*
 *
 */

#ifndef H_PAGEUTILS
#define H_PAGEUTILS

#include "io/page.hpp"
#include "io/fixedlendatapage.hpp"

namespace lsm { namespace io {

/*
 * Returns a raw pointer to the appropriate type of page, automatically deduced
 * from the page header. Must be manually freed. The provided pointer must
 * refer to the beginning of a valid data page. If this is not the case, all
 * calls to the returned object are undefined.
 */
io::Page *wrap_page_raw(byte *page_ptr);

/*
 * Returns a unique_ptr to the appropriate type of page, automatically
 * deduced from the page header. The provided pointer must refer to the
 * beginning of a valid data page. If this is not the case, all calls to
 * the returned object are undefined.
 */ 
std::unique_ptr<io::Page> wrap_page(byte *page_ptr); 

}}
#endif
