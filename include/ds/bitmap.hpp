/*
*
*/

#ifndef H_BITMAP
#define H_BITMAP

#include <cstdlib>
#include <cstddef>
#include <cstring>
#include <memory>

#include "io/pagedfile.hpp"
#include "util/global.hpp"
#include "util/pageutils.hpp"
#include "util/mem.hpp"

namespace lsm { namespace ds {
struct bit_masking_data {
    std::byte *check_byte;
    std::byte bit_mask;
};

struct BitMapMetaHeader {
    PageNum first_page;
    PageNum last_page;
    size_t physical_size;
    size_t logical_size;
};

class BitMap {
public:
    /*
     * Create a new persistent BitMap object within the file specified by the
     * flid component of meta_pid, with its metadata stored at meta_pid. The
     * bitmap will be stored in newly allocated pages, and thus the only
     * existing page within the file that will be altered is the one referenced
     * by meta_pid.
     *
     * All bits within the allocated pages used by the bitmap will be
     * initialized to default_value. Any additional bits within the pages
     * not used by the bitmap will have undefined values.
     */
    static std::unique_ptr<BitMap> create(size_t size, PageId meta_pid, global::g_state *state, bool default_value=0);


    /* 
     * Create a new persistent BitMap object within pfile with its metadata
     * stored at the pnum provided by meta_pnum. The bitmap will be stored in
     * newly allocated pages, and thus the only existing page within the file
     * that will be altered is the one referenced by meta_pnum.
     *
     * All bits within the allocated pages used by the bitmap will be
     * initialized to default_value. Any additional bits within the pages
     * not used by the bitmap will have undefined values.
     */
    static std::unique_ptr<BitMap> create(size_t size, PageNum meta_pnum, io::PagedFile *pfile, bool default_value=0);


    /*
     * Open an existing persistent BitMap object stored within the file
     * specified by the flid component of meta_pid, with its metadata stored at
     * meta_pid. The page referenced by meta_pid must be a valid BitMap metadata
     * page (initialized by a prior call to BitMap::create), or the object returned
     * by this function is undefined. This call will not alter the referenced file
     * in any way.
     */
    static std::unique_ptr<BitMap> open(PageId meta_pid, global::g_state *state);

    /*
     * Open an existing persistent BitMap object stored within pfile with its 
     * metadata stored at meta_pnum. The page referenced by meta_pnum must be a valid BitMap
     * metadata page (initialized by a prior call to BitMap::create)
     */
    static std::unique_ptr<BitMap> open(PageNum meta_pnum, io::PagedFile *pfile);

    /*
     * Set the specified bit within the bitmap to 1. If the bit is already set,
     * do nothing. Returns 1 on success. If the specified bit is larger than the
     * size of the bitmap, do nothing and return 0.
     */
    int set(size_t bit);

    /*
     * Set the specified bit within the bitmap to 0. If the bit is already 0,
     * do nothing. Returns 1 on success. If the specified bit is larger than
     * the size of the bitmap, do nothing and return 0.
     */
    int unset(size_t bit);

    /*
     * Check the value of the specified bit and return True if it is 1 and
     * False if it is 0. If the specified bit is larger than the size of the
     * bitmap, return False.
     */
    bool is_set(size_t bit);

    /*
     * Set all bits within the bitmap to 0.
     */
    void unset_all();

    /*
     * Write the buffered contents of the bitmap back to the underlying file.
     */
    void flush();
private:
    std::unique_ptr<byte[]> bits; // the backing array storing the bits
    size_t logical_size; // the requested number of bits
    size_t physical_size; // the amount of space allocated
    
    PageNum first_pnum; // the first page within the file containing bitmap data
    PageNum last_pnum; // the final page within the file containing bitmap data
    io::PagedFile *pfile; // the file containing the bitmap

    BitMap(PageNum meta_pnum, io::PagedFile *pfile);

    bit_masking_data calculate_mask(size_t bit);
};
}}
#endif
