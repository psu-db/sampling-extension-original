/*
 * pagedfile.hpp 
 * Douglas Rumbaugh
 *
 * A generic interface for accessing a DirectFile object via page numbers.
 *
 */

#ifndef PAGEDFILE_H
#define PAGEDFILE_H
#include <string>
#include <memory>
#include <cassert>

#include "util/types.hpp"
#include "util/base.hpp"
#include "util/iterator.hpp"
#include "io/directfile.hpp"
#include "io/page.hpp"

namespace lsm { namespace io {

// The header stored in Page 0 of the file, not file-level
// header information stored on each page.
struct PagedFileHeaderData {
    DirectFileHeaderData file_header;
    FileId flid;
    PageNum page_count;
};

constexpr size_t PagedFileHeaderSize = MAXALIGN(sizeof(PagedFileHeaderData));
static_assert(PagedFileHeaderSize <= parm::PAGE_SIZE);

class PagedFile {
public:
    PagedFile(std::unique_ptr<DirectFile> dfile, bool is_temp_file);

    /*
     * Add new_page to the file and return its associated PageId. Note that
     * PageIds are not necessarily monotonic, as freed pages will be recycled.
     *
     * If the allocation fails due to an IO error, or for some other reason,
     * returns INVALID_PID.
     */
    virtual PageId allocate_page() = 0;

    /*
     * Add new_page_count new pages to the file in bulk, and returns the
     * PageId of the first page in the new range. 
     *
     * If the allocation fails, returns INVALID_PID.
     */
    virtual PageId allocate_page_bulk(PageNum new_page_count) = 0;

    /*
     * Reads data from the specified page into a buffer pointed to by
     * buffer_ptr. It is necessary for buffer_ptr to be parm::SECTOR_SIZE
     * aligned, and also for it to be large enough to accommodate
     * parm::PAGE_SIZE bytes. If the read succeeds, returns 1. Otherwise
     * returns 0. The contents of the input buffer are undefined in the case of
     * an error.
     */
    virtual int read_page(PageId pid, byte *buffer_ptr);

    /*
     * Same as read_page(PageId, byte*), but accepts a PageNum rather than a
     * PageId.
     */
    virtual int read_page(PageNum pnum, byte *buffer_ptr);

    /*
     * Writes data from the provided buffer into the specified page within the
     * file. It is necessary for buffer_ptr to be parm::SECTOR_SIZE aligned,
     * and also for it to be at least parm::PAGE_SIZE bytes large. If it is
     * larger, only the first parm::PAGE_SIZE bytes will be written. If it is
     * smaller, the result is undefined.
     *
     * If the write succeeds, returns 1. Otherwise returns 0. The contents of
     * the specified page within the file are undefined in the case of an error.
     */
    virtual int write_page(PageId pid, const byte *buffer_ptr);

    /*
     * Same as write_page(PageId, const byte *), but accepts a PageNum rather
     * than a PageId.
     */
    virtual int write_page(PageNum pnum, const byte *buffer_ptr);

    /*
     * Delete a specified page and add it to the free list. The page can
     * still be accessed directly by PageId/PageNum, however it will no
     * longer appear in iterators over the page. Freed pages can be recycled
     * by allocate. Returns 1 on success and 0 on failure.
     *
     * It is not required for all PagedFile implementations to support deleting
     * pages. In this case, the implementation should return 0 and do nothing
     * else.
     */
    virtual int free_page(PageId pid) = 0;

    /*
     * Same as free_page(PageId), but accepts a PageNum rather than a PageId.
     */
    virtual int free_page(PageNum pnum) = 0;

    /*
     * Returns true if the class supports deleting pages via free_page, and
     * false if not.
     */
    virtual bool supports_free() = 0;

    /*
     * Converts a given PageNum into a PageId that is associated with this
     * file.
     */
    virtual PageId pnum_to_pid(PageNum pnum) = 0;

    /*
     * Returns true if this file is temporary, and false if it is not.
     */
    virtual bool is_temporary();

    /*
     * If this file is temporary, make it permanent (i.e., it will no longer
     * be automatically deleted on close). If this file is already permanent, 
     * this function does nothing.
     */
    virtual void make_permanent();

    /*
     * Returns the number of allocated paged in the file.
     */
    virtual PageNum get_page_count() = 0;

    /*
     * Returns the ID of the first page within the file. If there are no
     * allocated pages, returns INVALID_PID.
     */
    virtual PageId get_first_pid() = 0;

    /*
     * Returns the ID of the last page within the file. If there are no
     * allocated pages, returns INVALID_PID. If there is only one page
     * allocated, then get_first_pid() == get_last_pid().
     */
    virtual PageId get_last_pid() = 0;

    /*
     * Returns a PagefileIterator opened to the specified page. If INVALID_PID
     * is provided as an argument, then the iterator will be open to the first
     * page. If the provided page does not exist, or if the file has no pages,
     * then returns nullptr. If the specified page exists on the free list,
     * then all operations on the returned iterator are undefined.
     */
    virtual std::unique_ptr<iter::GenericIterator<Page *>> start_scan(PageId pid=INVALID_PID) = 0;

    /*
     * Same as start_scan(PageId), but accepts a PageNum as an argument instead.
     */
    virtual std::unique_ptr<iter::GenericIterator<Page *>> start_scan(PageNum pnum=INVALID_PNUM) = 0;

    /*
     * Delete this file from the underlying filesystem. Once this has been called,
     * this object will be closed, and all operations other than destructing it are
     * undefined. Returns 1 on successful removal of the file, and 0 on failure.
     */
    virtual int remove_file();

    /*
     * Close this file. Returns 1 on success and 0 on failure.
     */
    virtual int close_file() = 0;


    /*
     * Open the file if it is currently closed. Returns 1 on success and 0 on failure.
     */
    virtual int reopen_file() = 0;

    /*
     *  
     */
    virtual ~PagedFile() = default;

protected:
    static int initialize_pagedfile(byte *header_page_buf, FileId flid);

    static off_t pnum_to_offset(PageNum pnum);

    bool check_pnum(PageNum pnum);

    std::unique_ptr<DirectFile> dfile;
    bool is_temp_file;
};

}}
#endif
