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
#include <vector>

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
    PageNum virtual_header_page;
};

/*
 * The sorts of allocation call supported by a given PagedFile type.
 * Not all paged files support all allocation. 
 */
enum PageAllocSupport {
    // No allocation can be done--static file
    NONE,
    // Can allocate pages using allocate()
    SINGLE,

    // Can allocate pages using allocate and allocate_bulk
    BULK
};

constexpr size_t PagedFileHeaderSize = MAXALIGN(sizeof(PagedFileHeaderData));
static_assert(PagedFileHeaderSize <= parm::PAGE_SIZE);

class PagedFile {
public:
    /*
     * Create a new PagedFile object that has ownership of the provided DirectFile object. When
     * the PagedFile is destructed, the DirectFile will automatically be destructed as well.
     */
    PagedFile(std::unique_ptr<DirectFile> dfile, bool is_temp_file, bool free_supported, PageAllocSupport alloc_supported, bool virtualization_supported);

    /*
     * Create a new PagedFile object that does not own the underlying DirectFile. When the PagedFile is
     * destructed, the DirectFile will remain unaffected. Only use if the DirectFile is managed somewhere
     * else.
     */
    PagedFile(DirectFile *dfile, bool is_temp_file, bool free_supported, PageAllocSupport alloc_supported, bool virtualization_supported);

    /*
     * Default constructor. The created PagedFile object cannot be used for
     * anything until it is manually initialized. Mostly provided to allow
     * flexibility in sub-class implementations.
     */
    PagedFile() = default;

    /*
     * Add new_page to the file and return its associated PageId. Note that
     * PageIds are not necessarily monotonic, as freed pages will be recycled.
     *
     * If the allocation fails, returns INVALID_PID. Also returns INVALID_PID
     * if allocation is not supported by the implementation. This can be
     * checked via the supports_allocation method.
     */
    virtual PageId allocate_page() = 0;

    /*
     * Add new_page_count new pages to the file in bulk, and returns the
     * PageId of the first page in the new range. 
     *
     * If the allocation fails, returns INVALID_PID. Also returns INVALID_PID
     * if bulk allocation is not supported by the implementation. This can be
     * checked via the supports_allocation method.
     */
    virtual PageId allocate_page_bulk(PageNum new_page_count) = 0;

    /*
     * Returns the types of page allocation supported by this object.
     */
    virtual PageAllocSupport supports_allocation();

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
     * Reads several pages into associated buffers. It is necessary for the
     * buffer referred to by each pointer to be parm::SECTOR_SIZE aligned and
     * large enough to accommodate parm::PAGE_SIZE bytes. If possible,
     * vectorized IO may be used to read adjacent pages. If the reads succeed,
     * returns 1. If a read fails, returns 0. The contents of all the buffers
     * are undefined in the case of an error.
     */
    virtual int read_pages(std::vector<std::pair<PageId, byte*>> pages);

    /*
     * Same as read_pages(std::vector<std::pair<PageId, byte*>>) but accepts
     * pnums rather than pids.
     */
    virtual int read_pages(std::vector<std::pair<PageNum, byte*>> pages);

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
    virtual bool supports_free();

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
     * Returns the file ID of this PagedFile. In most implementations, this
     * will be obtained from the header of the file.
     */
    virtual FileId get_flid() = 0;

    /*
     * Returns a PagefileIterator opened to the specified page. The arguments
     * start_pid and stop_id define the range of pages within the file over
     * which the iterator will iterate. If INVALID_PID is passed as the start_pid,
     * the iterator will begin on the first page in sequence, and if INVALID_PID
     * is passed as the stop_pid, the iterator will stop on the last page of the 
     * file.
     *
     * stop_pid must come after start_pid in the sequence of pages within the
     * file. If this condition is violated, the iterator's stopping position is
     * undefined.
     */
    virtual std::unique_ptr<iter::GenericIterator<Page *>> start_scan(PageId start_pid=INVALID_PID, PageId stop_pid=INVALID_PID) = 0;

    /*
     * Same as start_scan(PageId), but accepts a PageNum as an argument instead.
     */
    virtual std::unique_ptr<iter::GenericIterator<Page *>> start_scan(PageNum start_pnum=INVALID_PNUM, PageNum stop_pnum=INVALID_PNUM) = 0;

    /*
     * Delete this file from the underlying filesystem. Once this has been called,
     * this object will be closed, and all operations other than destructing it are
     * undefined. Returns 1 on successful removal of the file, and 0 on failure.
     */
    virtual int remove_file();

    /*
     * Returns whether the file is initialized to support the creation of
     * virtual files within it. If this returns False, the file can be
     * initialized be calling its intialize_for_virtualization method, if
     * one is defined.
     */
    virtual bool virtual_header_initialized() = 0;

    /*
     * Returns whether the file object is capable of being a container for virtual files,
     * whether it is currently initialized as such or not. Not all PagedFile implementations
     * are required to support virtualization.
     */
    bool supports_virtualization();

    /*
     * If the PagedFile implementation supports being a virtual file container, initializes
     * the file for this purpose. This entails creating a virtual header page, and storing 
     * its PageNum in the virtual_header_page attribute of the paged file header. Note that
     * calling this method on a file that is not empty is undefined, and may overwrite data
     * stored in the file. Returns 1 if the initialization is successful, and 0 if it fails,
     * or if the operation is not supported. If initialization is not supported, the contents
     * of the file are guaranteed to not change. If initialization fails due to an error, the
     * contents of the file are undefined.
     */
    virtual int initialize_for_virtualization() = 0;

    /*
     * Only available as a default destructor. Any specific cleanup routines must
     * be defined by the subclass.
     */
    virtual ~PagedFile() = default;

protected:
    static int initialize_pagedfile(byte *header_page_buf, FileId flid);

    static off_t pnum_to_offset(PageNum pnum);
    bool check_pnum(PageNum pnum);

    std::unique_ptr<DirectFile> dfile;
    bool is_temp_file;
    bool free_supported;
    PageAllocSupport alloc_supported;
    bool virtualizable;
    DirectFile *dfile_ptr;
};

}}
#endif
