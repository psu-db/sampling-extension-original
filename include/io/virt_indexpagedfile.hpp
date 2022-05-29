/*
 * virt_indexpagedfile.hpp 
 * Douglas Rumbaugh
 *
 */

#ifndef V_INDEXPAGEDFILE_H
#define V_INDEXPAGEDFILE_H
#include <string>
#include <memory>
#include <cassert>

#include "util/types.hpp"
#include "util/base.hpp"
#include "util/iterator.hpp"
#include "io/directfile.hpp"
#include "io/page.hpp"

#include "io/pagedfile.hpp"
#include "io/indexpagedfile.hpp"
#include "io/linkpagedfile.hpp"
#include "catalog/schema.hpp"
#include "io/record.hpp"

namespace lsm { namespace io {

const catalog::FixedKVSchema VirtualHeaderRecordSchema(sizeof(FileId), sizeof(PageNum), RecordHeaderLength);

class VirtualPagedFile : public PagedFile {
public:
    // TODO: Move these two to the file manager
    static std::unique_ptr<VirtualPagedFile> create_static_indexed(IndexPagedFile *pfile, PageNum page_count);

    /*
     * Create a new IndexPagedFile object from a DirectFile. Note that the
     * Directfile in question must have been passed through
     * IndexPagedFile::initialize at some point in its existence, otherwise all
     * operations upon the resulting IndexPagedFile object are undefined. If
     * is_temp_file is set to true, then the file will be considered temporary,
     * and automatically deleted from the filesystem when it is closed, or when
     * the IndexPagedFile object is destructed.
     */
    VirtualPagedFile(std::shared_ptr<PagedFile> pfile, bool is_temp_file);

    /*
     * Add a new page to the file and return its associated PageId. Note that
     * PageIds are not necessarily monotonic, as freed pages will be recycled.
     *
     * If the allocation fails due to an IO error, or for some other reason,
     * returns INVALID_PID.
     */
    PageId allocate_page() override;

    PageId allocate_page_bulk(PageNum new_page_count) override;

    /*
     * Delete a specified page and add it to the free list. The page can
     * still be accessed directly by PageId/PageNum, however it will no
     * longer appear in iterators over the page. Freed pages can be recycled
     * by allocate. Returns 1 on success and 0 on failure.
     */
    int free_page(PageId pid) override;

    /*
     * Same as free_page(PageId), but accepts a PageNum rather than a PageId.
     */
    int free_page(PageNum pnum) override;

    /*
     * Converts a given PageNum into a PageId that is associated with this
     * file.
     */
    PageId pnum_to_pid(PageNum pnum) override;

    /*
     * Returns the number of allocated paged in the file.
     */
    PageNum get_page_count() override;

    /*
     * Returns the ID of the first page within the file. If there are no
     * allocated pages, returns INVALID_PID.
     */
    PageId get_first_pid() override;

    /*
     * Returns the ID of the last page within the file. If there are no
     * allocated pages, returns INVALID_PID. If there is only one page
     * allocated, then get_first_pid() == get_last_pid().
     */
    PageId get_last_pid() override;

    /*
     * Returns a PagefileIterator opened to the specified page. If INVALID_PID
     * is provided as an argument, then the iterator will be open to the first
     * page. If the provided page does not exist, or if the file has no pages,
     * then returns nullptr. If the specified page exists on the free list,
     * then all operations on the returned iterator are undefined.
     */
    std::unique_ptr<iter::GenericIterator<Page *>> start_scan(PageId pid=INVALID_PID) override;

    /*
     * Same as start_scan(PageId), but accepts a PageNum as an argument instead.
     */
    virtual std::unique_ptr<iter::GenericIterator<Page *>> start_scan(PageNum pnum=INVALID_PNUM) override;

    /*
     *  
     */
    ~VirtualPagedFile() override;

private:
    static const PageNum header_page_pnum = INVALID_PNUM;

    void flush_metadata();

    #ifdef NO_BUFFER_MANAGER
    void flush_buffer(PageNum pnum);
    std::unique_ptr<byte> buffer;
    #endif
};

}}
#endif
