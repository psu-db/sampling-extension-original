/*
 * pagedfile.hpp 
 * Douglas Rumbaugh
 *
 * An interface for accessing a file as a linked list of pages, along with an
 * associated iterator. Note that this class buffers some important metadata in
 * memory, which is flushed to disk automatically when the file is closed via
 * this object, or when its destructor is called. Because of this, it is
 * important to access the associated file only through PagedFile's interface.
 * If a raw pointer to the underlying DirectFile is used to access the file
 * after a PagedFile has been created, any operations done through the
 * PagedFile's interface are undefined.
 *
 * The PagedFile object contains a one page internal buffer used for metadata
 * updates if the NO_BUFFER_MANAGER macro is defined. (TODO: Otherwise, it will
 * attempt to use a buffer manager associated with the global file manager).
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

//#define NO_BUFFER_MANAGER

namespace lsm {
namespace io {

// The header stored in Page 0 of the file, not file-level
// header information stored on each page.
struct PagedFileHeaderData {
    DirectFileHeaderData file_header;
    FileId flid;

    PageNum first_page;
    PageNum last_page;
    PageNum page_count;
    PageNum first_free_page;

};

constexpr size_t PagedFileHeaderSize = MAXALIGN(sizeof(PagedFileHeaderData));
static_assert(PagedFileHeaderSize <= parm::PAGE_SIZE);

class PagedFile;

class PageFileIterator : public iter::GenericIterator<Page *> {
friend PagedFile;

public:
    PageFileIterator(PagedFile *file, PageNum pnum);

    bool next() override;
    Page *get_item() override;

    bool supports_rewind() override;
    iter::IteratorPosition save_position() override;
    void rewind(iter::IteratorPosition position) override;

    void end_scan() override;

    ~PageFileIterator();
private:
    PagedFile *pfile;
    PageNum current_page;
    PageOffset current_offset;

    union iter_position {
        iter::IteratorPosition packed_position = 0;
        struct {
            PageNum pnum;
            PageOffset offset;
        };
    };
};

class PagedFile {
public:
    // TODO: Move these two to the file manager
    static std::unique_ptr<PagedFile> create(std::string fname, bool new_file, FileId flid);
    static std::unique_ptr<PagedFile> create_temporary(FileId flid);

    /*
     * Initialize an empty DirectFile for use with paged accesses.
     * Specifically, allocates a header page (page 0) containing file-level
     * information used by the PagedFile class. Should only be called on an
     * empty file, and must be called on a file prior to passing it into a
     * PagedFile constructor.
     */
    static int initialize(DirectFile *dfile, FileId flid);

    /*
     * Create a new PagedFile object from a DirectFile. Note that the
     * Directfile in question must have been passed through
     * PagedFile::initialize at some point in its existence, otherwise all
     * operations upon the resulting PagedFile object are undefined. If
     * is_temp_file is set to true, then the file will be considered temporary,
     * and automatically deleted from the filesystem when it is closed, or when
     * the PagedFile object is destructed.
     */
    PagedFile(std::unique_ptr<DirectFile> dfile, bool is_temp_file);

    /*
     * Add a new page to the file and return its associated PageId. Note that
     * PageIds are not necessarily monotonic, as freed pages will be recycled.
     *
     * If the allocation fails due to an IO error, or for some other reason,
     * returns INVALID_PID.
     */
    PageId allocate_page();

    /*
     * Reads data from the specified page into a buffer pointed to by
     * buffer_ptr. It is necessary for buffer_ptr to be parm::SECTOR_SIZE
     * aligned, and also for it to be large enough to accommodate
     * parm::PAGE_SIZE bytes. If the read succeeds, returns 1. Otherwise
     * returns 0. The contents of the input buffer are undefined in the case of
     * an error.
     */
    int read_page(PageId pid, byte *buffer_ptr);

    /*
     * Same as read_page(PageId, byte*), but accepts a PageNum rather than a
     * PageId.
     */
    int read_page(PageNum pnum, byte *buffer_ptr);

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
    int write_page(PageId pid, const byte *buffer_ptr);

    /*
     * Same as write_page(PageId, const byte *), but accepts a PageNum rather
     * than a PageId.
     */
    int write_page(PageNum pnum, const byte *buffer_ptr);

    /*
     * Delete a specified page and add it to the free list. The page can
     * still be accessed directly by PageId/PageNum, however it will no
     * longer appear in iterators over the page. Freed pages can be recycled
     * by allocate. Returns 1 on success and 0 on failure.
     */
    int free_page(PageId pid);

    /*
     * Same as free_page(PageId), but accepts a PageNum rather than a PageId.
     */
    int free_page(PageNum pnum);

    /*
     * Converts a given PageNum into a PageId that is associated with this
     * file.
     */
    PageId pnum_to_pid(PageNum pnum);

    /*
     * Returns true if this file is temporary, and false if it is not.
     */
    bool is_temporary();
    /*
     * If this file is temporary, make it permanent (i.e., it will no longer
     * be automatically deleted on close). If this file is already permanent, 
     * this function does nothing.
     */
    void make_permanent();

    /*
     * Returns the number of allocated paged in the file.
     */
    PageNum get_page_count();

    /*
     * Returns the ID of the first page within the file. If there are no
     * allocated pages, returns INVALID_PID.
     */
    PageId get_first_pid();

    /*
     * Returns the ID of the last page within the file. If there are no
     * allocated pages, returns INVALID_PID. If there is only one page
     * allocated, then get_first_pid() == get_last_pid().
     */
    PageId get_last_pid();

    /*
     * Returns a PagefileIterator opened to the specified page. If INVALID_PID
     * is provided as an argument, then the iterator will be open to the first
     * page. If the provided page does not exist, or if the file has no pages,
     * then returns nullptr. If the specified page exists on the free list,
     * then all operations on the returned iterator are undefined.
     */
    std::unique_ptr<PageFileIterator> start_scan(PageId pid=INVALID_PID);

    /*
     * Same as start_scan(PageId), but accepts a PageNum as an argument instead.
     */
    std::unique_ptr<PageFileIterator> start_scan(PageNum pnum=INVALID_PNUM);

    /*
     * Delete this file from the underlying filesystem. Once this has been called,
     * this object will be closed, and all operations other than destructing it are
     * undefined. Returns 1 on successful removal of the file, and 0 on failure.
     */
    int remove_file();

    /*
     * Close this file. Returns 1 on success and 0 on failure.
     */
    int close_file();

    /*
     *  
     */
    ~PagedFile();

private:
    // TODO: move to file manager
    static FileId next_flid();

    static inline off_t pnum_to_offset(PageNum pnum);
    static const PageNum header_page_pnum = INVALID_PNUM;


    void flush_metadata();
    bool check_pnum(PageNum pnum);

    std::unique_ptr<DirectFile> dfile;
    bool is_temp_file;
    PagedFileHeaderData header_data;

    #ifdef NO_BUFFER_MANAGER
    void flush_buffer(PageNum pnum);
    std::unique_ptr<byte> buffer;
    #endif
};

}
}

#endif
