/*
 * indexpagedfile.hpp 
 * Douglas Rumbaugh
 *
 * An interface for accessing a file as a linked list of pages, along with an
 * associated iterator. Note that this class buffers some important metadata in
 * memory, which is flushed to disk automatically when the file is closed via
 * this object, or when its destructor is called. Because of this, it is
 * important to access the associated file only through IndexPagedFile's interface.
 * If a raw pointer to the underlying DirectFile is used to access the file
 * after a IndexPagedFile has been created, any operations done through the
 * IndexPagedFile's interface are undefined.
 *
 * The IndexPagedFile object contains a one page internal buffer used for metadata
 * updates if the NO_BUFFER_MANAGER macro is defined. (TODO: Otherwise, it will
 * attempt to use a buffer manager associated with the global file manager).
 */

#ifndef INDEXPAGEDFILE_H
#define INDEXPAGEDFILE_H
#include <string>
#include <memory>
#include <cassert>

#include "util/types.hpp"
#include "util/base.hpp"
#include "util/iterator.hpp"
#include "io/directfile.hpp"
#include "io/fixedlendatapage.hpp"
#include "io/readcache.hpp"
#include "io/pagedfile.hpp"

namespace lsm { namespace io {

// The header stored in Page 0 of the file, not file-level
// header information stored on each page.
struct IndexPagedFileHeaderData {
    PagedFileHeaderData paged_header;
};

constexpr size_t IndexPagedFileHeaderSize = MAXALIGN(sizeof(IndexPagedFileHeaderData));
static_assert(IndexPagedFileHeaderSize <= parm::PAGE_SIZE);

class IndexPagedFile;

class IndexPagedFilePageIterator : public iter::GenericIterator<Page *> {
public:
    IndexPagedFilePageIterator(IndexPagedFile *file, PageNum pnum, ReadCache *cache, bool fixedlen=true);

    bool next() override;
    Page *get_item() override;

    bool supports_rewind() override;
    iter::IteratorPosition save_position() override;
    void rewind(iter::IteratorPosition position) override;

    void end_scan() override;
    ~IndexPagedFilePageIterator();
private:
    IndexPagedFile *pfile;
    PageNum current_pnum;
    ReadCache *cache;
    FrameId current_frame_id;
    byte *current_frame_ptr;
    bool fixedlen;

    std::unique_ptr<Page> current_page;
    std::unique_ptr<iter::GenericIterator<Record>> record_itr;

    bool at_end;
    union iter_position {
        iter::IteratorPosition packed_position = 0;
        struct {
            PageNum pnum;
            PageOffset offset;
        };
    };
};

class IndexPagedFileRecordIterator : public iter::GenericIterator<Record> {
friend IndexPagedFile;

public:
    IndexPagedFileRecordIterator(IndexPagedFile *file, PageNum pnum, ReadCache *cache);

    bool next() override;
    Record get_item() override;

    bool supports_rewind() override;
    iter::IteratorPosition save_position() override;
    void rewind(iter::IteratorPosition position) override;

    void end_scan() override;

    ~IndexPagedFileRecordIterator();
private:
    Record current_record;

    Page *current_page;
    std::unique_ptr<iter::GenericIterator<Page *>> page_itr;
    std::unique_ptr<iter::GenericIterator<Record>> record_itr;

    bool at_end;
    union iter_position {
        iter::IteratorPosition packed_position = 0;
        struct {
            PageNum pnum;
            PageOffset offset;
        };
    };
};


class IndexPagedFile : public PagedFile {
public:
    // TODO: Move these two to the file manager
    static std::unique_ptr<IndexPagedFile> create(std::string fname, bool new_file, FileId flid);
    static std::unique_ptr<IndexPagedFile> create_temporary(FileId flid);

    /*
     * Initialize an empty DirectFile for use with paged accesses.
     * Specifically, allocates a header page (page 0) containing file-level
     * information used by the IndexPagedFile class. Should only be called on an
     * empty file, and must be called on a file prior to passing it into a
     * IndexPagedFile constructor.
     */
    static int initialize(DirectFile *dfile, FileId flid);

    /*
     * Create a new IndexPagedFile object from a DirectFile. Note that the
     * Directfile in question must have been passed through
     * IndexPagedFile::initialize at some point in its existence, otherwise all
     * operations upon the resulting IndexPagedFile object are undefined. If
     * is_temp_file is set to true, then the file will be considered temporary,
     * and automatically deleted from the filesystem when it is closed, or when
     * the IndexPagedFile object is destructed.
     *
     * IndexPagedFiles created by this constructor own their associated
     * DirectFile and will automatically destruct it when they are destructed.
     */
    IndexPagedFile(std::unique_ptr<DirectFile> dfile, bool is_temp_file);

    /*
     * Create a new IndexPagedFile object from a DirectFile. Note that the
     * Directfile in question must have been passed through
     * IndexPagedFile::initialize at some point in its existence, otherwise all
     * operations upon the resulting IndexPagedFile object are undefined. If
     * is_temp_file is set to true, then the file will be considered temporary,
     * and automatically deleted from the filesystem when it is closed, or when
     * the IndexPagedFile object is destructed.
     *
     * IndexPagedFiles created by this constructor do not own their associated
     * DirectFile and will not automatically destruct it when they are
     * destructed. Ensure that the DirectFile objects are properly managed
     * somewhere else.
     */
    IndexPagedFile(DirectFile *dfile, bool is_temp_file);

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
     * Returns the FileId of this file, as stored in the page header
     */
    FileId get_flid() override;

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
     * See page.hpp
     */
    bool virtual_header_initialized() override;

    /*
     * See page.hpp
     */
    int initialize_for_virtualization() override;

    /*
     *  
     */
    ~IndexPagedFile() override;

private:
    static const PageNum header_page_pnum = INVALID_PNUM;

    void flush_metadata();
    IndexPagedFileHeaderData header_data;

    #ifdef NO_BUFFER_MANAGER
    void flush_buffer(PageNum pnum);
    std::unique_ptr<byte> buffer;
    #endif
};

}}
#endif