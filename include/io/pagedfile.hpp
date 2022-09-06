/*
 * pagedfile.hpp 
 * Douglas Rumbaugh
 *
 * A generic interface for accessing a DirectFile object via page numbers.
 *
 */

#pragma once

#include <string>
#include <memory>
#include <cassert>
#include <vector>
#include <algorithm>

#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <fcntl.h>

#include "util/types.h"
#include "util/base.h"

namespace lsm {

class PagedFile;

class PagedFileIterator {
public:
    bool next();
    byte *get_item();
};

class PagedFile {
public:

    std::unique_ptr<PagedFile> create(const std::string fname, bool new_file);

    /*
     * Add new_page_count new pages to the file in bulk, and returns the
     * PageId of the first page in the new range. 
     *
     * If the allocation fails, returns INVALID_PID. Also returns INVALID_PID
     * if bulk allocation is not supported by the implementation. This can be
    iter1.
     * checked via the supports_allocation method.
     */
    PageNum allocate_pages(PageNum count=0);

    /*
     * Reads data from the specified page into a buffer pointed to by
     * buffer_ptr. It is necessary for buffer_ptr to be parm::SECTOR_SIZE
     * aligned, and also for it to be large enough to accommodate
     * parm::PAGE_SIZE bytes. If the read succeeds, returns 1. Otherwise
     * returns 0. The contents of the input buffer are undefined in the case of
     * an error.
     */
    int read_page(PageNum pnum, byte *buffer_ptr);

    /*
     * Reads several pages into associated buffers. It is necessary for the
     * buffer referred to by each pointer to be parm::SECTOR_SIZE aligned and
     * large enough to accommodate parm::PAGE_SIZE bytes. If possible,
     * vectorized IO may be used to read adjacent pages. If the reads succeed,
     * returns 1. If a read fails, returns 0. The contents of all the buffers
     * are undefined in the case of an error.
     */
    int read_pages(std::vector<std::pair<PageNum, byte*>> pages);

    /*
     * Reads several pages stored contiguously into a single buffer. It is 
     * necessary that buffer_ptr be SECTOR_SIZE aligned and also
     * at least page_cnt * PAGE_SIZE bytes large.
     */
    int read_pages(PageNum first_page, size_t page_cnt, byte *buffer_ptr);

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
    int write_page(PageNum pnum, const byte *buffer_ptr);

    /*
     * Writes multiple pages stored sequentially in the provided buffer into
     * a contiguous region of the file, starting at first_page. If the write
     * would overrun the allocated space in the file, no data is written.
     *
     * It is necessary for buffer_ptr to be SECTOR_SIZE aligned, and at 
     * least PAGE_SIZE * page_cnt bytes large.
     *
     * Returns the number of complete pages successfully written.
     */
    int write_pages(PageNum first_page, size_t page_cnt, const byte *buffer_ptr);

    /*
     * Returns the number of allocated paged in the file.
     */
    PageNum get_page_count();

    /*
     * Delete this file from the underlying filesystem. Once this has been called,
     * this object will be closed, and all operations other than destructing it are
     * undefined. Returns 1 on successful removal of the file, and 0 on failure.
     */
    int remove_file();

    PagedFileIterator *start_scan(PageNum start_page, PageNum end_page);

    std::string get_fname();

    ~PagedFile();

private:
    PagedFile(int fd, std::string fname, off_t size, mode_t mode);
    static off_t pnum_to_offset(PageNum pnum);
    bool check_pnum(PageNum pnum);

    int raw_read(byte *buffer, off_t amount, off_t offset);
    int raw_readv(std::vector<byte *> buffers, off_t buffer_size, off_t initial_offset);
    int raw_write(const byte *buffer, off_t amount, off_t offset);
    int raw_allocate(size_t amount);

    bool verify_io_parms(off_t amount, off_t offset); 

    int fd;
    bool file_open;
    off_t size;
    mode_t mode;
    std::string fname;
    int flags;
};

}
