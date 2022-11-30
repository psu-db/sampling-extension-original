/*
 * pagedfile.hpp 
 * Douglas Rumbaugh
 *
 * A generic interface for accessing a DirectFile object via page numbers.
 *
 */

#pragma once
#define __GNU_SOURCE

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

#define PF_COUNT_IO

#ifdef PF_COUNT_IO
    #define INC_READ() lsm::pf_read_cnt++
    #define INC_WRITE() lsm::pf_write_cnt++
    #define RESET_IO_CNT() \
        lsm::pf_read_cnt = 0; \
        lsm::pf_write_cnt = 0
#else
    #define INC_READ() do {} while (0)
    #define INC_WRITE() do {} while (0)
    #define RESET_IO_CNT() do {} while (0)
#endif

namespace lsm {

extern thread_local size_t pf_read_cnt;
extern thread_local size_t pf_write_cnt;

class PagedFileIterator;


class PagedFile {
public:
    static PagedFile *create(const std::string fname, bool new_file=true);

    /*
     * Add new_page_count new pages to the file in bulk, and returns the
     * PageId of the first page in the new range. 
     *
     * If the allocation fails, returns INVALID_PID. Also returns INVALID_PID
     * if bulk allocation is not supported by the implementation. This can be
    iter1.
     * checked via the supports_allocation method.
     */
    PageNum allocate_pages(PageNum count=1);

    /*
     * Reads data from the specified page into a buffer pointed to by
     * buffer_ptr. It is necessary for buffer_ptr to be parm::SECTOR_SIZE
     * aligned, and also for it to be large enough to accommodate
     * parm::PAGE_SIZE chars. If the read succeeds, returns 1. Otherwise
     * returns 0. The contents of the input buffer are undefined in the case of
     * an error.
     */
    int read_page(PageNum pnum, char *buffer_ptr);

    /*
     * Reads several pages into associated buffers. It is necessary for the
     * buffer referred to by each pointer to be parm::SECTOR_SIZE aligned and
     * large enough to accommodate parm::PAGE_SIZE chars. If possible,
     * vectorized IO may be used to read adjacent pages. If the reads succeed,
     * returns 1. If a read fails, returns 0. The contents of all the buffers
     * are undefined in the case of an error.
     */
    int read_pages(std::vector<std::pair<PageNum, char*>> pages);

    /*
     * Reads several pages stored contiguously into a single buffer. It is 
     * necessary that buffer_ptr be SECTOR_SIZE aligned and also
     * at least page_cnt * PAGE_SIZE chars large.
     */
    int read_pages(PageNum first_page, size_t page_cnt, char *buffer_ptr);

    /*
     * Writes data from the provided buffer into the specified page within the
     * file. It is necessary for buffer_ptr to be parm::SECTOR_SIZE aligned,
     * and also for it to be at least parm::PAGE_SIZE chars large. If it is
     * larger, only the first parm::PAGE_SIZE chars will be written. If it is
     * smaller, the result is undefined.
     *
     * If the write succeeds, returns 1. Otherwise returns 0. The contents of
     * the specified page within the file are undefined in the case of an error.
     */
    int write_page(PageNum pnum, const char *buffer_ptr);

    /*
     * Writes multiple pages stored sequentially in the provided buffer into
     * a contiguous region of the file, starting at first_page. If the write
     * would overrun the allocated space in the file, no data is written.
     *
     * It is necessary for buffer_ptr to be SECTOR_SIZE aligned, and at 
     * least PAGE_SIZE * page_cnt chars large.
     *
     * Returns the number of complete pages successfully written.
     */
    int write_pages(PageNum first_page, size_t page_cnt, const char *buffer_ptr);

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


    /*
     * Returns the raw number of bytes allocated in the backing file.
     */
    off_t get_file_size() const;

    PagedFileIterator *start_scan(PageNum start_page=1, PageNum end_page=0);

    std::string get_fname();

    void rename_file(std::string fname);

    ~PagedFile();

private:
    PagedFile(int fd, std::string fname, off_t size, mode_t mode);
    static off_t pnum_to_offset(PageNum pnum);
    bool check_pnum(PageNum pnum) const;

    int raw_read(char *buffer, off_t amount, off_t offset);
    int raw_readv(std::vector<char *> buffers, off_t buffer_size, off_t initial_offset);
    int raw_write(const char *buffer, off_t amount, off_t offset);
    int raw_allocate(size_t amount);

    bool verify_io_parms(off_t amount, off_t offset); 

    int fd;
    bool file_open;
    off_t size;
    mode_t mode;
    std::string fname;
    int flags;
};


class PagedFileIterator {
public:
  PagedFileIterator(PagedFile *pfile, PageNum start_page = 0,
                    PageNum stop_page = 0)
      : pfile(pfile),
        current_pnum((start_page == INVALID_PNUM) ? 0 : start_page - 1),
        start_pnum(start_page), stop_pnum(stop_page),
        buffer((char *)aligned_alloc(SECTOR_SIZE, PAGE_SIZE)) {}

  bool next() {
    while (this->current_pnum < this->stop_pnum) {
      if (this->pfile->read_page(++this->current_pnum, this->buffer)) {
        return true;
      }

      // IO error of some kind
      return false;
    }

    // no more pages to read
    return false;
    }

    char *get_item() {
        return this->buffer;
    }

    ~PagedFileIterator() {
        free(this->buffer);
    }

private:
    PagedFile *pfile;
    PageNum current_pnum;
    PageNum start_pnum;
    PageNum stop_pnum;

    char *buffer;
};
}
