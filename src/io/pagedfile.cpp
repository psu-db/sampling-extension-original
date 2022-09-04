/*
 * pagedfile.cpp
 * Douglas Rumbaugh
 *
 * PagedFile implementation
 */

#include "io/pagedfile.hpp"

namespace lsm { namespace io {

std::unique_ptr<PagedFile> PagedFile::create(const std::string fname, bool new_file)
{
    auto flags = O_RDWR | O_DIRECT;
    mode_t mode = 0640;
    off_t size = 0;

    if (new_file) {
        flags |= O_CREAT | O_TRUNC;
    } 

    int fd = open(fname.c_str(), flags, mode);
    if (fd == -1) {
        return nullptr;
    }
    
    if (!new_file) {
        struct stat buf;
        if (fstat(fd, &buf) == -1) {
            return nullptr;
        }

        size = buf.st_size;
    }

    if (fd) {
        return std::make_unique<PagedFile>(fd, fname, size, mode);
    }

    return nullptr;
}

PagedFile::PagedFile(int fd, std::string fname, off_t size, mode_t mode)
{
    this->file_open = true;
    this->fd = fd;
    this->fname = fname;
    this->size = size;
    this->mode = mode;
    this->flags = O_RDWR | O_DIRECT;
}


PageNum PagedFile::allocate_pages(PageNum count)
{
    PageNum new_first = this->get_page_count() + 1;
    size_t alloc_size = count * PAGE_SIZE;

    if (this->raw_allocate(alloc_size)) {
        return new_first;
    }

    return INVALID_PNUM;
}


int PagedFile::read_page(PageNum pnum, byte *buffer_ptr)
{
    if (this->check_pnum(pnum)) {
        return this->raw_read(buffer_ptr, PAGE_SIZE, PagedFile::pnum_to_offset(pnum));
    }

    return 0;
}


int PagedFile::read_pages(std::vector<std::pair<PageNum, byte*>> pages)
{
    if (pages.size() == 0) {
        return 0;
    }

    if (pages.size() == 1) {
        this->read_page(pages[0].first, pages[0].second);
    }

    std::sort(pages.begin(), pages.end());

    PageNum range_start = pages[0].first;
    PageNum prev_pnum = range_start;

    std::vector<byte *> buffers;
    buffers.push_back(pages[0].second);

    for (size_t i=1; i<pages.size(); i++) {
        if (pages[i].first == prev_pnum + 1) {
            buffers.push_back(pages[i].second);
            prev_pnum = pages[i].first;
        } else {
            if (!this->raw_readv(buffers, PAGE_SIZE, PagedFile::pnum_to_offset(range_start))) {
                return 0;
            }

            range_start = pages[i].first;
            prev_pnum = range_start;

            buffers.clear();
            buffers.push_back(pages[i].second);
        }
    }

    return this->raw_readv(buffers, PAGE_SIZE, PagedFile::pnum_to_offset(range_start));
}


int PagedFile::write_page(PageNum pnum, const byte *buffer_ptr)
{
    if (this->check_pnum(pnum)) {
        return this->raw_write(buffer_ptr, PAGE_SIZE, PagedFile::pnum_to_offset(pnum));
    }

    return 0;
}


int PagedFile::remove_file()
{
    if (unlink(this->fname.c_str())) {
        return 0;
    }

    this->file_open = false;

    return 1;
}


PageNum PagedFile::get_page_count()
{
    return this->size / PAGE_SIZE;
}


PagedFile::~PagedFile()
{
    if (this->file_open) {
        close(this->fd);
    }
}


bool PagedFile::check_pnum(PageNum pnum)
{
    return pnum != INVALID_PNUM && pnum <= (this->size / PAGE_SIZE);
}


off_t PagedFile::pnum_to_offset(PageNum pnum) 
{
    return pnum * PAGE_SIZE;
}


int PagedFile::raw_read(byte *buffer, off_t amount, off_t offset)
{
    if (!this->verify_io_parms(amount, offset)) {
        return 0;
    }

    if (pread(this->fd, buffer, amount, offset) != amount) {
        return 0;
    }

    return 1;
}


int PagedFile::raw_readv(std::vector<byte *> buffers, off_t buffer_size, off_t initial_offset)
{
    size_t buffer_cnt = buffers.size();

    off_t amount = buffer_size * buffer_cnt;
    if (!this->verify_io_parms(amount, initial_offset)) {
        return 0;
    }

    auto iov = new iovec[buffer_cnt];
    for (size_t i=0; i<buffer_cnt; i++) {
        iov[i].iov_base = buffers[i];
        iov[i].iov_len = buffer_size;
    }

    if (preadv(this->fd, iov, buffer_cnt, initial_offset) != amount) {
        return 0;
    }

    return 1;
}


int PagedFile::raw_write(const byte *buffer, off_t amount, off_t offset)
{
    if (!this->verify_io_parms(amount, offset)) {
        return 0;
    }

    if (pwrite(this->fd, buffer, amount, offset) != amount) {
        return 0;
    }

    return 1;
}


int PagedFile::raw_allocate(size_t amount)
{
    if (!this->file_open || (amount % SECTOR_SIZE != 0)) {
        return 0;
    }

    size_t iterations = amount / ZEROBUF_SIZE + 1;
    for (size_t i=0; i<iterations; i++) {
        size_t itr_amount = std::min(ZEROBUF_SIZE, amount);
        if (pwrite(this->fd, ZEROBUF, itr_amount, this->size) == -1) {
            return 0;
        }
        amount -= itr_amount;
        this->size += itr_amount;
    }

    return 1;
}

bool PagedFile::verify_io_parms(off_t amount, off_t offset) 
{
    if (!this->file_open || amount + offset > this->size) {
        return false;
    }

    if (amount % SECTOR_SIZE != 0) {
        return false;
    }

    if (offset % SECTOR_SIZE != 0) {
        return false;
    }

    return true;
}

}}
