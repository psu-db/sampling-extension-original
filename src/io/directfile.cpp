/*
*
*/

#include "io/directfile.hpp"

namespace lsm {
namespace io {

std::unique_ptr<DirectFile> DirectFile::create(const std::string fname, bool new_file)
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
        return std::make_unique<DirectFile>(fd, fname, size, mode);
    }

    return nullptr;
}


DirectFile::DirectFile(int fd, std::string fname, off_t size, mode_t mode)
{
    this->file_open = true;
    this->fd = fd;
    this->fname = fname;
    this->size = size;
    this->mode = mode;
    this->flags = O_RDWR | O_DIRECT;
}


DirectFile::~DirectFile()
{
    this->close_file();
}


int DirectFile::read(void *buffer, off_t amount, off_t offset)
{
    if (!this->verify_io_parms(amount, offset)) {
        return 0;
    }

    if (pread(this->fd, buffer, amount, offset) != amount) {
        return 0;
    }

    return 1;
}


int DirectFile::write(const void *buffer, off_t amount, off_t offset)
{
    if (!this->verify_io_parms(amount, offset)) {
        return 0;
    }

    if (pwrite(this->fd, buffer, amount, offset) != amount) {
        return 0;
    }

    return 1;
}



int DirectFile::allocate(size_t amount)
{
    if (!this->file_open || (amount % parm::SECTOR_SIZE != 0)) {
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


bool DirectFile::verify_io_parms(off_t amount, off_t offset) 
{
    if (!this->file_open || amount + offset > this->size) {
        return false;
    }

    if (amount % parm::SECTOR_SIZE != 0) {
        return false;
    }

    if (offset % parm::SECTOR_SIZE != 0) {
        return false;
    }

    return true;
}


int DirectFile::close_file()
{
    if (this->file_open) {
        if (close(this->fd)) {
            return 0;
        }
        this->file_open = false;
        return 1;
    }

    return 2;
}


int DirectFile::reopen()
{
    if (!this->file_open) {
        auto fd = open(this->fname.c_str(), this->flags, this->mode);
        if (fd == -1) {
            return 0;
        }

        this->fd = fd;
        this->file_open = true;
        return 1;
    }

    return 2;
}


int DirectFile::remove()
{
    if (unlink(this->fname.c_str())) {
        return 0;
    }

    return 1;
}


bool DirectFile::is_open()
{
    return this->file_open;
}


off_t DirectFile::get_size()
{
    return this->size;
}

}
}
