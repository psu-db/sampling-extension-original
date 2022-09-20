#pragma once
#define __GNU_SOURCE

#include <string>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cassert>

#include "util/base.h"
#include "util/types.h"


namespace lsm {

// Thin wrapper for a FS File, no page deletion/free list is supported.
// First page (page 0) is reserved for meta information.

class File {
public:
    static File* create(const std::string& fname, bool new_file = true) {
        auto flags = O_RDWR | O_DIRECT;
        mode_t mode = 0640;
        off_t size = 0;

        if (new_file) flags |= O_CREAT | O_TRUNC;

        int fd = open(fname.c_str(), flags, mode);
        if (fd == -1) return nullptr;
    
        if (new_file) {
            // Allocating the meta page.
            if(fallocate(fd, 0, 0, PAGE_SIZE)) return nullptr;
            size = 1;
        } else {
            struct stat buf;
            if (fstat(fd, &buf) == -1) return nullptr;

            assert(buf.st_size % PAGE_SIZE == 0);
            size = buf.st_size / PAGE_SIZE;
        } 

        if (fd) return new File(fd, fname, size);
        return nullptr;
    }

    PageNum allocate_pages(PageNum cnt = 1) {
        assert(m_fd != -1);
        PageNum new_first = m_size + 1;

        if (fallocate(m_fd, 0, pnum_to_offset(m_size), cnt * PAGE_SIZE))
            return INVALID_PNUM;
        
        m_size += cnt;
        return new_first;
    }

    int read_page(PageNum n, char* buf) {
        assert(n < m_size && n != 0);
        if (pread(m_fd, buf, PAGE_SIZE, pnum_to_offset(n)) != PAGE_SIZE)
            return 0;
        return 1;
    }

    int write_page(PageNum n, const char* buf) {
        assert(n < m_size && n != 0);
        if (pwrite(m_fd, buf, PAGE_SIZE, pnum_to_offset(n)) != PAGE_SIZE)
            return 0;
        return 1;
    }

    int remove_file() {
        close(m_fd);
        if (unlink(m_fname.c_str())) return 0;

        m_fd = -1;
        return 1;
    }

    static off_t pnum_to_offset(PageNum n) {
        return n * PAGE_SIZE;
    }

    PageNum get_page_cnt() { return m_size; }

    ~File() {
        if (m_fd != -1) close(m_fd);
    }

private:
    File(int fd, const std::string& fname, off_t size)
    : m_fd(fd), m_size(size), m_fname(fname) {}

    int m_fd;
    off_t m_size;
    std::string m_fname;
};

}