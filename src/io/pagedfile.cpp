/*
 * pagedfile.cpp
 * DouglasRumbaugh
 *
 * PagedFile ABC partial implementation
 */

#include "io/pagedfile.hpp"

namespace lsm { namespace io {

PagedFile::PagedFile(std::unique_ptr<DirectFile> dfile, bool is_temp_file, bool free_supported, PageAllocSupport alloc_supported,
                     bool virtualization_supported)
: dfile(std::move(dfile)), is_temp_file(is_temp_file),
    free_supported(free_supported), alloc_supported(alloc_supported),
    virtualizable(virtualization_supported), dfile_ptr(nullptr) {}


PagedFile::PagedFile(DirectFile *dfile, bool is_temp_file, bool free_supported, PageAllocSupport alloc_supported, bool virtualization_supported)
: dfile(nullptr), is_temp_file(is_temp_file), free_supported(free_supported),
    alloc_supported(alloc_supported),
    virtualizable(virtualization_supported), dfile_ptr(dfile) {}


int PagedFile::read_page(PageId pid, byte *buffer_ptr)
{
    return this->read_page(pid.page_number, buffer_ptr);
}


int PagedFile::read_page(PageNum pnum, byte *buffer_ptr)
{
    if (this->check_pnum(pnum)) {
        if (this->dfile) {
            return this->dfile->read(buffer_ptr, parm::PAGE_SIZE, PagedFile::pnum_to_offset(pnum));
        } else if (this->dfile_ptr) {
            return this->dfile_ptr->read(buffer_ptr, parm::PAGE_SIZE, PagedFile::pnum_to_offset(pnum));
        }
    }

    return 0;
}


int PagedFile::read_pages(std::vector<std::pair<PageId, byte*>> pages)
{
    size_t res = 0;
    for (auto pb_pair : pages) {
       res += this->read_page(pb_pair.first, pb_pair.second); 
    }

    return (res == pages.size()) ? 1 : 0;
}


int PagedFile::read_pages(std::vector<std::pair<PageNum, byte*>> pages) 
{
    size_t res = 0;
    for (auto pb_pair : pages) {
       res += this->read_page(pb_pair.first, pb_pair.second); 
    }

    return (res == pages.size()) ? 1 : 0;
}


int PagedFile::write_page(PageId pid, const byte *buffer_ptr)
{
    return this->write_page(pid.page_number, buffer_ptr);
}


int PagedFile::write_page(PageNum pnum, const byte *buffer_ptr)
{
    if (this->check_pnum(pnum)) {
        if (this->dfile) {
            return this->dfile->write(buffer_ptr, parm::PAGE_SIZE, PagedFile::pnum_to_offset(pnum));
        } else if (this->dfile_ptr) {
            return this->dfile_ptr->write(buffer_ptr, parm::PAGE_SIZE, PagedFile::pnum_to_offset(pnum));
        }
    }

    return 0;
}


bool PagedFile::supports_free()
{
    return this->free_supported;
}


PageAllocSupport PagedFile::supports_allocation()
{
    return this->alloc_supported;
}


bool PagedFile::is_temporary()
{
    return this->is_temp_file;
}


void PagedFile::make_permanent()
{
   this->is_temp_file = false; 
}


int PagedFile::remove_file()
{
    int res = 0;
    if (this->dfile) {
        res = this->dfile->remove();
        if (res) {
            this->dfile.reset();
        }
    } else if (this->dfile_ptr) {
        res = this->dfile_ptr->remove();
    }

    return res;
}


int PagedFile::initialize_pagedfile(byte *header_page_buf, FileId flid)
{
    if (header_page_buf) {
        PagedFileHeaderData* header = (PagedFileHeaderData*) header_page_buf;
        header->flid = flid;
        header->page_count = 0;
        header->virtual_header_page = INVALID_PNUM;
        return 1;
    }

    return 0;
}


bool PagedFile::check_pnum(PageNum pnum)
{
    if (this->dfile) {
        return pnum != INVALID_PNUM && pnum <= (this->dfile->get_size() / parm::PAGE_SIZE);
    } else if (this->dfile_ptr) {
        return pnum != INVALID_PNUM && pnum <= (this->dfile_ptr->get_size() / parm::PAGE_SIZE);
    }

    return false;
}


off_t PagedFile::pnum_to_offset(PageNum pnum) 
{
    return pnum * parm::PAGE_SIZE;
}

}}
