/*
 * pagedfile.cpp
 * DouglasRumbaugh
 *
 * PagedFile ABC partial implementation
 */

#include "io/pagedfile.hpp"

namespace lsm { namespace io {

PagedFile::PagedFile(std::unique_ptr<DirectFile> dfile, bool is_temp_file)
: dfile(std::move(dfile)), is_temp_file(is_temp_file) {}


int PagedFile::read_page(PageId pid, byte *buffer_ptr)
{
    return this->read_page(pid.page_number, buffer_ptr);
}


int PagedFile::read_page(PageNum pnum, byte *buffer_ptr)
{
    return (this->check_pnum(pnum)) 
             ? this->dfile->read(buffer_ptr, parm::PAGE_SIZE,
                                 PagedFile::pnum_to_offset(pnum))
             : 0;
}


int PagedFile::write_page(PageId pid, const byte *buffer_ptr)
{
    return this->write_page(pid.page_number, buffer_ptr);
}


int PagedFile::write_page(PageNum pnum, const byte *buffer_ptr)
{
    return (this->check_pnum(pnum)) 
             ? this->dfile->write(buffer_ptr, parm::PAGE_SIZE,
                                 PagedFile::pnum_to_offset(pnum))
             : 0;
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
    auto res = this->dfile->remove();
    if (res) {
        this->dfile.release();
    }

    return res;
}


int PagedFile::initialize_pagedfile(byte *header_page_buf, FileId flid)
{
        if (header_page_buf) {
            PagedFileHeaderData* header = (PagedFileHeaderData*) header_page_buf;
            header->flid = flid;
            header->page_count = 0;
            return 1;
        }

        return 0;
}


bool PagedFile::check_pnum(PageNum pnum)
{
    return pnum != INVALID_PNUM && pnum <= (this->dfile->get_size() / parm::PAGE_SIZE);
}

off_t PagedFile::pnum_to_offset(PageNum pnum) 
{
    return pnum * parm::PAGE_SIZE;
}

}}
