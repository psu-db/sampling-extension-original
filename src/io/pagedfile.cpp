/*
 * pagedfile.cpp
 * Douglas Rumbaugh
 *
 * PagedFile implementation
 */

#include "io/pagedfile.hpp"

namespace lsm { namespace io {

PageNum PagedFile::allocate_pages(PageNum count)
{
    PageNum new_first = this->get_page_count() + 1;
    size_t alloc_size = count * parm::PAGE_SIZE;

    if (this->dfile->allocate(alloc_size)) {
        return new_first;
    }

    return INVALID_PNUM;
}


int PagedFile::read_page(PageNum pnum, byte *buffer_ptr)
{
    if (this->check_pnum(pnum)) {
        return this->dfile->read(buffer_ptr, parm::PAGE_SIZE, PagedFile::pnum_to_offset(pnum));
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
            if (!this->dfile->readv(buffers, parm::PAGE_SIZE, PagedFile::pnum_to_offset(range_start))) {
                return 0;
            }

            range_start = pages[i].first;
            prev_pnum = range_start;

            buffers.clear();
            buffers.push_back(pages[i].second);
        }
    }

    return this->dfile->readv(buffers, parm::PAGE_SIZE, PagedFile::pnum_to_offset(range_start));
}


int PagedFile::write_page(PageNum pnum, const byte *buffer_ptr)
{
    if (this->check_pnum(pnum)) {
        return this->dfile->write(buffer_ptr, parm::PAGE_SIZE, PagedFile::pnum_to_offset(pnum));
    }

    return 0;
}


int PagedFile::remove_file()
{
    return this->dfile->remove();
}


PageNum PagedFile::get_page_count()
{
    return this->dfile->get_size() / parm::PAGE_SIZE;
}


PagedFile::~PagedFile()
{
    this->dfile->close_file();
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
