/*
 *
 */
#include "io/pagedfile.hpp"

namespace lsm {
namespace io {

std::unique_ptr<PagedFile> PagedFile::create(std::string fname, bool new_file)
{
    auto dfile = DirectFile::create(fname, new_file);
    if (new_file) {
        PagedFile::initialize(dfile.get());
    }

    return std::make_unique<PagedFile>(std::move(dfile), false);
}


std::unique_ptr<PagedFile> PagedFile::create_temporary()
{
    std::string fname; // TODO: name generation
    
    auto dfile = DirectFile::create(fname, true);
    PagedFile::initialize(dfile.get());

    return std::make_unique<PagedFile>(std::move(dfile), true);
}


PageId PagedFile::allocate_page()
{
    PageNum last_page = this->header_data.last_page;
    PageNum pnum = INVALID_PNUM;

    if (this->header_data.first_free_page == INVALID_PNUM) {
        pnum = last_page + 1;
        this->dfile->allocate(parm::PAGE_SIZE);
        this->header_data.last_page = pnum;
    } else {
        pnum = this->header_data.first_free_page;
    }
        
    #ifdef NO_BUFFER_MANAGER
    if (last_page != INVALID_PNUM) {
        this->dfile->read(buffer.get(), parm::PAGE_SIZE, PagedFile::pnum_to_offset(last_page));
        PageHeaderData *header = (PageHeaderData *) buffer.get();
        header->next_page = pnum;
        this->flush_buffer(last_page);
    }

    this->dfile->read(buffer.get(), parm::PAGE_SIZE, PagedFile::pnum_to_offset(pnum));
    PageHeaderData *header = (PageHeaderData *) buffer.get();
    PageNum next = header->next_page;
    header->next_page = INVALID_PNUM;
    header->prev_page = last_page;
    this->flush_buffer(pnum);

    if (this->header_data.first_free_page != INVALID_PNUM) {
        if (next != INVALID_PNUM) {
            this->dfile->read(buffer.get(), parm::PAGE_SIZE, PagedFile::pnum_to_offset(next));
            PageHeaderData *header = (PageHeaderData *) buffer.get();
            header->prev_page = INVALID_PNUM;
            this->flush_buffer(next);
        }

        this->header_data.first_free_page = next;
    }
    #endif

    if (pnum == INVALID_PNUM) {
        return INVALID_PID;
    } 

    return this->pnum_to_pid(pnum);
}


int PagedFile::read_page(PageId pid, char *buffer_ptr)
{
    return this->read_page(pid.page_number, buffer_ptr);
}


int PagedFile::read_page(PageNum pnum, char *buffer_ptr)
{
    return (this->check_pnum(pnum)) 
             ? this->dfile->read(buffer_ptr, parm::PAGE_SIZE,
                                 PagedFile::pnum_to_offset(pnum))
             : 0;
}


int PagedFile::write_page(PageId pid, const char *buffer_ptr)
{
    return this->write_page(pid.page_number, buffer_ptr);
}


int PagedFile::write_page(PageNum pnum, const char *buffer_ptr)
{
    return (this->check_pnum(pnum)) 
             ? this->dfile->write(buffer_ptr, parm::PAGE_SIZE,
                                 PagedFile::pnum_to_offset(pnum))
             : 0;
}


int PagedFile::free_page(PageId pid)
{
    return this->free_page(pid.page_number);
}


int PagedFile::free_page(PageNum pnum)
{
    #ifdef NO_BUFFER_MANAGER
    if (check_pnum(pnum)) {
        if (this->header_data.first_free_page != INVALID_PNUM) {
            this->dfile->read(this->buffer.get(), parm::PAGE_SIZE, PagedFile::pnum_to_offset(this->header_data.first_free_page));
            PageHeaderData *header = (PageHeaderData *) buffer.get();
            header->prev_page = pnum;
            this->flush_buffer(this->header_data.first_free_page);
        }

        this->dfile->read(this->buffer.get(), parm::PAGE_SIZE, PagedFile::pnum_to_offset(pnum));
        PageHeaderData *header = (PageHeaderData *) buffer.get();
        PageNum next = header->next_page;
        PageNum prev = header->prev_page;

        header->next_page = this->header_data.first_free_page;
        header->prev_page = INVALID_PNUM;
        this->flush_buffer(pnum);

        // Update adjacent links to circumvent the
        // freed page, if necessary.
        if (next != INVALID_PNUM) {
            this->dfile->read(this->buffer.get(), parm::PAGE_SIZE, PagedFile::pnum_to_offset(next));
            header = (PageHeaderData *) buffer.get();
            header->prev_page = prev;
            this->flush_buffer(next);
        }

        if (prev != INVALID_PNUM) {
            this->dfile->read(this->buffer.get(), parm::PAGE_SIZE, PagedFile::pnum_to_offset(prev));
            header = (PageHeaderData *) buffer.get();
            header->next_page = next;
            this->flush_buffer(prev);
        }

        // Update first/last page listing in the header if need be
        if (this->header_data.first_page == pnum) {
            this->header_data.first_page = next;
        }

        if (this->header_data.last_page == pnum) {
            this->header_data.last_page = prev;
        }

        this->header_data.page_count--;
        return 1;
    }
    #endif

    return 0;
}


PageId PagedFile::pnum_to_pid(PageNum pnum)
{
    PageId pid;
    pid.page_number = pnum;
    pid.file_id = this->header_data.flid;
    return pid;
}


bool PagedFile::is_temporary()
{
    return this->is_temp_file;
}


void PagedFile::make_permanent()
{
   this->is_temp_file = false; 
}


PageNum PagedFile::get_page_count()
{
    return this->header_data.page_count;
}


PageId PagedFile::get_first_pid()
{
    return this->pnum_to_pid(this->header_data.first_page);
}


PageId PagedFile::get_last_pid()
{
    return this->pnum_to_pid(this->header_data.last_page);
}


std::unique_ptr<PageFileIterator> PagedFile::start_scan(PageId pid)
{
    return start_scan(pid.page_number);
}


std::unique_ptr<PageFileIterator> PagedFile::start_scan(PageNum pnum)
{
    return nullptr;
}

int PagedFile::remove_file()
{
    auto res = this->dfile->remove();
    if (res) {
        this->dfile.release();
    }

    return res;
}

PagedFile::~PagedFile()
{
    this->flush_metadata();
    this->dfile->close_file();

    if (this->is_temporary()) {
        this->dfile->remove();
    }
}


void PagedFile::initialize(DirectFile *dfile)
{
    dfile->allocate(parm::PAGE_SIZE);

    #ifdef NO_BUFFER_MANAGER
    byte page[parm::PAGE_SIZE];

    PagedFileHeaderData *header = (PagedFileHeaderData *) page;
    #endif

    header->last_page = INVALID_PNUM;
    header->first_page = INVALID_PNUM;
    header->first_free_page = INVALID_PNUM;
    header->page_count = 0;
    header->flid = next_flid();

    auto offset = PagedFile::pnum_to_offset(PagedFile::header_page_pnum);
    dfile->write(page, parm::PAGE_SIZE, offset);
}


PagedFile::PagedFile(std::unique_ptr<DirectFile> dfile, bool is_temp_file)
{
    this->is_temp_file = is_temp_file;
    this->dfile = std::move(dfile);

    #ifdef NO_BUFFER_MANAGER
    this->buffer = std::unique_ptr<byte>((byte *) aligned_alloc(parm::PAGE_SIZE, parm::PAGE_SIZE));
    auto offset = PagedFile::pnum_to_offset(PagedFile::header_page_pnum);
    this->dfile->read(this->buffer.get(), parm::PAGE_SIZE, offset);
    memcpy(&this->header_data, buffer.get(), sizeof(header_data));
    #endif
}


#ifdef NO_BUFFER_MANAGER
void PagedFile::flush_buffer(PageNum pnum)
{
    assert(this->check_pnum(pnum));
    off_t offset = PagedFile::pnum_to_offset(pnum);
    this->dfile->write(this->buffer.get(), parm::PAGE_SIZE, offset);
}
#endif


void PagedFile::flush_metadata()
{
    #ifdef NO_BUFFER_MANAGER
    memcpy(buffer.get(), &this->header_data, PagedFileHeaderSize);
    this->flush_buffer(PagedFile::header_page_pnum);
    #endif
}


off_t PagedFile::pnum_to_offset(PageNum pnum) 
{
    return pnum * parm::PAGE_SIZE;
}
}
}
