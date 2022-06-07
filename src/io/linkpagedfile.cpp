/*
 * linkpagedfile.cpp
 * Douglas Rumbaugh
 *
 * LinkPagedFile Implementation
 */
 
#include "io/linkpagedfile.hpp"

namespace lsm { namespace io {

std::unique_ptr<LinkPagedFile> LinkPagedFile::create(std::string fname, bool new_file, FileId flid)
{
    auto dfile = DirectFile::create(fname, new_file);
    if (new_file) {
        LinkPagedFile::initialize(dfile.get(), flid);
    }

    return std::make_unique<LinkPagedFile>(std::move(dfile), false);
}


std::unique_ptr<LinkPagedFile> LinkPagedFile::create_temporary(FileId flid)
{
    std::string fname; // TODO: name generation
    
    auto dfile = DirectFile::create(fname, true);
    LinkPagedFile::initialize(dfile.get(), flid);

    return std::make_unique<LinkPagedFile>(std::move(dfile), true);
}


PageId LinkPagedFile::allocate_page()
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

    if (this->header_data.first_page == INVALID_PNUM) {
        this->header_data.first_page = pnum;
    }

    if (this->header_data.last_page == INVALID_PNUM) {
        this->header_data.last_page = pnum;
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

    this->header_data.last_page = pnum;
    this->header_data.paged_header.page_count++;
    return this->pnum_to_pid(pnum);
}


PageId LinkPagedFile::allocate_page_bulk(PageNum /*new_page_count*/)
{
    return INVALID_PID;
}


int LinkPagedFile::free_page(PageId pid)
{
    return this->free_page(pid.page_number);
}


int LinkPagedFile::free_page(PageNum pnum)
{
    #ifdef NO_BUFFER_MANAGER
    if (PagedFile::check_pnum(pnum)) {
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


        this->header_data.first_free_page = pnum;
        this->header_data.paged_header.page_count--;
        return 1;
    }
    #endif

    return 0;
}


PageId LinkPagedFile::pnum_to_pid(PageNum pnum)
{
    PageId pid;
    pid.page_number = pnum;
    pid.file_id = this->header_data.paged_header.flid;
    return pid;
}


PageNum LinkPagedFile::get_page_count()
{
    return this->header_data.paged_header.page_count;
}


PageId LinkPagedFile::get_first_pid()
{
    return this->pnum_to_pid(this->header_data.first_page);
}


PageId LinkPagedFile::get_last_pid()
{
    return this->pnum_to_pid(this->header_data.last_page);
}


std::unique_ptr<iter::GenericIterator<Page *>> LinkPagedFile::start_scan(PageId start_page, PageId stop_page)
{
    return start_scan(start_page.page_number, stop_page.page_number);
}


std::unique_ptr<iter::GenericIterator<Page *>> LinkPagedFile::start_scan(PageNum /*start_page*/, PageNum /*stop_page*/)
{
    return nullptr;
}


LinkPagedFile::~LinkPagedFile()
{
    this->flush_metadata();
    this->dfile->close_file();

    if (this->is_temporary()) {
        this->dfile->remove();
    }
}


int LinkPagedFile::initialize(DirectFile *dfile, FileId flid)
{
    if (dfile->allocate(parm::PAGE_SIZE)) {
        #ifdef NO_BUFFER_MANAGER
        std::unique_ptr<byte> page = std::unique_ptr<byte>((byte *) std::aligned_alloc(parm::SECTOR_SIZE, parm::PAGE_SIZE));

        LinkPagedFileHeaderData *header = (LinkPagedFileHeaderData *) page.get();
        #endif

        PagedFile::initialize_pagedfile(page.get(), flid);

        header->last_page = INVALID_PNUM;
        header->first_page = INVALID_PNUM;
        header->first_free_page = INVALID_PNUM;

        auto offset = PagedFile::pnum_to_offset(LinkPagedFile::header_page_pnum);
        if (dfile->write(page.get(), parm::PAGE_SIZE, offset)) {
            return 1;
        }

        return 0;
    }

    return 0;
}


LinkPagedFile::LinkPagedFile(std::unique_ptr<DirectFile> dfile, bool is_temp_file)
: PagedFile(std::move(dfile), is_temp_file, true, PageAllocSupport::SINGLE, false)
{
    #ifdef NO_BUFFER_MANAGER
    this->buffer = std::unique_ptr<byte>((byte *) aligned_alloc(parm::PAGE_SIZE, parm::PAGE_SIZE));
    auto offset = PagedFile::pnum_to_offset(LinkPagedFile::header_page_pnum);
    this->dfile->read(this->buffer.get(), parm::PAGE_SIZE, offset);
    memcpy(&this->header_data, buffer.get(), sizeof(header_data));
    #endif
}


#ifdef NO_BUFFER_MANAGER
void LinkPagedFile::flush_buffer(PageNum pnum)
{
    off_t offset = PagedFile::pnum_to_offset(pnum);
    this->dfile->write(this->buffer.get(), parm::PAGE_SIZE, offset);
}
#endif


void LinkPagedFile::flush_metadata()
{
    #ifdef NO_BUFFER_MANAGER
    memcpy(buffer.get(), &this->header_data, LinkPagedFileHeaderSize);
    this->flush_buffer(LinkPagedFile::header_page_pnum);
    #endif
}

bool LinkPagedFile::virtual_header_initialized()
{
    return false;
}


int LinkPagedFile::initialize_for_virtualization()
{
    return 0;
}


FileId LinkPagedFile::get_flid()
{
    return this->header_data.paged_header.flid;
}

}}
