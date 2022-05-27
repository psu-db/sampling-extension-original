/*
 * indexpagedfile.cpp
 * Douglas Rumbaugh
 *
 * IndexPagedFile implementation
 */

#include "io/indexpagedfile.hpp"

namespace lsm { namespace io {

std::unique_ptr<IndexPagedFile> IndexPagedFile::create(std::string fname, bool new_file, FileId flid)
{
    auto dfile = DirectFile::create(fname, new_file);
    if (new_file) {
        IndexPagedFile::initialize(dfile.get(), flid);
    }

    return std::make_unique<IndexPagedFile>(std::move(dfile), false);
}


std::unique_ptr<IndexPagedFile> IndexPagedFile::create_temporary(FileId flid)
{
    std::string fname; // TODO: name generation
    
    auto dfile = DirectFile::create(fname, true);
    IndexPagedFile::initialize(dfile.get(), flid);

    return std::make_unique<IndexPagedFile>(std::move(dfile), true);
}


PageId IndexPagedFile::allocate_page()
{
    return allocate_page_bulk(1);
}


PageId IndexPagedFile::allocate_page_bulk(PageNum new_page_count)
{
    PageNum new_first = this->get_last_pid().page_number + 1;
    size_t alloc_size = new_page_count * parm::PAGE_SIZE;


    if (this->dfile->allocate(alloc_size)) {
        this->header_data.paged_header.page_count += new_page_count;
        return this->pnum_to_pid(new_first);
    }

    return INVALID_PID;
}


int IndexPagedFile::free_page(PageId pid)
{
    return 0;
}


int IndexPagedFile::free_page(PageNum pnum)
{
    return 0;
}


PageId IndexPagedFile::pnum_to_pid(PageNum pnum)
{
    PageId pid;
    pid.page_number = pnum;
    pid.file_id = this->header_data.paged_header.flid;
    return pid;
}


PageNum IndexPagedFile::get_page_count()
{
    return this->header_data.paged_header.page_count;
}


PageId IndexPagedFile::get_first_pid()
{
    if (this->header_data.paged_header.page_count == 0) {
        return this->pnum_to_pid(INVALID_PNUM);
    } 

    return this->pnum_to_pid(1);
}


PageId IndexPagedFile::get_last_pid()
{
    return this->pnum_to_pid(this->header_data.paged_header.page_count);
}


std::unique_ptr<iter::GenericIterator<Page *>> IndexPagedFile::start_scan(PageId pid)
{
    return start_scan(pid.page_number);
}


std::unique_ptr<iter::GenericIterator<Page *>> IndexPagedFile::start_scan(PageNum pnum)
{
    return nullptr;
}


IndexPagedFile::~IndexPagedFile()
{
    this->flush_metadata();
    this->dfile->close_file();

    if (this->is_temporary()) {
        this->dfile->remove();
    }
}


int IndexPagedFile::initialize(DirectFile *dfile, FileId flid)
{
    if (dfile->allocate(parm::PAGE_SIZE)) {
        #ifdef NO_BUFFER_MANAGER
        std::unique_ptr<byte> page = std::unique_ptr<byte>((byte *) std::aligned_alloc(parm::SECTOR_SIZE, parm::PAGE_SIZE));

        IndexPagedFileHeaderData *header = (IndexPagedFileHeaderData *) page.get();
        #endif

        PagedFile::initialize_pagedfile(page.get(), flid);

        auto offset = PagedFile::pnum_to_offset(IndexPagedFile::header_page_pnum);
        if (dfile->write(page.get(), parm::PAGE_SIZE, offset)) {
            return 1;
        }

        return 0;
    }

    return 0;
}


IndexPagedFile::IndexPagedFile(std::unique_ptr<DirectFile> dfile, bool is_temp_file)
: PagedFile(std::move(dfile), is_temp_file, false, PageAllocSupport::BULK, true)
{
    #ifdef NO_BUFFER_MANAGER
    this->buffer = std::unique_ptr<byte>((byte *) aligned_alloc(parm::PAGE_SIZE, parm::PAGE_SIZE));
    auto offset = PagedFile::pnum_to_offset(IndexPagedFile::header_page_pnum);
    this->dfile->read(this->buffer.get(), parm::PAGE_SIZE, offset);
    memcpy(&this->header_data, buffer.get(), sizeof(header_data));
    #endif
}


#ifdef NO_BUFFER_MANAGER
void IndexPagedFile::flush_buffer(PageNum pnum)
{
    off_t offset = PagedFile::pnum_to_offset(pnum);
    this->dfile->write(this->buffer.get(), parm::PAGE_SIZE, offset);
}
#endif


void IndexPagedFile::flush_metadata()
{
    #ifdef NO_BUFFER_MANAGER
    memcpy(buffer.get(), &this->header_data, IndexPagedFileHeaderSize);
    this->flush_buffer(IndexPagedFile::header_page_pnum);
    #endif
}


bool IndexPagedFile::virtual_header_initialized()
{
    return this->header_data.paged_header.virtual_header_page != INVALID_PNUM;
}


int IndexPagedFile::initialize_for_virtualization()
{
    return 0;
}

}}
