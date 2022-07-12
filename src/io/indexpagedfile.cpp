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

    if (this->dfile_ptr->allocate(alloc_size)) {
        this->header_data.paged_header.page_count += new_page_count;
        return this->pnum_to_pid(new_first);
    }

    return INVALID_PID;
}


int IndexPagedFile::free_page(PageId /*pid*/)
{
    return 0;
}


int IndexPagedFile::free_page(PageNum /*pnum*/)
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


std::unique_ptr<iter::GenericIterator<Page *>> IndexPagedFile::start_scan(PageId start_page, PageId stop_page)
{
    return start_scan(start_page.page_number, stop_page.page_number);
}


std::unique_ptr<iter::GenericIterator<Page *>> IndexPagedFile::start_scan(PageNum /*start_page*/, PageNum /*stop_page*/)
{
    return nullptr;
}


IndexPagedFile::~IndexPagedFile()
{
    if (this->dfile_ptr) {
        this->flush_metadata();
        this->dfile_ptr->close_file();

        if (this->is_temporary()) {
            this->dfile_ptr->remove();
        }
    }
}


int IndexPagedFile::initialize(DirectFile *dfile, FileId flid)
{
    if (dfile->allocate(parm::PAGE_SIZE)) {
        auto page = std::unique_ptr<byte, decltype(&free)>((byte *) std::aligned_alloc(parm::SECTOR_SIZE, parm::PAGE_SIZE), &free);


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
    this->buffer = std::unique_ptr<byte, decltype(&free)>((byte *) aligned_alloc(parm::PAGE_SIZE, parm::PAGE_SIZE), &free);
    auto offset = PagedFile::pnum_to_offset(IndexPagedFile::header_page_pnum);
    this->dfile->read(this->buffer.get(), parm::PAGE_SIZE, offset);
    memcpy(&this->header_data, buffer.get(), sizeof(header_data));

    this->dfile_ptr = this->dfile.get();
}


IndexPagedFile::IndexPagedFile(DirectFile *dfile, bool is_temp_file)
: PagedFile(dfile, is_temp_file, false, PageAllocSupport::BULK, true)
{
    this->buffer = std::unique_ptr<byte, decltype(&free)>((byte *) aligned_alloc(parm::PAGE_SIZE, parm::PAGE_SIZE), &free);
    auto offset = PagedFile::pnum_to_offset(IndexPagedFile::header_page_pnum);
    this->dfile_ptr->read(this->buffer.get(), parm::PAGE_SIZE, offset);
    memcpy(&this->header_data, buffer.get(), sizeof(header_data));
}


#ifdef NO_BUFFER_MANAGER
void IndexPagedFile::flush_buffer(PageNum pnum)
{
    off_t offset = PagedFile::pnum_to_offset(pnum);
    if (this->dfile_ptr) {
        this->dfile_ptr->write(this->buffer.get(), parm::PAGE_SIZE, offset);
    }
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
    PageNum header_pg = this->allocate_page().page_number;

    if (header_pg == INVALID_PNUM) {
        return 0;
    }

    this->header_data.paged_header.virtual_header_page = header_pg;
    return 1;
}


FileId IndexPagedFile::get_flid()
{
    return this->header_data.paged_header.flid;
}


IndexPagedFilePageIterator::IndexPagedFilePageIterator(IndexPagedFile *file, ReadCache *cache, PageNum start_page, PageNum stop_page) 
{
    this->pfile = file;
    this->current_pnum = (start_page == INVALID_PNUM) ? 0 : start_page - 1;
    this->first_pnum = this->current_pnum + 1;
    this->final_pnum = (stop_page == INVALID_PNUM) ? this->pfile->get_last_pid().page_number : stop_page;
    this->cache = cache;
    this->current_frame_id = INVALID_FRID;
    this->current_frame_ptr = nullptr;
    this->current_page = nullptr;
    this->at_end = false;
}


bool IndexPagedFilePageIterator::next()
{
    while (this->current_pnum < this->final_pnum) {
        if (this->current_frame_id != INVALID_FRID) {
            this->cache->unpin(this->current_frame_id);
        }

        this->current_frame_id = this->cache->pin(++this->current_pnum, this->pfile, &this->current_frame_ptr);
        this->current_page = wrap_page(this->current_frame_ptr); 

        return true;
    }

    this->current_page = nullptr;
    this->at_end = true;
    return false;
}


Page *IndexPagedFilePageIterator::get_item()
{
    return this->current_page.get();
}


bool IndexPagedFilePageIterator::supports_rewind()
{
    return true;
}


iter::IteratorPosition IndexPagedFilePageIterator::save_position()
{
    return (iter::IteratorPosition) this->current_pnum;
}


void IndexPagedFilePageIterator::rewind(iter::IteratorPosition position)
{
    if (this->current_frame_id != INVALID_FRID) {
        this->cache->unpin(this->current_frame_id);
    }

    this->current_pnum = (PageNum) position;
    this->current_frame_id = this->cache->pin(this->current_pnum, this->pfile, &this->current_frame_ptr);
    this->current_page = wrap_page(this->current_frame_ptr);
}


void IndexPagedFilePageIterator::end_scan()
{
    if (this->current_frame_id != INVALID_FRID) {
        this->cache->unpin(this->current_frame_id);
    }
}


IndexPagedFilePageIterator::~IndexPagedFilePageIterator()
{
    this->end_scan();
}


size_t IndexPagedFilePageIterator::element_count()
{
    return this->final_pnum - this->first_pnum + 1;
}


bool IndexPagedFilePageIterator::supports_element_count()
{
    return true;
}


IndexPagedFileRecordIterator::IndexPagedFileRecordIterator(IndexPagedFile *file, ReadCache *cache, PageNum start_page, PageNum stop_page)
{
    this->page_itr = std::make_unique<IndexPagedFilePageIterator>(file, cache, start_page, stop_page);
    this->page_itr->next();
    this->current_page = this->page_itr->get_item();
    if (this->current_page) {
        this->record_itr = this->current_page->start_scan();
    }

    this->at_end = false;
}


IndexPagedFileRecordIterator::IndexPagedFileRecordIterator(IndexPagedFile *file, ReadCache *cache, PageNum start_page, SlotId start_slot)
{
    this->page_itr = std::make_unique<IndexPagedFilePageIterator>(file, cache, start_page, INVALID_PNUM);
    this->page_itr->next();
    this->current_page = this->page_itr->get_item();
    if (this->current_page) {
        this->record_itr = this->current_page->start_scan(start_slot);
    }

    this->at_end = false;
}


bool IndexPagedFileRecordIterator::next()
{
    while (!this->at_end) {
        if (this->record_itr->next()) {
            this->current_record = this->record_itr->get_item();
            return true;
        } else {
            if (this->page_itr->next()) {
                this->record_itr->end_scan();
                this->current_page = this->page_itr->get_item();
                this->record_itr = this->current_page->start_scan();
            } else {
                this->record_itr->end_scan();
                this->page_itr->end_scan();
                this->at_end = true;
            }
        }
    }

    return false;
}


Record IndexPagedFileRecordIterator::get_item()
{
    return this->current_record;
}


bool IndexPagedFileRecordIterator::supports_rewind()
{
    return false;
}


iter::IteratorPosition IndexPagedFileRecordIterator::save_position()
{
    return 0;
}


void IndexPagedFileRecordIterator::rewind(iter::IteratorPosition /*position*/)
{

}


size_t IndexPagedFileRecordIterator::element_count()
{
    return 0;
}


bool IndexPagedFileRecordIterator::supports_element_count()
{
    return false;
}

void IndexPagedFileRecordIterator::end_scan()
{
    this->record_itr->end_scan();
    this->page_itr->end_scan();
}


IndexPagedFileRecordIterator::~IndexPagedFileRecordIterator()
{
    this->end_scan();
}


}}
