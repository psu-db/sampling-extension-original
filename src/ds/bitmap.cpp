#include "ds/bitmap.hpp"

namespace lsm { namespace ds {

std::unique_ptr<BitMap> BitMap::create(size_t size, PageId meta_pid, global::g_state *state, bool default_value)
{
    auto pfile = state->file_manager->get_pfile(meta_pid.file_id);    
    if (pfile) {
        return BitMap::create(size, meta_pid.page_number, pfile, default_value);
    }

    return nullptr;
}


std::unique_ptr<BitMap> BitMap::create(size_t size, PageNum meta_pnum, io::PagedFile *pfile, bool default_value)
{
    auto page_buf = mem::page_alloc();
         PageNum first_pnum;
         PageNum last_pnum;

    {
        auto meta_page = (BitMapMetaHeader *) page_buf.get();

        meta_page->logical_size = size;
        meta_page->physical_size = size + (size % 8) / 8;

        PageNum page_cnt = meta_page->physical_size / parm::PAGE_SIZE;
        meta_page->first_page = first_pnum =  pfile->allocate_page().page_number;
        meta_page->last_page = last_pnum = meta_page->first_page;

        for (PageNum pnum = 0; pnum < page_cnt; pnum++) {
            meta_page->last_page = last_pnum = pfile->allocate_page().page_number;
        }

        pfile->write_page(meta_pnum, page_buf.get());
    }

    memset(page_buf.get(), default_value, parm::PAGE_SIZE);
    for (PageNum pnum=first_pnum; pnum<=last_pnum; pnum++) {
        pfile->write_page(pnum, page_buf.get());
    }

    auto bm = new BitMap(meta_pnum, pfile);
    return std::unique_ptr<BitMap>(bm);
}


std::unique_ptr<BitMap> BitMap::open(PageId meta_pid, global::g_state *state)
{
    auto pfile = state->file_manager->get_pfile(meta_pid.file_id);
    return BitMap::open(meta_pid.page_number, pfile);
}


std::unique_ptr<BitMap> BitMap::open(PageNum meta_pnum, io::PagedFile *pfile)
{
    auto bm = new BitMap(meta_pnum, pfile);
    return std::unique_ptr<BitMap>(bm);
}


BitMap::BitMap(PageNum meta_pnum, io::PagedFile *pfile)
{
    this->pfile = pfile;

    auto buf = mem::page_alloc();
    this->pfile->read_page(meta_pnum, buf.get());
    {
        auto meta = (BitMapMetaHeader *) buf.get();
        this->first_pnum = meta->first_page;
        this->last_pnum = meta->last_page;
        this->physical_size = meta->physical_size;
        this->logical_size = meta->logical_size;
    }

    this->bits = std::make_unique<byte[]>(this->physical_size);
    
    size_t total_bytes_copied = 0;
    for (PageNum pnum=this->first_pnum; pnum<=this->last_pnum; pnum++) {
        this->pfile->read_page(pnum, buf.get());
        size_t to_copy = std::min(parm::PAGE_SIZE, this->physical_size - total_bytes_copied);
        memcpy(this->bits.get() + total_bytes_copied, buf.get(), to_copy);
        total_bytes_copied += to_copy;
    }
}


bit_masking_data BitMap::calculate_mask(size_t bit)
{
    bit_masking_data masking;

    auto byte_offset = bit / 8;
    auto bit_offset = bit % 8;

    masking.check_byte = this->bits.get() + byte_offset;
    masking.bit_mask = (std::byte) 0x1 << bit_offset;

    return masking;
}


bool BitMap::is_set(size_t bit) 
{
    if (bit >= this->logical_size) {
        return false;
    }

    auto masking = this->calculate_mask(bit);

    return (bool) (*masking.check_byte & masking.bit_mask);
}


int BitMap::set(size_t bit) 
{
    if (bit >= this->logical_size) {
        return 0;
    }

    auto masking = this->calculate_mask(bit);

    *masking.check_byte |= masking.bit_mask;

    return 1;
}


int BitMap::unset(size_t bit) 
{
    if (bit >= this->logical_size) {
        return 0;
    }

    auto masking = this->calculate_mask(bit);

    *masking.check_byte &= ~masking.bit_mask;

    return 1;
}


void BitMap::unset_all() 
{
   for (size_t i=0; i<this->physical_size; i++) {
        this->bits[i] = (std::byte) 0x0;
    } 
}


void BitMap::flush()
{
    auto buf = mem::page_alloc();

    size_t total_bytes_copied = 0;
    for (PageNum pnum=this->first_pnum; pnum<=this->last_pnum; pnum++) {
        size_t to_copy = std::min(parm::PAGE_SIZE, this->physical_size - total_bytes_copied);
        memcpy(buf.get(), this->bits.get() + total_bytes_copied, to_copy);
        this->pfile->write_page(pnum, buf.get());
        total_bytes_copied += to_copy;
    }
}

}}
