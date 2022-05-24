/*
 *
 */
#include "io/fixedlendatapage.hpp"

namespace lsm {
namespace io {

void FixedlenDataPage::initialize(byte *page_ptr, PageOffset record_length, PageOffset user_data_length) 
{
    auto header = (PageHeaderData *) page_ptr;
    header->fixed_record_length = record_length;
    header->user_data_length = MAXALIGN(user_data_length);
    header->record_count = 0;
    header->free_space_begin = (size_t) (page_ptr + MAXALIGN(user_data_length) + PageHeaderSize);
}

FixedlenDataPage::FixedlenDataPage(byte *page_ptr)
    : Page(page_ptr) {}



bool FixedlenDataPage::is_occupied(SlotId sid) 
{
    // For fixed-length pages without storage-engine level support for
    // record deletion, a slot is considered occupied if it is less than
    // or equal to the number of records in the page.
    return (sid >= this->get_min_sid()) && (sid <= this->get_max_sid());
}


byte *FixedlenDataPage::get_record_buffer(SlotId sid, PageOffset *record_length) 
{
    if (this->is_occupied(sid)) {
        if (record_length) {
            *record_length = this->get_header()->fixed_record_length;
        }

        return this->get_page_data() + (sid * this->get_header()->fixed_record_length);
    }

    return nullptr;
}


SlotId FixedlenDataPage::insert_record(Record rec)
{
    // Check if there is room for the insertion
    if (this->get_header()->free_space_begin +
      this->get_header()->fixed_record_length > parm::PAGE_SIZE) { 
        return INVALID_SID;
    }

    // Copy the record into the page buffer
    memmove(this->page_data + this->get_header()->free_space_begin,
            rec.get_data(), this->get_header()->fixed_record_length);

    // Update the page header information
    this->get_header()->free_space_begin +=
        this->get_header()->fixed_record_length;
    this->get_header()->record_count++;

    // The SlotId for the record will be the record count after insertion,
    // as the record are numbered sequentially, without gaps, and deletions
    // are not possible.
    return this->get_max_sid();
}


SlotId FixedlenDataPage::get_max_sid() 
{
    return this->get_header()->record_count;
}


std::unique_ptr<iter::GenericIterator<Record>> FixedlenDataPage::start_scan(SlotId sid) 
{
    // if no sid is specified, start the scan at the first record.
    if (sid == INVALID_SID) {
        sid = 1;
    }

    if (sid > this->get_max_sid()) {
        return nullptr;
    }

    return std::make_unique<FixedlenDataPageRecordIterator>(this, sid);
}


FixedlenDataPageRecordIterator::FixedlenDataPageRecordIterator(FixedlenDataPage *dpage, SlotId starting_slot)
{
    this->current_slot = (starting_slot == INVALID_SID) ? starting_slot : starting_slot - 1;
    this->page = dpage;
}


bool FixedlenDataPageRecordIterator::next() 
{
    while (this->page->is_occupied(++(this->current_slot))) {
        return true;
    }

    return false;
}


Record FixedlenDataPageRecordIterator::get_item() 
{
    return page->get_record(this->current_slot);
}


bool FixedlenDataPageRecordIterator::supports_rewind() 
{
    return true;
}


iter::IteratorPosition FixedlenDataPageRecordIterator::save_position()
{
    return (iter::IteratorPosition) this->current_slot;
}


void FixedlenDataPageRecordIterator::rewind(iter::IteratorPosition position)
{
    if ((SlotId) position == INVALID_SID) {
        this->current_slot = 1;
    } else {
        this->current_slot = (SlotId) position;
    }
}


void end_scan() 
{

}

}
}
