/*
 *
 */

#include "io/page.hpp"

namespace lsm { namespace io { 

byte *Page::get_user_data() 
{ 
    if (this->get_header()->user_data_length == 0) {
        return nullptr;
    }

    return this->page_data + PageHeaderSize;
}


byte *Page::get_page_data()
{
    return this->page_data + this->get_header()->user_data_length + PageHeaderSize;
}


SlotId Page::get_record_count()
{
    return this->get_header()->record_count;
}


Record Page::get_record(SlotId sid) 
{
    Record new_record;
    if (this->is_occupied(sid)) {
        PageOffset len;
        byte *rec_data = this->get_record_buffer(sid, &len);
        new_record.get_length() = len;
        new_record.get_data() = rec_data;
    } 

    return new_record;
}


SlotId Page::get_min_sid()
{
    return this->min_slot_id;
}


PageHeaderData *Page::get_header()
{
    return (PageHeaderData *) this->page_data;
}

}}
