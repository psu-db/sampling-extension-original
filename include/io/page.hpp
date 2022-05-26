/*
 *
 */
#ifndef PAGE_H
#define PAGE_H

#include "util/types.hpp"
#include "io/record.hpp"
#include "util/iterator.hpp"

namespace lsm {
namespace io {

struct PageHeaderData {
    PageNum next_page;
    PageNum prev_page;

    SlotId record_count;
    PageOffset user_data_length;
    PageOffset fixed_record_length; // 0 means it is a varlen data page
    PageOffset free_space_begin;
};

constexpr size_t PageHeaderSize = MAXALIGN(sizeof(PageHeaderData));

class Page {
public:
    Page() = default;
    ~Page() = default;
    Record get_record(SlotId sid);
    byte *get_user_data(); 
    byte *get_page_data();
    SlotId get_record_count();
    SlotId get_min_sid();

    virtual bool is_occupied(SlotId sid) = 0;
    virtual byte *get_record_buffer(SlotId sid, PageOffset *record_length) = 0;
    virtual SlotId insert_record(Record rec) = 0;
    virtual std::unique_ptr<iter::GenericIterator<Record>> start_scan(SlotId sid=0) = 0;
    virtual SlotId get_max_sid() = 0;

protected:
    PageHeaderData *get_header();
    byte *page_data;

private:
    SlotId min_slot_id = 1;
};

}
}
#endif 
