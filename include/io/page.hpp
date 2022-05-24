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
    Page(byte *page_data_ptr) : page_data(page_data_ptr) {}
    ~Page() {}

    byte *get_user_data() {
        if (this->get_header()->user_data_length == 0) {
            return nullptr;
        }

        return this->page_data + PageHeaderSize;
    }

    byte *get_page_data() {
        return this->page_data + this->get_header()->user_data_length + PageHeaderSize;
    };

    virtual bool is_occupied(SlotId sid) = 0;

    SlotId get_record_count() {
        return this->get_header()->record_count;
    };

    virtual byte *get_record_buffer(SlotId sid, PageOffset *record_length) = 0;

    Record get_record(SlotId sid) {
        Record new_record;
        if (this->is_occupied(sid)) {
            PageOffset len;
            byte *rec_data = this->get_record_buffer(sid, &len);
            new_record.get_length() = len;
            new_record.get_data() = rec_data;
        } 

        return new_record;
    }

    virtual SlotId insert_record(Record rec) = 0;

    virtual std::unique_ptr<iter::GenericIterator<Record>> start_scan(SlotId sid=0);

    SlotId get_min_sid() {
        return this->min_slot_id;
    }

    virtual SlotId get_max_sid() = 0;

protected:
    PageHeaderData *get_header() {
        return (PageHeaderData *) this->page_data;
    }

    byte *page_data;

private:
    SlotId min_slot_id = 1;
};

}
}
#endif 
