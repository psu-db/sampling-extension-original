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

    /*
     * Returns the record located as the specified SlotId. If the specified
     * SlotId is invalid or unoccupied, the returned Record will be invalid.
     */
    Record get_record(SlotId sid);

    /*
     * Returns a pointer to the start of the user data region of this page.
     * If there is no user data region, returns nullptr.
     */
    byte *get_user_data(); 

    /*
     * Returns a pointer to the start of the data region of this page, where records
     * are stored. This pointer will fall after the page header and user data regions.
     */
    byte *get_page_data();

    /*
     * Returns the number of records stored in the page.
     */
    SlotId get_record_count();

    /*
     * Returns the minimum valid SlotId for this page. This slot may not
     * be occupied.
     */
    SlotId get_min_sid();

    /*
     * Returns true is a record is stored in the specified slot, and false if
     * one is not.
     */
    virtual bool is_occupied(SlotId sid) = 0;

    /*
     * Returns a pointer to the beginning of the record data associated with the
     * specified SlotId. If the specified slot is not occupied, return nullptr.
     *
     * If provided, the record_length argument will be updated with the length
     * of the record buffer returned.
     */
    virtual byte *get_record_buffer(SlotId sid, PageOffset *record_length) = 0;

    /*
     * Attempt to insert the provided record into the page. Note that for
     * non-fixed length page implementations, the length attribute of the
     * record must be set. If the insert succeeds, returns 1. If there is no
     * room in the page to accommodate the record, returns 0 without modifying
     * the page.
     */
    virtual SlotId insert_record(Record rec) = 0;

    /*
     * Open a record iterator over the page at the specified slot. If INVALID_SID is passed,
     * the iterator will be opened at the beginning of the page. If the provided sid is larger
     * than the page's max_sid, no iterator can be opened and nullptr will be returned instead.
     */
    virtual std::unique_ptr<iter::GenericIterator<Record>> start_scan(SlotId sid=0) = 0;

    /*
     * Returns the largest SlotId containing a record in the page. If the page
     * is empty, return INVALID_SID. Note that this means that, on an empty
     * page, get_min_sid() may return a larger value than get_max_sid().
     */
    virtual SlotId get_max_sid() = 0;

protected:
    /*
     * Returns a pointer to the header of this page.
     */
    PageHeaderData *get_header();

    byte *page_data;

private:
    SlotId min_slot_id = 1;
};

}
}
#endif 
