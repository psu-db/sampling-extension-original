/*
 *
 */
#ifndef FIXEDLENDATAPAGE_H
#define FIXEDLENDATAPAGE_H

#include "io/page.hpp"
#include "io/record.hpp"

namespace lsm {
namespace io {

class FixedlenDataPage : public Page {
public:
    /*
     * Initialize an empty page buffer for use as a FixedLenDataPage, by
     * setting up the page's header information. It is assumed that the buffer
     * pointed to by page_ptr is of size PAGE_SIZE, and is either (a) already
     * zeroed, or (b) obtained by reading a page from a PagedFile that has just
     * been allocated by PagedFile::allocate.
     */
    static void initialize(byte *page_ptr, PageOffset record_length, PageOffset user_data_length=0);

    /*
     * Construct a new FixedlenDataPage object using the page buffer pointed to
     * by page_ptr. It is assume that this page buffer is of size PAGE_SIZE,
     * that it will remain in memory for the lifetime of the FixedlenDataPage
     * object, and that it has already been initialized at some point via a
     * call to FixedlenDataPage::initialize.
     */
    FixedlenDataPage(byte *page_ptr);

    ~FixedlenDataPage() = default;

    /*
     * See Page::is_occupied in io/page.hpp
     */
    bool is_occupied(SlotId sid) override;

    /*
     * See Page::get_record_buffer in io/page.hpp
     */
    byte *get_record_buffer(SlotId sid, PageOffset *record_length) override;

    /*
     * See Page::insert_record in io/page.hpp
     */
    SlotId insert_record(Record rec) override;

    /*
     * See Page::get_max_sid in io/page.hpp
     */
    SlotId get_max_sid() override;

    /*
     * See page::start_scan in io/page.hpp
     */
    std::unique_ptr<iter::GenericIterator<Record>> start_scan(SlotId sid=0) override;

    SlotId get_record_capacity();
};

class FixedlenDataPageRecordIterator : public iter::GenericIterator<Record> {
public:
    FixedlenDataPageRecordIterator(FixedlenDataPage *dpage, SlotId starting_slot);
    bool next() override;
    Record get_item() override;
    bool supports_rewind() override;
    iter::IteratorPosition save_position() override;
    void rewind(iter::IteratorPosition position) override;
    void end_scan() override;

private:
    SlotId current_slot;
    FixedlenDataPage *page;
};

}
}

#endif
