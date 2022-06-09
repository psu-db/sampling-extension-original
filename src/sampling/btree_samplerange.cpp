/*
 *
 */

#include "sampling/btree_samplerange.hpp"
#include "util/pageutils.hpp"

namespace lsm { namespace sampling {


std::unique_ptr<SampleRange> BTreeSampleRange::create(ds::StaticBTree *btree, byte *lower_key, 
                                    byte *upper_key, global::g_state *state)
{
    if (!btree) {
        return nullptr;
    }

    // obtain the page range for the given keys
    auto start_page = btree->get_lower_bound(lower_key);
    auto stop_page = btree->get_upper_bound(upper_key);

    // verify that the page range is valid
    if (stop_page.page_number < start_page.page_number || stop_page == INVALID_PID || start_page == INVALID_PID) {
        return nullptr;
    }

    byte *frame_ptr;

    // we can easily calculate the number of records based on the
    // page range, in the case of a fixed length schema.
    size_t record_count;
    if (btree->is_fixed_length()) {
        auto frid = state->cache->pin(stop_page, btree->get_pfile(), &frame_ptr);

        auto page = io::FixedlenDataPage(frame_ptr);
        auto records_per_page = page.get_record_capacity();
        auto record_for_last_page = page.get_max_sid();

        state->cache->unpin(frid);

        record_count = (stop_page.page_number - start_page.page_number - 1) * records_per_page + record_for_last_page;
    }

    return std::unique_ptr<BTreeSampleRange>(new BTreeSampleRange(btree, start_page.page_number, lower_key, stop_page.page_number, upper_key, record_count, state));
}


BTreeSampleRange::BTreeSampleRange(ds::StaticBTree *btree, PageNum start_page, byte *lower_key, PageNum stop_page, 
                     byte *upper_key, size_t record_count, global::g_state *state)
{
    this->btree = btree;
    this->start_page = start_page;
    this->stop_page = stop_page;
    this->lower_key = lower_key;
    this->upper_key = upper_key;
    this->state = state;
    this->record_count = record_count;
    this->cmp = btree->get_key_cmp();
    this->range_len = stop_page - start_page + 1;
}


io::Record BTreeSampleRange::get(FrameId *frid)
{
    auto record = this->get_random_record(frid);

    if (!frid || !record.is_valid()) {
        //fprintf(stderr, "invalid frid or record\n");
        return io::Record();
    }

    auto key = this->state->record_schema->get_key(record.get_data()).Bytes();
    auto tkey = this->state->record_schema->get_key(record.get_data()).Int64();

    // Reject if the record selected is outside of the specified key range.
    if (this->cmp(key, this->lower_key) < 0 || this->cmp(key, this->upper_key) > 0) {
        //fprintf(stderr, "%ld\t was sampled, but out of range.\n", tkey);
        this->state->cache->unpin(*frid);
        *frid = INVALID_FRID;
        return io::Record();
    }

    // Reject if the record select is deleted, or is a tombstone.
    if (this->btree->is_deleted(record.get_id()) || record.is_tombstone()) {
        //fprintf(stderr, "%ld\t was sampled, but deleted.\n", tkey);
        this->state->cache->unpin(*frid);
        *frid = INVALID_FRID;
        return io::Record();
    }

    //fprintf(stderr, "%ld\t was sampled successfully.\n", tkey);
    // Otherwise, we're good to return the record.
    return record;
}


size_t BTreeSampleRange::length()
{
    return this->record_count;
}


Record BTreeSampleRange::get_random_record(FrameId *frid)
{
    auto pnum = this->start_page + gsl_rng_uniform_int(this->state->rng, this->range_len + 1);

    byte *frame_ptr;
    *frid = this->state->cache->pin(pnum, btree->get_pfile(), &frame_ptr);
    auto page = io::wrap_page(frame_ptr);

    SlotId sid = 1 + gsl_rng_uniform_int(this->state->rng, page->get_max_sid() + 1);

    auto rec =  page->get_record(sid);

    if (!rec.is_valid()) {
        this->state->cache->unpin(*frid);
        *frid = INVALID_FRID;
        return io::Record();
    }

    rec.get_id().pid = this->btree->get_pfile()->pnum_to_pid(pnum);
    rec.get_id().sid = sid;

    return rec;
}


}}
