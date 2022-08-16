/*
 *
 */

#include "sampling/unsortedrejection_samplerange.hpp"

namespace lsm { namespace sampling {

UnsortedRejectionSampleRange::UnsortedRejectionSampleRange(size_t tail_idx, const byte *lower_key, const byte *upper_key, global::g_state *state, ds::MemoryTable *table)
{
    this->state = state;
    this->table = table;
    this->lower_key = lower_key;
    this->upper_key = upper_key;
    this->cmp = state->record_schema->get_key_cmp();

    this->tail_idx = tail_idx;
}


io::Record UnsortedRejectionSampleRange::get(FrameId *frid)
{
    auto record = this->get_random_record();

    // This is the memtable, so frid will always be invalid as
    // no pages were pinned.
    if (frid) {
        *frid = INVALID_FRID;
    }

    if (!record.is_valid()) {
        return io::Record();
    }

    // Reject if the record select is deleted, or is a tombstone.
    if (record.is_tombstone()) {
        return io::Record();
    }

    // Verify that the record is within the specified sample range
    auto key = this->state->record_schema->get_key(record.get_data()).Bytes();
    if (this->cmp(key, this->lower_key) < 0 || this->cmp(key, this->upper_key) > 0) {
        return io::Record();
    }

    // Otherwise, we're good to return the record.
    return record;
}


PageId UnsortedRejectionSampleRange::get_page() 
{
    return INVALID_PID;
}


size_t UnsortedRejectionSampleRange::length()
{
    return this->tail_idx + 1;
}


io::Record UnsortedRejectionSampleRange::get_random_record()
{
    if (this->length() <= 0) {
        return io::Record();
    }


    auto idx = gsl_rng_uniform_int(this->state->rng, this->length());
    return this->table->get(idx);
}


bool UnsortedRejectionSampleRange::is_memtable() 
{
    return true;
}


bool UnsortedRejectionSampleRange::is_memory_resident()
{
    return true;
}

UnsortedRejectionSampleRange::~UnsortedRejectionSampleRange()
{
    if (this->table) {
        this->table->thread_unpin();
    }
}


}}
