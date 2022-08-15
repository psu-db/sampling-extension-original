/*
 *
 */

#include "sampling/sorted_samplerange.hpp"

namespace lsm { namespace sampling {


std::unique_ptr<SampleRange> SortedSampleRange::create(ds::SortedRun *run, byte *lower_key, 
                                    byte *upper_key, global::g_state *state)
{
    if (!run) {
        return nullptr;
    }

    // obtain the idx range for the given keys
    auto start_idx = run->get_lower_bound(lower_key);
    auto stop_idx = run->get_upper_bound(upper_key);

    // verify that the idx range is valid
    if (stop_idx < start_idx) {
        return nullptr;
    }

    // we can easily calculate the number of records based on the index range
    size_t record_count = stop_idx - start_idx;

    return std::unique_ptr<SortedSampleRange>(new SortedSampleRange(run, start_idx, lower_key, stop_idx, upper_key, record_count, state));
}


SortedSampleRange::SortedSampleRange(ds::SortedRun *run, size_t start_idx, byte *lower_key, size_t stop_idx, 
                     byte *upper_key, size_t record_count, global::g_state *state)
{
    this->run = run;
    this->start_idx = start_idx;
    this->stop_idx = stop_idx;
    this->lower_key = lower_key;
    this->upper_key = upper_key;
    this->state = state;
    this->record_count = record_count;
    this->cmp = run->get_key_cmp();
    this->range_len = stop_idx - start_idx + 1;
}


io::Record SortedSampleRange::get(FrameId *frid)
{
    auto record = this->get_random_record(frid);

    if (frid) {
        *frid = INVALID_FRID;
    }

    // Reject if the record is invalid or a tombstone
    if (!record.is_valid() || record.is_tombstone()) {
        return io::Record();
    }

    auto key = this->state->record_schema->get_key(record.get_data()).Bytes();

    // Reject if the record selected is outside of the specified key range.
    if (this->cmp(key, this->lower_key) < 0 || this->cmp(key, this->upper_key) > 0) {
        return io::Record();
    }

    // Otherwise, we're good to return the record.
    return record;
}


PageId SortedSampleRange::get_page()
{
    return INVALID_PID;
}


size_t SortedSampleRange::length()
{
    return this->record_count;
}


io::Record SortedSampleRange::get_random_record(FrameId *frid)
{
    if (frid) {
        *frid = INVALID_FRID;
    }

    auto idx = this->start_idx + gsl_rng_uniform_int(this->state->rng, this->range_len);
    return this->run->get_record(idx);
}

bool SortedSampleRange::is_memtable() 
{
    return false;
}


bool SortedSampleRange::is_memory_resident() 
{
    return true;
}

}}
