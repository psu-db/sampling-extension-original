/*
 *
 */

#include "sampling/unsortedmemtable_samplerange.hpp"

namespace lsm { namespace sampling {

UnsortedMemTableSampleRange::UnsortedMemTableSampleRange(std::vector<io::Record>::const_iterator begin, std::vector<io::Record>::const_iterator end,
                                                         const byte *lower_key, const byte *upper_key, global::g_state *state)
{
    this->state = state;
    auto iter = begin;
    auto key_cmp = state->record_schema->get_key_cmp();
    for (auto iter=begin; iter<end; iter++) {
        io::Record rec = *iter;
        auto key = state->record_schema->get_key(rec.get_data()).Bytes();
        if (key_cmp(key, lower_key) >= 0 && key_cmp(key, upper_key) <= 0) {
            this->records.push_back(rec.get_data());
        }
    }
}


io::Record UnsortedMemTableSampleRange::get(FrameId *frid)
{
    auto record = this->get_random_record();

    // This is the memtable, so frid will always be invalid as
    // no pages were pinned.
    *frid = INVALID_FRID;

    if (!record.is_valid()) {
        return io::Record();
    }

    // Reject if the record select is deleted, or is a tombstone.
    if (record.is_tombstone()) {
        return io::Record();
    }

    // Otherwise, we're good to return the record.
    return record;
}


size_t UnsortedMemTableSampleRange::length()
{
    return this->records.size();
}


io::Record UnsortedMemTableSampleRange::get_random_record()
{
    if (this->length() <= 0) {
        return io::Record();
    }


    auto idx = gsl_rng_uniform_int(this->state->rng, this->length());

    auto rec_buf = this->records[idx];
    return io::Record(rec_buf, this->state->record_schema->record_length());
}


}}
