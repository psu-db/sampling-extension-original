/*
 *
 */

#include "sampling/mapmemtable_samplerange.hpp"

namespace lsm { namespace sampling {

MapMemTableSampleRange::MapMemTableSampleRange(
    ds::SkipList::iterator start, ds::SkipList::iterator stop, global::g_state *state)
{
    this->state = state;

    while (start != stop) {
        this->records.push_back(start->second);
        ++start;
    }
}


io::Record MapMemTableSampleRange::get(FrameId *frid)
{
    auto record = this->get_random_record();
    *frid = INVALID_FRID;

    if (!record.is_valid()) {
        //fprintf(stderr, "invalid frid or record\n");
        return io::Record();
    }

    // Reject if the record select is deleted, or is a tombstone.
    if (record.is_tombstone()) {
        //fprintf(stderr, "%ld\t was sampled, but deleted.\n", tkey);
        this->state->cache->unpin(*frid);
        return io::Record();
    }

    //fprintf(stderr, "%ld\t was sampled successfully.\n", tkey);
    // Otherwise, we're good to return the record.
    return record;
}


size_t MapMemTableSampleRange::length()
{
    return this->records.size();
}


io::Record MapMemTableSampleRange::get_random_record()
{
    if (this->length() <= 0) {
        return io::Record();
    }


    auto idx = gsl_rng_uniform_int(this->state->rng, this->length());

    auto rec_buf = this->records[idx];
    return io::Record(rec_buf, this->state->record_schema->record_length());
}


}}
