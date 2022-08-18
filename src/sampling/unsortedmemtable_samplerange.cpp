/*
 *
 */

#include "sampling/unsortedmemtable_samplerange.hpp"

namespace lsm { namespace sampling {

UnsortedMemTableSampleRange::UnsortedMemTableSampleRange(std::vector<io::CacheRecord>::const_iterator begin, std::vector<io::CacheRecord>::const_iterator end,
                                                         const byte *lower_key, const byte *upper_key, global::g_state *state, ds::MemoryTable *table)
{
    this->state = state;
    this->table = table;
    auto key_cmp = state->record_schema->get_key_cmp();
    for (auto iter=begin; iter<end; iter++) {
        io::Record rec = iter->rec;
        auto key = state->record_schema->get_key(rec.get_data()).Bytes();
        if (rec.is_valid() && key_cmp(key, lower_key) >= 0 && key_cmp(key, upper_key) <= 0) {
            this->records.push_back(rec.get_data());
        }
    }
}


UnsortedMemTableSampleRange::UnsortedMemTableSampleRange(ds::SkipList::iterator begin, ds::SkipList::iterator end,
                                                         const byte *lower_key, const byte *upper_key, global::g_state *state, ds::MemoryTable *table)
{
    this->state = state;
    this->table = table;
    auto key_cmp = state->record_schema->get_key_cmp();
    for (auto iter=std::move(begin); iter!=end; ++iter) {
        io::Record rec = {iter->second, state->record_schema->record_length()};
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

    // Otherwise, we're good to return the record.
    return record;
}


PageId UnsortedMemTableSampleRange::get_page() 
{
    return INVALID_PID;
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


bool UnsortedMemTableSampleRange::is_memtable() 
{
    return true;
}


bool UnsortedMemTableSampleRange::is_memory_resident()
{
    return true;
}

UnsortedMemTableSampleRange::~UnsortedMemTableSampleRange()
{
    if (this->table) {
        this->table->thread_unpin();
    }
}


}}
