/*
 *
 */

#include "ds/sortedrun.hpp"

namespace lsm { namespace ds {

std::unique_ptr<SortedRun> SortedRun::create(std::unique_ptr<iter::MergeIterator> iter, size_t record_cnt, bool bloom_filters, global::g_state *state, size_t tombstone_count)
{
    return nullptr;
}

SortedRun::SortedRun(io::PagedFile *pfile, global::g_state *state)
{
    this->state = state;
}

size_t SortedRun::get_lower_bound(const byte *key)
{
    return 0;
}


size_t SortedRun::get_upper_bound(const byte *key)
{
    return 0;
}


catalog::KeyCmpFunc SortedRun::get_key_cmp()
{
    return this->state->record_schema->get_key_cmp();
}


io::Record SortedRun::get_record(size_t idx)
{
    if (idx < this->record_cnt) {
        size_t len = this->state->record_schema->record_length();
        byte *data = this->data_array.get() + idx * len;
        return io::Record(data, len);
    }

    return io::Record();
}


io::Record SortedRun::get(const byte *key, Timestamp time) 
{
    return io::Record();
}


io::Record SortedRun::get_tombstone(const byte *key, const byte *val, Timestamp time)
{
    return io::Record();
}


size_t SortedRun::memory_utilization()
{
    return 0;
}


std::unique_ptr<iter::GenericIterator<io::Record>> SortedRun::start_scan()
{
    return nullptr;
}


size_t SortedRun::record_count()
{
    return this->record_count();
}


size_t SortedRun::tombstone_count()
{
    return this->tombstones;
}

}}
