/*
 *
 */

#include "sampling/mapmemtable_samplerange.hpp"

namespace lsm { namespace sampling {


/*
std::unique_ptr<SampleRange> MapMemTableSampleRange::create(ds::MapMemTable *table, byte *lower_key, 
                                    byte *upper_key, global::g_state *state)
{
    std::vector<byte> lower_key_bytes;
    lower_key_bytes.insert(lower_key_bytes.end(), lower_key, lower_key + state->record_schema->key_len());

    std::vector<byte> upper_key_bytes;
    upper_key_bytes.insert(upper_key_bytes.end(), upper_key, upper_key + state->record_schema->key_len());

    auto start = table->get_table()->lower_bound(std::pair(lower_key_bytes, TIMESTAMP_MIN));
    auto stop = table->get_table()->upper_bound(std::pair(upper_key_bytes, TIMESTAMP_MAX));

    // the range doesn't work for the memtable
    if (start == table->get_table()->end() || stop == table->get_table()->end()) {
        return nullptr;
    }

}
*/


MapMemTableSampleRange::MapMemTableSampleRange(
    std::map<std::pair<std::vector<byte>, Timestamp>, byte*>::const_iterator start,
    std::map<std::pair<std::vector<byte>, Timestamp>, byte*>::const_iterator stop,
    global::g_state *state)
{
    this->state = state;

    while (start != stop) {
        this->records.push_back(start->second);
        start++;
    }
}


io::Record MapMemTableSampleRange::get(FrameId *frid)
{
    auto record = this->get_random_record();

    if (!record.is_valid()) {
        //fprintf(stderr, "invalid frid or record\n");
        return io::Record();
    }

    auto key = this->state->record_schema->get_key(record.get_data()).Bytes();
    auto tkey = this->state->record_schema->get_key(record.get_data()).Int64();

    // Reject if the record select is deleted, or is a tombstone.
    if (record.is_tombstone()) {
        //fprintf(stderr, "%ld\t was sampled, but deleted.\n", tkey);
        this->state->cache->unpin(*frid);
        *frid = INVALID_FRID;
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
