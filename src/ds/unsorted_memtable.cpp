/*
 *
 */

#include "ds/unsorted_memtable.hpp"

namespace lsm { namespace ds {

UnsortedMemTable::UnsortedMemTable(size_t capacity, global::g_state *state) : data_array(nullptr, free)
{
    this->buffer_size = capacity * state->record_schema->record_length();
    this->data_array = mem::create_aligned_buffer(buffer_size);

    this->table = std::vector<io::Record>(capacity);

    this->state = state;
    this->current_tail = 0;
    this->tombstones = 0;

    this->key_cmp = this->state->record_schema->get_key_cmp();
    this->tombstone_cache = util::TombstoneCache(-1, state->record_schema.get());
}


UnsortedMemTable::~UnsortedMemTable()
{
    this->truncate();
}


int UnsortedMemTable::insert(byte *key, byte *value, Timestamp time, bool tombstone)
{
    auto idx = this->get_index();

    // there is no room left for the insert
    if (idx == -1) {
        return 0;
    }

    auto rec_ptr = this->data_array.get() + (idx * this->state->record_schema->record_length());
    this->state->record_schema->create_record_at(rec_ptr, key, value);

    auto record = io::Record(rec_ptr, this->state->record_schema->record_length(), time, tombstone);

    this->table[idx] = record;

    if (record.is_tombstone()) {
        tombstones++;

        tombstone_cache.insert(key, value, time);
    }

    return 1;
}


int UnsortedMemTable::remove(byte * /*key*/, byte * /*value*/, Timestamp /*time*/)
{
    return 0;
}


io::Record UnsortedMemTable::get(const byte *key, Timestamp time) 
{
    auto idx = this->find_record(key, time);

    if (idx >= 0) {
        return this->table[idx];
    }

    return io::Record();
}


size_t UnsortedMemTable::get_record_count()
{
    return std::min((size_t) this->current_tail, this->table.size());
}


size_t UnsortedMemTable::get_capacity()
{
    return this->table.size();
}


bool UnsortedMemTable::is_full()
{
    return this->get_record_count() == this->get_capacity();
}

bool UnsortedMemTable::has_tombstone(const byte *key, const byte *val, Timestamp time)
{
    return this->tombstone_cache.exists(key, val, time);
}

void UnsortedMemTable::truncate()
{
    // FIXME: This could have some synchronization problems when we
    // implement concurrency, if we end up trying a concurrent version
    // of this memtable implementation.
    
    ssize_t upper_bound = this->get_record_count() - 1;

    // the table is empty, nothing to do
    if (upper_bound == -1) {
        return;
    }

    // reset the tail index
    this->current_tail = 0;

    this->tombstones = 0;

    this->tombstone_cache.truncate();
}


std::unique_ptr<sampling::SampleRange> UnsortedMemTable::get_sample_range(byte *lower_key, byte *upper_key)
{
    return std::make_unique<sampling::UnsortedMemTableSampleRange>(this->table.begin(), this->table.begin() + this->get_record_count(), lower_key, upper_key, this->state);
}


std::unique_ptr<iter::GenericIterator<io::Record>> UnsortedMemTable::start_sorted_scan()
{
    return std::make_unique<UnsortedRecordIterator>(this, this->state);
}


ssize_t UnsortedMemTable::find_record(const byte *key, Timestamp time)
{
    ssize_t current_best_match = -1;
    Timestamp current_best_time = 0;

    ssize_t upper_bound = this->get_record_count() - 1;
    for (size_t i=0; i<=(size_t)upper_bound; i++) {
        if (this->table[i].get_timestamp() <= time) {
            const byte *table_key = this->state->record_schema->get_key(this->table[i].get_data()).Bytes();
            if (this->key_cmp(key, table_key) == 0) {
                if (this->table[i].get_timestamp() >= current_best_time) {
                    current_best_time = this->table[i].get_timestamp();
                    current_best_match = i;
                }
            }
        }
    }

    return current_best_match;
}


size_t UnsortedMemTable::tombstone_count() 
{
    return this->tombstones;
}


ssize_t UnsortedMemTable::get_index()
{
    size_t idx = this->current_tail.fetch_add(1);    

    // there is space, so return the reserved index
    if (idx < this->table.size()) {
        return idx;
    }

    // no space in the buffer
    return -1;
}


UnsortedRecordIterator::UnsortedRecordIterator(const UnsortedMemTable *table, global::g_state *state)
{
    this->cmp.rec_cmp = state->record_schema->get_record_cmp();
    
    // Copy the records into the iterator and sort them
    this->sorted_records = table->table;
    std::sort(sorted_records.begin(), sorted_records.end(), this->cmp);

    this->current_index = -1;
}


bool UnsortedRecordIterator::next()
{
    while ((size_t) ++this->current_index < this->element_count()) {
        return true;
    }

    return false;
}


io::Record UnsortedRecordIterator::get_item()
{
    return this->sorted_records[this->current_index];
}


void UnsortedRecordIterator::end_scan()
{
    return;
}


}}
