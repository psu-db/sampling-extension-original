/*
 *
 */

#include "ds/unsorted_memtable.hpp"

namespace lsm { namespace ds {

UnsortedMemTable::UnsortedMemTable(size_t capacity, global::g_state *state, bool rejection_sampling, size_t filter_size) : data_array(nullptr, free)
{
    this->buffer_size = capacity * state->record_schema->record_length();
    this->data_array = mem::create_aligned_buffer(buffer_size);

    this->table = std::vector<io::CacheRecord>(capacity, {io::Record()});

    this->state = state;
    this->current_tail.store(0);
    this->tombstones.store(0);

    this->unsafe_tail = 0;

    this->record_cap = capacity;

    this->key_cmp = this->state->record_schema->get_key_cmp();
    this->val_cmp = this->state->record_schema->get_val_cmp();

    this->tombstone_filter = nullptr;

    if (filter_size > 0) {
        tombstone_filter = ds::BloomFilter::create_volatile(filter_size, this->state->record_schema->key_len(), 8, this->state);
    }

    this->thread_pins = 0;
    this->rejection_sampling = rejection_sampling;
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

    if (record.is_tombstone()) {
        tombstones.fetch_add(1);
        if (this->tombstone_filter) {
            tombstone_filter->insert(key);
        }
    }

    this->finalize_insertion(idx, record);

    return 1;
}


int UnsortedMemTable::remove(byte * /*key*/, byte * /*value*/, Timestamp /*time*/)
{
    return 0;
}


io::Record UnsortedMemTable::get(const byte *key, Timestamp time) 
{
    auto idx = this->find_record(key, nullptr, time);

    if (idx == -1) {
        return io::Record();
    }

    return this->table[idx].rec;
}


io::Record UnsortedMemTable::get(size_t idx) 
{
    if (idx >= this->table.size()){
        return io::Record();
    }

    return this->table[idx].rec;
}


size_t UnsortedMemTable::get_record_count()
{
    return std::min((size_t) this->current_tail.load(), this->table.size());
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
    if (this->tombstone_filter) {
        if (!this->tombstone_filter->lookup(key)) {
            return false;
        }
    }

    auto idx = this->find_record(key, val, time);

    return (idx == -1) ? false : this->table[idx].rec.is_tombstone();
}


bool UnsortedMemTable::truncate()
{
    if (this->thread_pins > 0) {
        return false;
    }

    // We need to re-zero the record vector to ensure that sampling during the
    // gap between an insert obtaining an index, and finalizing the insert,
    // doesn't return an old record. This ensures that, should this occur, the
    // resulting record will be tagged as invalid and skipped until insert
    // finalization.
    this->table = std::vector<io::CacheRecord>(this->record_cap, {io::Record()});
    
    this->current_tail.store(0);
    this->tombstones.store(0);

    this->unsafe_tail = 0;

    if (this->tombstone_filter) {
        this->tombstone_filter->clear();
    }

    return true;
}


std::unique_ptr<sampling::SampleRange> UnsortedMemTable::get_sample_range(byte *lower_key, byte *upper_key)
{
    this->thread_pin();
    if (this->rejection_sampling) {
        return std::make_unique<sampling::UnsortedRejectionSampleRange>(this->get_record_count() - 1, lower_key, upper_key, this->state, this);
    } 
    return std::make_unique<sampling::UnsortedMemTableSampleRange>(this->table.begin(), this->table.begin() + this->get_record_count(), lower_key, upper_key, this->state, this);
}


std::unique_ptr<iter::GenericIterator<io::Record>> UnsortedMemTable::start_sorted_scan()
{
    this->thread_pin();
    return std::make_unique<UnsortedRecordIterator>(this, this->state);
}


ssize_t UnsortedMemTable::find_record(const byte *key, const byte *val, Timestamp time)
{
    ssize_t current_best_match = -1;
    Timestamp current_best_time = 0;

    ssize_t upper_bound = this->get_record_count() - 1;
    for (size_t i=0; i<=(size_t)upper_bound; i++) {
        if (this->table[i].rec.get_timestamp() <= time) {
            const byte *table_key = this->state->record_schema->get_key(this->table[i].rec.get_data()).Bytes();
            if (this->key_cmp(key, table_key) == 0) {
                if (val) {
                    const byte *table_val = this->state->record_schema->get_val(this->table[i].rec.get_data()).Bytes();
                    if (this->val_cmp(val, table_val) != 0) {
                        continue;
                    }
                }

                if (this->table[i].rec.get_timestamp() >= current_best_time) {
                    current_best_time = this->table[i].rec.get_timestamp();
                    current_best_match = i;
                }
            }
        }
    }

    return current_best_match;
}


size_t UnsortedMemTable::tombstone_count() 
{
    return this->tombstones.load();
}


ssize_t UnsortedMemTable::get_index()
{
    //size_t idx = this->current_tail.load(std::memory_order_relaxed);
    //while (!this->current_tail.compare_exchange_weak(idx, idx + 1, std::memory_order_relaxed));
    
    size_t idx = this->current_tail.fetch_add(1);    

    // there is space, so return the reserved index
    if (idx < this->table.size()) {
        return idx;
    }

    // no space in the buffer
    return -1;
}


void UnsortedMemTable::finalize_insertion(size_t idx, io::Record record)
{
    this->table[idx].rec = record;
}


UnsortedRecordIterator::UnsortedRecordIterator(UnsortedMemTable *table, global::g_state *state)
{
    this->cmp.rec_cmp = state->record_schema->get_record_cmp();
    
    // Copy the records into the iterator and sort them
    this->sorted_records.resize(table->table.size());
    for (size_t i=0; i<table->table.size(); i++) {
        this->sorted_records[i] = table->table[i].rec;
    }

    std::sort(sorted_records.begin(), sorted_records.end(), this->cmp);

    this->table = table;
    this->unpinned = false;

    this->current_index = -1;
}


bool UnsortedRecordIterator::next()
{
    while ((size_t) ++this->current_index < this->element_count() && this->get_item().is_valid()) {
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
    this->table->thread_unpin();
    this->unpinned = true;
}

UnsortedRecordIterator::~UnsortedRecordIterator()
{
    if (!this->unpinned) {
        this->table->thread_unpin();
    }
}

}}
