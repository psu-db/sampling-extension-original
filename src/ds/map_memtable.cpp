/*
 *
 */

#include "ds/map_memtable.hpp"

namespace lsm { namespace ds {

catalog::KeyCmpFunc sl_global_key_cmp;

MapMemTable::MapMemTable(size_t capacity, global::g_state *state)
{
    this->capacity = capacity;
    this->state = state;
    this->tombstones = 0;

    sl_global_key_cmp = state->record_schema->get_key_cmp();

    this->table = std::make_unique<SkipList>();
    this->tombstone_table = std::multimap<TombstoneKey, Timestamp, tombstone_cmp>(tombstone_cmp{state->record_schema->get_key_cmp(), state->record_schema->get_val_cmp()});
}


MapMemTable::~MapMemTable() 
{
    this->truncate();
}


int MapMemTable::insert(byte *key, byte *value, Timestamp time, bool tombstone) 
{
    if (this->is_full()) {
        return 0;
    }

    auto record_buffer = this->state->record_schema->create_record_raw(key, value);
    auto record = io::Record(record_buffer, this->state->record_schema->record_length(), time, tombstone);

    if (this->insert_internal(record)) {
        return 1;
    }

    delete[] record_buffer;
    return 0;
}


int MapMemTable::insert_internal(io::Record record)
{
    Timestamp time = record.get_timestamp();
    const byte *key = this->get_key(record);
    const byte *val = this->get_val(record);
    bool tomb = record.is_tombstone();

    // should be okay -- but I ought to figure out a better
    // approach to this that lets me retain the const specifier
    // on the MapKey type itself--it really should be const.
    MapKey memtable_key {const_cast<byte*>(key), time};

    auto res = this->table->insert({memtable_key, record.get_data()});

    if (res.second && tomb) {
        TombstoneKey tomb_key = {key, val};
        this->tombstones++;
        this->tombstone_table.insert({tomb_key, time});
    }
    
    return res.second;
}


const byte *MapMemTable::get_key(io::Record record) 
{
    return this->state->record_schema->get_key(record.get_data()).Bytes();
}


const byte *MapMemTable::get_val(io::Record record) 
{
    return this->state->record_schema->get_val(record.get_data()).Bytes();
}


size_t MapMemTable::get_record_count()
{
    return this->table->size();
}


size_t MapMemTable::get_capacity()
{
    return this->capacity;
}


bool MapMemTable::is_full()
{
    return this->get_record_count() >= this->capacity;
}


int MapMemTable::remove(byte * /*key*/, byte* /*value*/, Timestamp /*time*/) 
{
    return 0;
}


io::Record MapMemTable::get(const byte *key, Timestamp time)
{
    // should be okay -- but I ought to figure out a better approach to this
    // that lets me retain the const specifier on the MapKey type itself--it
    // really should be const.
    auto result = this->table->upper_bound(MapKey{const_cast<byte*>(key), time});

    if (result != this->table->end() && this->state->record_schema->get_key_cmp()(key, result->first.key) == 0
    && result->first.time <= time) {
        return io::Record(result->second, this->state->record_schema->record_length());
    }

    return io::Record();
}


bool MapMemTable::has_tombstone(const byte *key, const byte *val, Timestamp time)
{
    // FIXME: this is not fully functional just yet. Namely, we need to account
    // still for scanning the full set of cached matches to get the newest
    // timestamp associated with the tombstone, and then we need to do a lookup
    // against the memory table to ensure that the newest tombstone isn't
    // superseded by an active record. This shouldn't happen in any current
    // benchmarks, so we'll do a simple point lookup for now.
    //
    // This would only crop up if we were to update a record, and then update
    // it back to its original configuration prior to the memtable merging.
    // Once the records are within the on-disk structure, the above situation
    // should be automatically handled correctly.

    return this->tombstone_table.find({key, val}) != this->tombstone_table.end();
}


void MapMemTable::truncate()
{
    for (auto rec : *this->table) {
        delete[] rec.second;
    }

    this->tombstone_table.clear();
    this->table.reset();
    this->table = std::make_unique<SkipList>();
    this->tombstones = 0;
}


std::unique_ptr<iter::GenericIterator<io::Record>> MapMemTable::start_sorted_scan()
{
    auto start = this->table->begin();
    auto end = this->table->end();
    return std::make_unique<MapRecordIterator>(std::move(start), std::move(end), this->get_record_count(), this->state);
}


std::unique_ptr<sampling::SampleRange> MapMemTable::get_sample_range(byte *lower_key, byte *upper_key)
{
    const MapKey lower {lower_key, TIMESTAMP_MIN};
    const MapKey upper {upper_key, TIMESTAMP_MAX};


    auto start = this->table->lower_bound(lower);
    auto stop = this->table->upper_bound(upper);
    auto end = this->table->end();
    
    // the range doesn't work for the memtable
    if (start == this->table->end()) {
        return nullptr;
    }

    return std::make_unique<sampling::MapMemTableSampleRange>(std::move(start), std::move(stop), std::move(end), this->state);
}


std::unique_ptr<sampling::SampleRange> MapMemTable::get_sample_range_bench(byte *lower_key, byte *upper_key, size_t *bounds_time, size_t *iter_time)
{
    const MapKey lower {lower_key, TIMESTAMP_MIN};
    const MapKey upper {upper_key, TIMESTAMP_MAX};

    auto bounds_start = std::chrono::high_resolution_clock::now();
    auto start = this->table->lower_bound(lower);
    auto stop = this->table->upper_bound(upper);
    auto bounds_stop = std::chrono::high_resolution_clock::now();
    
    auto end = this->table->end();

    // the range doesn't work for the memtable
    if (start == this->table->end()) {
        return nullptr;
    }

    auto iter_start = std::chrono::high_resolution_clock::now();
    auto range = new sampling::MapMemTableSampleRange(std::move(start), std::move(stop), std::move(end), this->state);
    auto iter_stop = std::chrono::high_resolution_clock::now();

    *bounds_time = std::chrono::duration_cast<std::chrono::nanoseconds>(bounds_stop - bounds_start).count();
    *iter_time = std::chrono::duration_cast<std::chrono::nanoseconds>(iter_stop - iter_start).count();

    return std::unique_ptr<sampling::SampleRange>(range);
}


SkipList *MapMemTable::get_table()
{
    return this->table.get();
}


size_t MapMemTable::tombstone_count()
{
    return this->tombstones;
}


MapRecordIterator::MapRecordIterator(SkipList::iterator begin, SkipList::iterator end, size_t record_count, global::g_state *state)
    : iter(std::move(begin)), end(std::move(end))
{
    this->at_end = (this->iter == this->end);

    this->state = state;
    this->current_record = io::Record();
    this->element_cnt = record_count;
}


bool MapRecordIterator::next()
{
    if (this->at_end) {
        return false;
    }

    while (this->iter != this->end) {
        this->current_record = io::Record(this->iter->second, this->state->record_schema->record_length());
        this->iter++;
        return true;
    }

    this->at_end = true;
    return false;
}


io::Record MapRecordIterator::get_item()
{
    return this->current_record;
}


void MapRecordIterator::end_scan()
{
    // this releases the block on any node pointed to by the iterator.
    this->iter = this->end;
}



}}
