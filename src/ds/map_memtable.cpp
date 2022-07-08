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

    sl_global_key_cmp = state->record_schema->get_key_cmp();

    this->table = std::make_unique<SkipList>();
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
    bool tomb = record.is_tombstone();
    const byte *key = this->get_key(record);

    // should be okay -- but I ought to figure out a better
    // approach to this that lets me retain the const specifier
    // on the MapKey type itself--it really should be const.
    MapKey memtable_key {const_cast<byte*>(key), time, tomb};

    auto res = this->table->insert({memtable_key, record.get_data()});
    
    return res.second;
}


const byte *MapMemTable::get_key(io::Record record) 
{
    return this->state->record_schema->get_key(record.get_data()).Bytes();
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


int MapMemTable::remove(byte *key, byte* value, Timestamp time) 
{
    return 0;
}


io::Record MapMemTable::get(const byte *key, Timestamp time)
{
    // should be okay -- but I ought to figure out a better
    // approach to this that lets me retain the const specifier
    // on the MapKey type itself--it really should be const.
    //
    // FIXME: Need to make this function not care about whether tomb is true
    // or false on a given key. That or split the tombstone lookups out.
    auto result = this->table->find(MapKey{const_cast<byte*>(key), time, false});

    if (result != this->table->end()) {
        return io::Record(result->second, this->state->record_schema->record_length());
    }

    return io::Record();
}


void MapMemTable::truncate()
{
    for (auto rec : *this->table) {
        delete[] rec.second;
    }

    this->table.reset();
    this->table = std::make_unique<SkipList>();
}


std::unique_ptr<iter::GenericIterator<io::Record>> MapMemTable::start_sorted_scan()
{
    auto start = this->table->begin();
    auto end = this->table->end();
    return std::make_unique<MapRecordIterator>(std::move(start), std::move(end), this->get_record_count(), this->state);
}


std::unique_ptr<sampling::SampleRange> MapMemTable::get_sample_range(byte *lower_key, byte *upper_key)
{
    const MapKey lower {lower_key, TIMESTAMP_MIN, true};
    const MapKey upper {upper_key, TIMESTAMP_MAX, false};


    auto start = this->table->lower_bound(lower);
    auto stop = this->table->upper_bound(upper);
    
    // the range doesn't work for the memtable
    if (start == this->table->end()) {
        return nullptr;
    }

    return std::make_unique<sampling::MapMemTableSampleRange>(std::move(start), std::move(stop), this->state);
}


SkipList *MapMemTable::get_table()
{
    return this->table.get();
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
