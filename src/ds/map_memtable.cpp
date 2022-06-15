/*
 *
 */

#include "ds/map_memtable.hpp"

namespace lsm { namespace ds {

MapMemTable::MapMemTable(size_t capacity, global::g_state *state)
{
    this->capacity = capacity;
    this->state = state;
    this->rec_cmp = state->record_schema->get_key_cmp();
    this->cmp = MapCompareFunc{this->rec_cmp};

    this->table = std::map<std::pair<std::vector<byte>, Timestamp>, byte*, MapCompareFunc>(this->cmp);
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

    auto res = this->insert_internal(record);

    if (res == 1) {
        return 1;
    }

    delete[] record_buffer;
    return 0;
}


int MapMemTable::insert_internal(io::Record record)
{
    auto time = record.get_timestamp();
    auto key_buf = this->create_key_buf(record);
    std::pair<std::vector<byte>, Timestamp> memtable_key {key_buf, time};

    auto existing_record = this->table.find(memtable_key);

    if (existing_record == this->table.end()) {
        this->table.insert({memtable_key, record.get_data()});
        return 1;
    } else if (record.is_tombstone()) {
        // we'll force insert tombstones over any duplicate key objections,
        // replacing the existing record.
        // TODO: Ensure that the value matches up. I need to extend my
        // comparator function set for this.
        this->table.erase(existing_record);
        this->table.insert({memtable_key, record.get_data()});
        return 1;
    } else if (io::Record(existing_record->second, this->state->record_schema->record_length()).is_tombstone() && !record.is_tombstone()) {
        // If the record we found is a tombstone we'll replace it with the new record.
        this->table.erase(existing_record);
        this->table.insert({memtable_key, record.get_data()});
        return 1;
    }

    return 0;
}


int MapMemTable::remove(byte *key, byte* /*value*/, Timestamp time)
{
    auto key_buf = this->create_key_buf(key);
    auto result = this->table.find({key_buf, time});

    if (result == this->table.end()) {
        return 0;
    }

    auto rec = io::Record(result->second, this->state->record_schema->record_length());

    // TODO: compare values too. I may need another comparator for this.

    // if the value matches, {
    //     delete result.second;
    //     this->table.erase({key_buf, time});
    //     return 1;
    // }

    return 0;
}


std::vector<byte> MapMemTable::create_key_buf(const byte *key)
{
    std::vector<byte> key_bytes;
    key_bytes.insert(key_bytes.end(), key, key + this->state->record_schema->key_len());

    return key_bytes;
}


std::vector<byte> MapMemTable::create_key_buf(io::Record record) 
{
    return this->create_key_buf(this->state->record_schema->get_key(record.get_data()).Bytes());
}


size_t MapMemTable::get_record_count()
{
    return this->table.size();
}


size_t MapMemTable::get_capacity()
{
    return this->capacity;
}


bool MapMemTable::is_full()
{
    return this->get_record_count() >= this->capacity;
}


io::Record MapMemTable::get(const byte *key, Timestamp time)
{
    // TODO: This should return the first record with a timestamp less than
    // or equal to the specified one. Need to dig into the interface for map
    // to see how that sort of thing could be done. For now, the time is always
    // 0 anyway.
    auto key_buf = this->create_key_buf(key);
    auto result = this->table.find({key_buf, time});

    if (result != this->table.end()) {
        return io::Record(result->second, this->state->record_schema->record_length());
    }

    return io::Record();
}


void MapMemTable::truncate()
{
    for (auto rec : this->table) {
        delete[] rec.second;
    }

    this->table.clear();
}


std::unique_ptr<iter::GenericIterator<io::Record>> MapMemTable::start_sorted_scan()
{
    return std::make_unique<MapRecordIterator>(this, this->get_record_count(), this->state);
}


std::unique_ptr<sampling::SampleRange> MapMemTable::get_sample_range(byte *lower_key, byte *upper_key)
{
    std::vector<byte> lower_key_bytes;
    lower_key_bytes.insert(lower_key_bytes.end(), lower_key, lower_key + state->record_schema->key_len());

    std::vector<byte> upper_key_bytes;
    upper_key_bytes.insert(upper_key_bytes.end(), upper_key, upper_key + state->record_schema->key_len());

    auto start = this->table.lower_bound(std::pair(lower_key_bytes, TIMESTAMP_MIN));
    auto stop = this->table.upper_bound(std::pair(upper_key_bytes, TIMESTAMP_MAX));

    // the range doesn't work for the memtable
    if (start == this->table.end()) {
        return nullptr;
    }

    return std::make_unique<sampling::MapMemTableSampleRange>(start, stop, this->state);
}


std::map<std::pair<std::vector<byte>, Timestamp>, byte*, MapCompareFunc> *MapMemTable::get_table()
{
    return &this->table;
}


MapRecordIterator::MapRecordIterator(const MapMemTable *table, size_t record_count, global::g_state *state)
{
    this->iter = table->table.begin();
    this->end = table->table.end();
    this->first_iteration = true;
    this->at_end = false;
    this->state = state;
    this->current_record = io::Record();
    this->element_cnt = record_count;
}


bool MapRecordIterator::next()
{
    if (this->at_end) {
        return false;
    }

    if (first_iteration && this->iter != this->end) {
        first_iteration = false;
        this->current_record = io::Record(this->iter->second, this->state->record_schema->record_length());
        return true;
    }

    while (++this->iter != this->end) {
        this->current_record = io::Record(this->iter->second, this->state->record_schema->record_length());
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

}



}}
