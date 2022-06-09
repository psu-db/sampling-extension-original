/*
 *
 */

#include "ds/map_memtable.hpp"

namespace lsm { namespace ds {

int MapMemTable::insert(byte *key, byte *value, Timestamp time) 
{
    auto record_buffer = this->state->record_schema->create_record_raw(key, value);
    auto record = io::Record(record_buffer, this->state->record_schema->record_length(), time, false);


    auto key_buf = this->create_key_buf(record);
    std::pair<std::vector<byte>, Timestamp> memtable_record {key_buf, time};

    if (this->table.find(memtable_record) == this->table.end()) {
        this->table.insert({memtable_record, record_buffer});
        return 1;
    }

    delete record_buffer;
    return 0;
}


int MapMemTable::remove(byte *key, byte *value, Timestamp time)
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
    return this->get_record_count() < this->capacity;
}


io::Record MapMemTable::get(byte *key, Timestamp time)
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
        delete rec.second;
    }

    this->table.clear();
}


std::unique_ptr<iter::GenericIterator<io::Record>> MapMemTable::start_sorted_scan()
{
    return std::make_unique<MapRecordIterator>(this, this->state);
}

MapRecordIterator::MapRecordIterator(const MapMemTable *table, global::g_state *state)
{
    this->iter = table->table.begin();
    this->end = table->table.end();
    this->first_iteration = true;
    this->at_end = false;
    this->state = state;
    this->current_record = io::Record();
}


bool MapRecordIterator::next()
{
    if (this->at_end) {
        return false;
    }

    if (first_iteration && this->iter != this->end) {
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
