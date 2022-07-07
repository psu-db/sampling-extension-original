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
    this->key_cmp = state->record_schema->get_key_cmp();

    sl_global_key_cmp = state->record_schema->get_key_cmp();

    cds::Initialize();
    cds::gc::hp::GarbageCollector::Construct(70);
    cds::threading::Manager::attachThread();

    this->table = std::make_unique<SkipList>();
}


MapMemTable::~MapMemTable() 
{
    this->truncate();
    cds::gc::hp::GarbageCollector::Destruct();
    cds::Terminate();
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
    MapKey memtable_key {key, time};

    return this->table->insert(memtable_key, record.get_data());
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
    auto result = this->table->get_with(MapKey{key, time}, this->cmp_less);

    if (result) {
        return io::Record(result->second, this->state->record_schema->record_length());
    }

    return io::Record();
}


void MapMemTable::truncate()
{
    for (auto rec : *this->table) {
        delete[] rec.second;
    }

    this->table->clear();
}


std::unique_ptr<iter::GenericIterator<io::Record>> MapMemTable::start_sorted_scan()
{
    return std::make_unique<MapRecordIterator>(this, this->get_record_count(), this->state);
}


std::unique_ptr<sampling::SampleRange> MapMemTable::get_sample_range(byte *lower_key, byte *upper_key)
{
    auto start = this->table->begin();
    auto stop = this->table->end();
    return std::make_unique<sampling::UnsortedMemTableSampleRange>(start, stop, lower_key, upper_key, this->state);
    /*
    const MapKey lower {lower_key, TIMESTAMP_MIN};
    const MapKey upper {upper_key, TIMESTAMP_MAX};

    return nullptr;

    auto start = std::lower_bound<SkipList::iterator, MapKey, MapCompareFuncLess>(this->table.begin(), this->table.end(), lower, this->cmp_less);
    auto stop = std::lower_bound<SkipList::iterator, MapKey, MapCompareFuncLess>(this->table.begin(), this->table.end(), upper, this->cmp_less);
    // the range doesn't work for the memtable
    if (start == this->table.end()) {
        return nullptr;
    }

    return std::make_unique<sampling::MapMemTableSampleRange>(start, stop, this->state);
    */
}


SkipList *MapMemTable::get_table()
{
    return this->table.get();
}


MapRecordIterator::MapRecordIterator(const MapMemTable *mem_table, size_t record_count, global::g_state *state)
{
    this->iter = mem_table->table->begin();
    this->end = mem_table->table->end();
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
