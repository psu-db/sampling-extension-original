/*
 *
 */

#include "util/tombstonecache.hpp"

namespace lsm { namespace util {

TombstoneCache::TombstoneCache(ssize_t capacity, catalog::FixedKVSchema *schema) 
{
    this->capacity = capacity;
    this->table =  std::multimap<TombstoneKey, Timestamp, TombstoneCmp>(TombstoneCmp{schema->get_key_cmp(), schema->get_val_cmp()});
}


void TombstoneCache::insert(const byte *key, const byte *value, Timestamp time) 
{
    this->table.insert({{key, value}, time});
}


bool TombstoneCache::exists(const byte *key, const byte *value, Timestamp time)  
{
    return this->table.find({key, value}) != this->table.end();
}


void TombstoneCache::truncate()
{
    this->table.clear();
}

}}
