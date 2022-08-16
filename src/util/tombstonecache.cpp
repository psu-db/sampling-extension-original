/*
 *
 */

#include "util/tombstonecache.hpp"

namespace lsm { namespace util {

TombstoneCache::TombstoneCache(ssize_t capacity, catalog::FixedKVSchema *schema, bool locking) 
{
    this->capacity = capacity;
    this->cmp = {schema->get_key_cmp(), schema->get_val_cmp()};
    this->table =  std::multimap<TombstoneKey, Timestamp, TombstoneCmp>(this->cmp);
    this->locking = locking;
}


void TombstoneCache::insert(const byte *key, const byte *value, Timestamp time) 
{
    if (this->locking) {
        this->mut.lock();
        this->table.insert({{key, value}, time});
        this->mut.unlock();
    } else {
        this->table.insert({{key, value}, time});
    }

}


bool TombstoneCache::exists(const byte *key, const byte *value, Timestamp time)  
{
    if (this->table.size() == 0) {
        return false;
    }

    TombstoneKey search_key = {key, value};
    // FIXME: This should hook in to the tree itself to determine if a new
    // record exists with a newer but in range timestamp that overrides this
    // tombstone. This will be an issue if a record with identical key and value
    // is deleted, and then later reinserted.
    auto iter = this->table.find({key, value});
    while (iter != this->table.end() && this->cmp(iter->first, search_key) == 0) {
        if (iter->second <= time) {
            return true;
        }
        iter++;
    }

    return false;
}


void TombstoneCache::truncate()
{
    if (this->locking) {
        this->mut.lock();
        this->table.clear();
        this->mut.unlock();
    } else {
        this->table.clear();
    }
}

}}
