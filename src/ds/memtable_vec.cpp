/*
 *
 */

#include "ds/memtable_vec.hpp"

namespace lsm { namespace ds {
MemtableVector::MemtableVector(MemtableType type, size_t memtable_cnt, size_t memtable_capacity, size_t filter_size, global::g_state *state, int64_t flags)
{
    this->memtables.resize(memtable_cnt);

    for (size_t i=0; i<memtables.size(); i++) {
        MemoryTable *table;

        switch (type) {
            case MTT_SKIPLIST:
                table = new MapMemTable(memtable_capacity, filter_size, state); 
                break;
            case MTT_UNSORTED:
                table = new UnsortedMemTable(memtable_capacity, state, false, filter_size);
                break;
            case MTT_UNSORT_REJ:
                table = new UnsortedMemTable(memtable_capacity, state, true, filter_size);
                break;
        }
    }

    this->memtable_cap = memtable_cnt;
    this->state = state;
    this->active_idx.store(0);
}


}}
