/*
 *
 */

#include "util/memtablerefptr.hpp"

namespace lsm { namespace ds {

MemtableRefPtr MemtableRefPtr::create(MemoryTable *table)
{
    table->thread_pin();
    return MemtableRefPtr(table);
}


MemtableRefPtr::MemtableRefPtr(MemoryTable *table)
{
    this->memtable = table;
}


MemoryTable *MemtableRefPtr::get()
{
    return this->memtable;
}


void MemtableRefPtr::reset()
{
    this->memtable = nullptr;
    this->memtable->thread_unpin();
}


MemoryTable *MemtableRefPtr::operator->()
{
    return this->memtable;
}


MemoryTable *MemtableRefPtr::operator*()
{
    return this->memtable;
}


MemtableRefPtr::~MemtableRefPtr()
{
    if (this->memtable) {
        this->memtable->thread_unpin();
    }
}

}}
