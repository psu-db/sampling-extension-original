/*
 *
 */

#ifndef H_MEMTABLEREFPTR
#define H_MEMTABLEREFPTR

#include "ds/memtable.hpp"

namespace lsm { namespace ds{ 

class MemtableRefPtr {
public:
    /*
     * Create a MemtableRefPtr on the provided MemoryTable. If a pin cannot be
     * obtained on the table (perhaps due to a data race), will return a
     * MemtableRefPtr set to nullptr.
     */
    MemtableRefPtr create(MemoryTable *memtable);

    /*
     * Returns a pointer reference to the underlying memtable.
     */
    MemoryTable *get();

    /*
     * Release the pin on the underlying memtable and set the
     * containing memtable pointer to nullptr.
     */
    void reset();

    MemoryTable *operator->();
    MemoryTable *operator*();

    ~MemtableRefPtr();
private:
    MemtableRefPtr(MemoryTable *memtable);
    MemoryTable *memtable;
};

}}
#endif

