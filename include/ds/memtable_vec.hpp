/*
 *
 */

#ifndef H_MEMTABLE_VEC
#define H_MEMTABLE_VEC

#include <mutex>
#include <atomic>

#include "ds/memtable.hpp"
#include "ds/map_memtable.hpp"
#include "ds/unsorted_memtable.hpp"

#include "sampling/samplerange.hpp"
#include "util/global.hpp"
#include "util/flags.hpp"
#include "io/record.hpp"
#include "util/memtablerefptr.hpp"


namespace lsm { namespace ds {

enum MemtableType {
    MTT_SKIPLIST,
    MTT_UNSORTED,
    MTT_UNSORT_REJ
};

enum MemtableStatus {
    MTS_ACTIVE, // the memtable is actively processing inserts
    MTS_MERGING, // the memtable is being merged by another thread
    MTS_EMPTY,   // the memtable is currently unused and has been truncated
    MTS_RETAINED // the memtable is an old version, retained due to active pins
};

constexpr int64_t F_MTV_NONE = F_FLAG0;

struct MemtableVecElem {
    std::unique_ptr<MemoryTable> table;
    MemtableStatus stat;
    Timestamp merging_timestamp;
};

typedef std::unique_ptr<MemtableVecElem> ElemPtr;

/*
 * A protected, non-owning reference to a memtable object. A pin is held on the
 * referenced memtable for the lifetime of this object, preventing truncation
 * of the table. The pin is released automatically on object destruction.
 */
class MemtableVector {
friend class MemtablePtr;

public:
    MemtableVector(MemtableType type, size_t memtable_cnt, size_t memtable_capacity, size_t filter_size, global::g_state *state, int64_t flags=F_MTV_NONE);

    /*
     * Attempts to insert a new record with the specified parameters into the
     * currently active memtable within the vector. If no memtable is active,
     * or if the active memtable is full, will return 0. Will return 1 if the
     * record is successfully inserted. The record will, at most, be inserted
     * into 1 memtable.
     */
    int insert(const byte *key, const byte *val, Timestamp time, bool tombstone=false);

    /*
     * Returns a vector of sample ranges over each currently ACTIVE or MERGING memtables. If
     * no memtables are ACTIVE or MERGING, or none of the memtables matches the key range, will
     * return an empty vector.
     */
    std::vector<std::unique_ptr<sampling::SampleRange>> get_sample_ranges(const byte *lower_key, const byte *upper_key);

    /*
     * Returns the first record found within ACTIVE or MERGING memtables with
     * the specified key, that has a timestamp less than or equal to the
     * specified timestamp. If multiple memtables are searched, it will be done
     * in reverse chronological order--the ACTIVE table will be searched first,
     * then any MERGING tables will be searched in descending timestamp order.
     *
     * If no match is found, returns an invalid record.
     */
    io::Record get(const byte* key, Timestamp time);

    /*
     *
     */
    bool has_tombstone(const byte* key, const byte* val, Timestamp time);

    /*
     * Returns a protected reference to the specified memtable within the
     * vector. The table in question will be pinned, and thus protected from
     * truncation until the returned reference is destructed.
     */
    MemtableRefPtr get_table(size_t idx);

    /*
     * Returns an unprotected reference to the specified memtable within
     * the vector. The table in question will not be pinned, and so may be
     * truncated during the lifetime of the returned reference.
     */
    MemoryTable *get_table_unsafe(size_t idx);
private:
    std::vector<ElemPtr> memtables;
    std::atomic<ssize_t> active_idx;

    size_t memtable_cap;
    global::g_state *state;

    void pin_table(size_t idx);
    void unpin_table(size_t idx);
};



}}

#endif
