/*
 *
 */

#ifndef H_MAPMEMTABLE
#define H_MAPMEMTABLE

#include <chrono>
#include <map>

#include "util/base.hpp"
#include "util/types.hpp"
#include "ds/memtable.hpp"
#include "io/record.hpp"
#include "util/global.hpp"
#include "util/tombstonecache.hpp"

#include "ds/skiplist_core.hpp"
#include "sampling/mapmemtable_samplerange.hpp"

namespace lsm { namespace ds {

class MapRecordIterator;

class MapMemTable : public MemoryTable {
    friend class MapRecordIterator;

public:

    MapMemTable(size_t capacity, global::g_state *state);

    ~MapMemTable() override;

    /*
     * Attempts to insert a key-value pair into the memtable with a given
     * timestamp. Will return 1 on success and 0 on failure. The insert can
     * fail because the memtable is at capacity, or because a duplicate
     * key-timestamp pair already exists within it.
     */
    int insert(byte *key, byte *value, Timestamp time=0, bool tombstone=false) override;

    /*
     * Attempts to remove a key-value pair from the memtable with a 
     * given timestamp. Will return 1 on success and 0 on failure. The
     * removal can fail because the desired record doesn't exist within
     * the memtable.
     *
     * Note that this is distinct from deleting a record from the LSM Tree,
     * which can be managed elsewhere. While this operation can be used for
     * this, other methods of handling deletes, such as tombstones or
     * bitmaps, do not require the use of this function.
     */
    int remove(byte *key, byte *value, Timestamp time=0) override;

    /*
     * Attempts to retrieve a record with a given key and timestamp from the
     * memtable. Returns the record on success. Returns in invalid record on
     * failure. This can fail because the desired key-timestamp pair does
     * not appear within the memtable.
     */
    io::Record get(const byte *key, Timestamp time=0) override;

    /*
     * Get record by index.
     *
     * Not supported by this datatype. Will always return an invalid record.
     */
    io::Record get(size_t /*idx*/) override { return io::Record(); }

    /*
     * Attempts to retrieve a record with a given key, value, and timestamp
     * from the memtable. Returns the record on success. Returns in invalid
     * record on failure. This can fail because the desired key-timestamp pair
     * does not appear within the memtable.
     */
    bool has_tombstone(const byte *key, const byte *val, Timestamp time=0) override;

    /*
     * Returns the maximum capacity (in records) of the memtable.
     */
    size_t get_capacity() override;

    /*
     * Returns the current number of stored records in the memtable
     */
    size_t get_record_count() override;

    /*
     * Returns true if the memtable is full, and false if it is not.
     */
    bool is_full() override;

    /*
     * Deletes all records from the memtable, and frees their associated
     * memory, leaving the memtable empty.
     */
    bool truncate() override;

    /*
     * Create a sample range object over the memtable for use in drawing
     * samples.
     */
    std::unique_ptr<sampling::SampleRange> get_sample_range(byte *lower_key, byte *upper_key) override;

    /*
     * Create a sample range object over the memtable for use in drawing
     * samples. Using std::chrono to time the various stages.
     */
    std::unique_ptr<sampling::SampleRange> get_sample_range_bench(byte *lower_key, byte *upper_key, size_t *bounds_time, size_t *iter_time);

    /*
     * Returns a record iterator over the memtable that produces elements in
     * sorted order.
     */
    std::unique_ptr<iter::GenericIterator<io::Record>> start_sorted_scan() override;

    size_t tombstone_count() override;

    SkipList *get_table();
private:
    size_t capacity;
    global::g_state *state;
    std::unique_ptr<SkipList> table;
    size_t tombstones;

    std::unique_ptr<util::TombstoneCache> tombstone_cache;

    const byte *get_key(io::Record record);
    const byte *get_val(io::Record record);
    int insert_internal(io::Record record);
};

class MapRecordIterator : public iter::GenericIterator<io::Record> {
public:
    MapRecordIterator(SkipList::iterator begin, SkipList::iterator end, size_t record_count, global::g_state *state, ds::MemoryTable *table);
    ~MapRecordIterator() override;
    bool next() override;
    io::Record get_item() override;
    void end_scan() override;

    void rewind(iter::IteratorPosition /*position*/) override {}
    iter::IteratorPosition save_position() override {return 0;}
    size_t element_count() override {return this->element_cnt;}

    bool supports_rewind() override {return false;}
    bool supports_element_count() override {return true;}

private:
    SkipList::iterator iter;
    SkipList::iterator end;

    bool first_iteration;
    bool at_end;
    size_t element_cnt;
    io::Record current_record;
    global::g_state *state;
    MemoryTable *table;
    bool unpinned;
};

}}

#endif
