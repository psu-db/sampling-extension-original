/*
 *
 */

#ifndef H_MAPMEMTABLE
#define H_MAPMEMTABLE

#include <map>

#include "util/base.hpp"
#include "util/types.hpp"
#include "ds/memtable.hpp"
#include "io/record.hpp"
#include "sampling/samplerange.hpp"
#include "util/global.hpp"

namespace lsm { namespace ds {

class MapRecordIterator;

class MapMemTable {
    friend class MapRecordIterator;

public:

    MapMemTable(size_t capacity, global::g_state *state);

    /*
     * Attempts to insert a key-value pair into the memtable with a given
     * timestamp. Will return 1 on success and 0 on failure. The insert can
     * fail because the memtable is at capacity, or because a duplicate
     * key-timestamp pair already exists within it.
     */
    int insert(byte *key, byte *value, Timestamp time=0);

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
    int remove(byte *key, byte *value, Timestamp time=0);

    /*
     * Attempts to retrieve a reocrd with a given key and timestamp from the
     * memtable. Returns the record on success. Returns in invalid record on
     * failure. This can fail because the desired key-timestamp pair does
     * not appear within the memtable.
     */
    io::Record get(byte *key, Timestamp time=0);

    /*
     * Returns the maximum capacity (in records) of the memtable.
     */
    size_t get_capacity();

    /*
     * Returns the current number of stored records in the memtable
     */
    size_t get_record_count();

    /*
     * Returns true if the memtable is full, and false if it is not.
     */
    bool is_full();


    /*
     * Deletes all records from the memtable, and frees their associated
     * memory, leaving the memtable empty.
     */
    void truncate();

    /*
     * Create a sample range object over the memtable for use in drawing
     * samples.
     */
    std::unique_ptr<sampling::SampleRange> get_sample_range(byte *lower_key, byte *upper_key);

    /*
     * Returns a record iterator over the memtable that produces elements in
     * sorted order.
     */
    std::unique_ptr<iter::GenericIterator<io::Record>> start_sorted_scan();
private:
    size_t capacity;
    global::g_state *state;

    std::map<std::pair<std::vector<byte>, Timestamp>, byte*> table;

    std::vector<byte> create_key_buf(const byte *key);
    std::vector<byte> create_key_buf(io::Record record);
};

class MapRecordIterator : public iter::GenericIterator<io::Record> {
public:
    MapRecordIterator(const MapMemTable *table, global::g_state *state);
    bool next() override;
    io::Record get_item() override;
    void end_scan() override;

    void rewind(iter::IteratorPosition /*position*/) override {}
    iter::IteratorPosition save_position() override {return 0;}
    size_t element_count() override {return 0;}

    bool supports_rewind() override {return false;}
    bool supports_element_count() override {return false;}

private:
    std::map<std::pair<std::vector<byte>, Timestamp>, byte*>::const_iterator iter;
    std::map<std::pair<std::vector<byte>, Timestamp>, byte*>::const_iterator end;

    bool first_iteration;
    bool at_end;
    io::Record current_record;
    global::g_state *state;
};

}}

#endif
