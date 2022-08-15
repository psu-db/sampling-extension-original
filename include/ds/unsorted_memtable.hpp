/*
 *
 */

#ifndef H_UNSORTEDMEMTABLE
#define H_UNSORTEDMEMTABLE

#include <atomic>

#include "util/base.hpp"
#include "util/types.hpp"
#include "util/iterator.hpp"
#include "io/record.hpp"
#include "sampling/unsortedmemtable_samplerange.hpp"
#include "ds/memtable.hpp"
#include "util/global.hpp"
#include "util/tombstonecache.hpp"
#include "util/mem.hpp"

namespace lsm { namespace ds {

class UnsortedRecordIterator;

class UnsortedMemTable : public MemoryTable {
    friend class UnsortedRecordIterator;

public:
    UnsortedMemTable(size_t capacity, global::g_state *state);

    int insert(byte *key, byte *value, Timestamp time=0, bool tombstone=false) override;
    int remove(byte *key, byte *value, Timestamp time=0) override;
    io::Record get(const byte *key, Timestamp time=0) override;

    /*
     * Attempts to retrieve a record with a given key, value, and timestamp
     * from the memtable. Returns the record on success. Returns in invalid
     * record on failure. This can fail because the desired key-timestamp pair
     * does not appear within the memtable.
     */
    bool has_tombstone(const byte *key, const byte *val, Timestamp time=0) override;

    size_t get_record_count() override;
    size_t get_capacity() override;
    bool is_full() override;

    void truncate() override;

    std::unique_ptr<sampling::SampleRange> get_sample_range(byte *lower_key, byte *upper_key) override;
    std::unique_ptr<iter::GenericIterator<io::Record>> start_sorted_scan() override;

    size_t tombstone_count() override;

    ~UnsortedMemTable() override;

private:
    mem::aligned_buffer data_array;
    size_t buffer_size;
    size_t record_cnt;
    size_t record_cap;

    std::vector<io::Record> table;
    util::TombstoneCache tombstone_cache;
    global::g_state *state;

    catalog::KeyCmpFunc key_cmp;

    size_t tombstones;

    std::atomic<size_t> current_tail;
    ssize_t find_record(const byte* key, Timestamp time);
    ssize_t get_index();
};


class UnsortedRecordIterator : public iter::GenericIterator<io::Record> {
public:
    UnsortedRecordIterator(const UnsortedMemTable *table, global::g_state *state);
    ~UnsortedRecordIterator() override = default;
    bool next() override;
    io::Record get_item() override;
    void end_scan() override;

    void rewind(iter::IteratorPosition /*position*/) override {}
    iter::IteratorPosition save_position() override {return 0;}
    size_t element_count() override {return this->sorted_records.size();}

    bool supports_rewind() override {return false;}
    bool supports_element_count() override {return true;}

private:
    struct RecordSortCmp {
        catalog::RecordCmpFunc rec_cmp;
        bool operator()(io::Record a, io::Record b) const {
            auto result = this->rec_cmp(a.get_data(), b.get_data());
            return result < 0;
        }
    };

    std::vector<io::Record> sorted_records;
    ssize_t current_index;
    global::g_state *state;
    RecordSortCmp cmp;
};

}}

#endif
