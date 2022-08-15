/*
 *
 */

#ifndef H_SORTEDRUN
#define H_SORTEDRUN

#include "util/types.hpp"
#include "catalog/schema.hpp"
#include "io/record.hpp"
#include "util/global.hpp"
#include "util/iterator.hpp"
#include "util/mergeiter.hpp"
#include "util/mem.hpp"
#include "util/tombstonecache.hpp"

namespace lsm { namespace ds {

class SortedRun;

class SortedRunRecordIterator : public iter::GenericIterator<io::Record> {
public:
    SortedRunRecordIterator(const SortedRun *run, size_t record_count, size_t start_idx, size_t stop_idx);
    bool next() override;
    io::Record get_item() override;

    bool supports_rewind() override;
    iter::IteratorPosition save_position() override;
    void rewind(iter::IteratorPosition position) override;

    size_t element_count() override;
    bool supports_element_count() override;

    void end_scan() override;

    ~SortedRunRecordIterator() = default;

private:
    size_t start_idx;
    size_t stop_idx;
    ssize_t cur_idx;
    size_t record_count;
    io::Record current_record;
    const SortedRun *run;
};

class SortedRun {
    friend SortedRunRecordIterator;
public:
    static std::unique_ptr<SortedRun> create(std::unique_ptr<iter::MergeIterator> iter, size_t record_cnt, global::g_state *state, size_t tombstone_count);
    static std::unique_ptr<util::TombstoneCache> initialize(byte *buffer, std::unique_ptr<iter::MergeIterator> record_iter, 
                                                            size_t record_count, global::g_state *state);

    SortedRun(io::PagedFile *pfile, global::g_state *state);
    SortedRun(mem::aligned_buffer data_array, size_t record_count, global::g_state *state, size_t tombstone_count, std::unique_ptr<util::TombstoneCache> tombstone_cache=nullptr);

    ssize_t get_lower_bound(const byte *key);
    ssize_t get_upper_bound(const byte *key);

    catalog::KeyCmpFunc get_key_cmp();

    io::Record get_record(size_t idx) const;

    size_t record_count();
    size_t tombstone_count();

    io::Record get(const byte *key, Timestamp time=0);
    io::Record get_tombstone(const byte *key, const byte *val, Timestamp time=0);

    std::unique_ptr<iter::GenericIterator<io::Record>> start_scan();

    size_t memory_utilization();

private:
    mem::aligned_buffer data_array;
    global::g_state *state;
    size_t record_cnt;

    size_t tombstones;
    std::unique_ptr<util::TombstoneCache> tombstone_cache;

};
}}

#endif
