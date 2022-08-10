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

namespace lsm { namespace ds {

class SortedRun {

public:
    static std::unique_ptr<SortedRun> create(std::unique_ptr<iter::MergeIterator> iter, size_t record_cnt, bool bloom_filters, global::g_state *state, size_t tombstone_count);

    SortedRun(io::PagedFile *pfile, global::g_state *state);

    size_t get_lower_bound(const byte *key);
    size_t get_upper_bound(const byte *key);

    catalog::KeyCmpFunc get_key_cmp();

    io::Record get_record(size_t idx);

    size_t record_count();
    size_t tombstone_count();

    io::Record get(const byte *key, Timestamp time);
    io::Record get_tombstone(const byte *key, const byte *val, Timestamp time);

    std::unique_ptr<iter::GenericIterator<io::Record>> start_scan();

    size_t memory_utilization();

private:
    std::unique_ptr<byte> data_array;
    global::g_state *state;
    size_t record_cnt;
    size_t record_cap;

    size_t tombstones;

};

}}

#endif
