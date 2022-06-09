/*
 *
 */

#ifndef H_MEMTABLE
#define H_MEMTABLE

namespace lsm { namespace ds {

#include "util/base.hpp"
#include "util/types.hpp"
#include "util/iterator.hpp"
#include "io/record.hpp"
#include "sampling/samplerange.hpp"

class MemoryTable {
public:
    int insert(byte *key, byte *value, Timestamp time=0);
    int remove(byte *key, byte *value, Timestamp time=0);
    io::Record get(byte *key, Timestamp time=0);

    size_t get_capacity();
    bool is_full();

    void truncate();

    std::unique_ptr<sampling::SampleRange> get_sample_range(byte *lower_key, byte *upper_key);
    std::unique_ptr<iter::GenericIterator<io::Record>> start_sorted_scan();

private:
};

}}

#endif
