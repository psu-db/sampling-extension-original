/*
 *
 */

#ifndef H_MEMTABLE
#define H_MEMTABLE

#include "util/base.hpp"
#include "util/types.hpp"
#include "util/iterator.hpp"
#include "io/record.hpp"
#include "sampling/samplerange.hpp"

namespace lsm { namespace ds {

class MemoryTable {
public:
    virtual int insert(byte *key, byte *value, Timestamp time=0, bool tombstone=false) = 0;
    virtual int remove(byte *key, byte *value, Timestamp time=0) = 0;
    virtual io::Record get(const byte *key, Timestamp time=0) = 0;
    virtual io::Record get(const byte *key, const byte* /* val */, Timestamp time=0) {
        return this->get(key, time);
    }

    virtual size_t get_record_count() = 0;
    virtual size_t get_capacity() = 0;
    virtual bool is_full() = 0;

    virtual void truncate() = 0;

    virtual std::unique_ptr<sampling::SampleRange> get_sample_range(byte *lower_key, byte *upper_key) = 0;
    virtual std::unique_ptr<iter::GenericIterator<io::Record>> start_sorted_scan() = 0;

    virtual ~MemoryTable() = default;

    virtual size_t tombstone_count() = 0;
private:
};

}}

#endif
