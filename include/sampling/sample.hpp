/*
 *
 */

#ifndef H_SAMPLE
#define H_SAMPLE

#include <vector>

#include "io/record.hpp"

namespace lsm { namespace sampling {

class Sample {
public:
    Sample(size_t size);
    ~Sample();

    /*
     * Adds a record to the sample, if there is room. Returns 1 if successful,
     * and 0 if not. Note that the record passed as an argument will be
     * deep-copied, and the Sample object manage ownership of the copy. If the
     * input record is manually managed, the caller will still be responsible
     * for it after the call.
     */
    int add_record(io::Record rec);

private:
    std::vector<io::Record> records;
    size_t sample_capacity;
    size_t sample_count;
};

}}

#endif
