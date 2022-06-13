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

    /*
     * Returns a record from the sample, by index. If the specified index
     * exceeds the sample size (or if the sample is empty), the returned
     * record will be invalid. 
     *
     * The data reference for the returned record will refer to the Sample's
     * copy of the record data, and should not be freed. The pointer will
     * remain valid for the lifetime of the Sample object, only.
     */
    io::Record get(size_t index);

    /*
     * Returns the number of elements within the sample.
     */
    size_t sample_size();

private:
    std::vector<io::Record> records;
    size_t record_capacity;
    size_t record_count;
};

}}

#endif
