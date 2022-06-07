/*
 *
 */

#include "sampling/sample.hpp"

namespace lsm { namespace sampling {

Sample::Sample(size_t sample_size)
{
    this->sample_capacity = sample_size;
    this->records = std::vector<io::Record>(this->sample_capacity);
    this->sample_count = 0;
}


Sample::~Sample()
{
    for (size_t i=0; i<this->sample_count; i++) {
        this->records[i].free_data();
    }
}


int Sample::add_record(io::Record rec)
{
    if (sample_count < sample_capacity) {
        this->records[sample_count++] = rec.deep_copy();
        return 1;
    }

    return 0;
}

}}
