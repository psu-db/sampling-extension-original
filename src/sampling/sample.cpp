/*
 *
 */

#include "sampling/sample.hpp"

namespace lsm { namespace sampling {

Sample::Sample(size_t sample_size)
{
    this->record_capacity = sample_size;
    this->records = std::vector<io::Record>(this->record_capacity);
    this->record_count = 0;
}


Sample::~Sample()
{
    for (size_t i=0; i<this->record_count; i++) {
        this->records[i].free_data();
    }
}


int Sample::add_record(io::Record rec)
{
    if (this->record_count < this->record_capacity) {
        this->records[this->record_count++] = rec.deep_copy();
        return 1;
    }

    return 0;
}


size_t Sample::sample_size()
{
    return this->record_count;
}


io::Record Sample::get(size_t idx) 
{
    if (idx < this->records.size()) {
        return this->records[idx];
    }

    return io::Record();
}

}}
