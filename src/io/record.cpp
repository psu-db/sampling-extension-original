/*
 * record.cpp
 * Douglas Rumbaugh
 *
 * Record implementation
 */

#include "io/record.hpp"

namespace lsm { namespace io {

Record::Record(byte *data, size_t len, Timestamp time, bool tombstone)
{
    this->data_ref = data;
    this->length = len;

    this->get_header()->time = time;
    this->get_header()->is_tombstone = tombstone;
}


PageOffset &Record::get_length()
{
    return this->length;
}


RecordId &Record::get_id()
{
    return this->rid;
}


byte *&Record::get_data()
{
    return this->data_ref;
}


RecordHeader *Record::get_header()
{
    return (RecordHeader *) this->data_ref;
}


Timestamp Record::get_timestamp()
{
    return this->get_header()->time;
}


bool Record::is_tombstone()
{
    return this->get_header()->is_tombstone;
}


bool Record::is_valid()
{
    return !(this->length == 0 || this->data_ref == nullptr);
}


void Record::free_data()
{
    if (this->data_ref) {
        delete this->get_data();
    }

    this->data_ref = nullptr;
    this->length = 0;
}


Record Record::deep_copy()
{
    byte *new_buf = new byte[this->length]();
    memcpy(new_buf, this->data_ref, this->length);

    return Record(new_buf, this->length);
}

}}
