
/*
*
*/

#include "io/record.hpp"

namespace lsm {
namespace io {
byte *Record::create(byte *key, size_t key_len, byte *value, size_t value_len, Timestamp time, bool tombstone)
{
    size_t record_size = RecordHeaderLength + MAXALIGN(key_len) + MAXALIGN(value_len);

    byte *data = new byte[record_size];
    ((RecordHeader *) data)->is_tombstone = tombstone;
    ((RecordHeader *) data)->time = time;

    memcpy(data + RecordHeaderLength, key, key_len);
    memcpy(data + RecordHeaderLength + MAXALIGN(key_len), value, value_len);

    return data;
}


std::unique_ptr<byte> Record::create_uniq(byte *key, size_t key_len, byte *value, size_t value_len, Timestamp time, bool tombstone)
{
    return std::unique_ptr<byte>(Record::create(key, key_len, value, value_len, time, tombstone));
}

PageOffset &Record::get_length()
{
    return this->length;
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

}
}
