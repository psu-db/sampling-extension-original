
/*
*
*/

#include "io/record.hpp"

namespace lsm {
namespace io {
byte *Record::create(byte *key, size_t key_len, byte *value, size_t value_len, Timestamp time, bool tombstone, size_t *reclen)
{
    size_t record_size = RecordHeaderLength + MAXALIGN(key_len) + MAXALIGN(value_len);

    byte *data = new byte[record_size];
    memset(data, 0, record_size);
    ((RecordHeader *) data)->is_tombstone = tombstone;
    ((RecordHeader *) data)->time = time;

    memcpy(data + RecordHeaderLength, key, key_len);
    memcpy(data + RecordHeaderLength + MAXALIGN(key_len), value, value_len);

    if (reclen) {
        *reclen = record_size;
    }

    return data;
}


std::unique_ptr<byte> Record::create_uniq(byte *key, size_t key_len, byte *value, size_t value_len, Timestamp time, bool tombstone, size_t *reclen)
{
    return std::unique_ptr<byte>(Record::create(key, key_len, value, value_len, time, tombstone, reclen));
}


PageOffset &Record::get_length()
{
    return this->length;
}


byte *&Record::get_data()
{
    return this->data_ref;
}


byte *Record::get_key()
{
    return (this->data_ref + RecordHeaderLength);
}


byte *Record::get_value()
{
    return (this->data_ref + RecordHeaderLength + this->key_length);
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
    size_t val_len = this->length - this->key_length - RecordHeaderLength;
    auto new_buf = create(this->get_key(), this->key_length, this->get_value(), val_len, this->get_timestamp(), this->is_tombstone());

    return Record(new_buf, this->length, this->key_length);
}

}
}
