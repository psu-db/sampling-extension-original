
/*
 *
 */

#ifndef SCHEMA_H
#define SCHEMA_H

#include <memory>
#include <cstring>

#include "util/types.hpp"
#include "util/base.hpp"

#include "catalog/field.hpp"

namespace lsm { namespace catalog {

class FixedKVSchema {
public:
    FixedKVSchema(size_t key_length, size_t value_length, size_t header_length) 
    : key_length(key_length), value_length(value_length), header_length(header_length) {}

    ~FixedKVSchema() = default;

    Field get_key(const byte *record_buffer) {
        return Field(record_buffer + header_length, key_length);
    };

    Field get_val(const byte *record_buffer){
        return Field(record_buffer + key_length + header_length, value_length);
    }

    // TODO: Provide an overload that allows for a pre-allocated buffer to be provided
    //       for efficiency.
    byte *create_record(const byte *key, const byte *val) {
        PageOffset total_length = this->record_length(); 
        byte *data = new byte[total_length]();

        memcpy(data + this->header_length, key, this->key_length);
        memcpy(data + this->header_length + MAXALIGN(this->key_length), val, this->value_length);

        return data;
    }

    std::unique_ptr<byte> create_record_unique(const byte *key, const byte *val) {
        return std::unique_ptr<byte>(this->create_record(key, val));
    }

    PageOffset record_length() {
        return this->header_length + MAXALIGN(this->key_length) + MAXALIGN(this->value_length);
    }

    PageOffset key_len() {
        return this->key_length;
    }

    PageOffset val_len() {
        return this->value_length;
    }

private:
    PageOffset key_length;
    PageOffset value_length;
    PageOffset header_length;
};

}}
#endif
