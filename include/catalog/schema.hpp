
/*
 *
 */

#ifndef SCHEMA_H
#define SCHEMA_H

#include <memory>
#include <cstring>
#include <functional>

#include "util/types.hpp"
#include "util/base.hpp"

#include "catalog/field.hpp"

namespace lsm { namespace catalog {

typedef std::function<int(const byte* item1, const byte* item2)> RecordCmpFunc;
typedef std::function<int(const byte* item1, const byte* item2)> KeyCmpFunc;
typedef std::function<int(const byte* item1, const byte* item2)> ValCmpFunc;

class FixedKVSchema {
public:
    FixedKVSchema(size_t key_length, size_t value_length, size_t header_length) 
    : key_length(key_length), value_length(value_length),
        header_length(header_length), has_cmps(false) {}


    FixedKVSchema(size_t key_length, size_t value_length, size_t header_length, KeyCmpFunc key_cmp, ValCmpFunc val_cmp, RecordCmpFunc record_cmp) 
    : key_length(key_length), value_length(value_length),
        header_length(header_length), key_cmp(key_cmp), rec_cmp(record_cmp),
        val_cmp(val_cmp), has_cmps(true) {}

    ~FixedKVSchema() = default;

    /*
     * Returns a Field object corresponding to the key field of a record
     * matching this schema. The input pointer must be to the start of the
     * record (including the header). If the pointer does not point to a valid
     * record according to this schema, then the output is undefined.
     */
    Field get_key(const byte *record_buffer) {
        return Field(record_buffer + header_length, key_length);
    };

    /*
     * Returns a Field object corresponding to the value field of a record
     * matching this schema. The input pointer must be to the start of the
     * record (including the header). If the pointer does not point to a valid
     * record according to this schema, then the output is undefined.
     */
    Field get_val(const byte *record_buffer){
        return Field(record_buffer + key_length + header_length, value_length);
    }

    /*
     * Creates a new record according to this schema with the specified key and
     * value. The lengths of the provided key and value must match the
     * requirements of the schema, or the output is not defined. The record header
     * region of the buffer will be zeroed, and should be initialized following
     * the return of this function, if desired. This version returns a raw
     * pointer, which must be freed manually when necessary.
     */
    byte *create_record_raw(const byte *key, const byte *val) {
        PageOffset total_length = this->record_length(); 
        byte *data = new byte[total_length]();

        memcpy(data + this->header_length, key, this->key_length);
        memcpy(data + this->header_length + MAXALIGN(this->key_length), val, this->value_length);

        return data;
    }

    /*
     * Creates a new record according to this schema with the specified key and
     * value. The lengths of the provided key and value must match the
     * requirements of the schema, or the output is not defined. The record
     * header region of the buffer will be zeroed, and should be initialized
     * following the return of this function, if desired. This version returns
     * a unique_ptr.
     */
    std::unique_ptr<byte[]> create_record(const byte *key, const byte *val) {
        return std::unique_ptr<byte[]>(this->create_record_raw(key, val));
    }

    /*
     * Creates a new record according to this schema with the specified key and
     * value, stored at the location pointed to by dest. The lengths of the
     * provided key and value must match the requirements of the schema, and
     * the size of the allocated memory pointed to by dest must be sufficient
     * to store a fully formatted record, or the results of this call are
     * undefined. The key and val pointers cannot be overlapping with the
     * dest region. The header region of the buffer will be zeroed and should
     * be initialized following the return of this function.
     */
    void create_record_at(byte *dest, const byte *key, const byte *val) {
        memset(dest, 0, this->header_length);
        memcpy(dest + this->header_length, key, this->key_length);
        memcpy(dest + this->header_length + MAXALIGN(this->key_length), val, this->value_length);
    }

    /*
     * Returns the overall length of a record based on this schema (including
     * alignment padding). This is the minimum size required for any buffer to
     * store a record of this schema.
     */
    PageOffset record_length() {
        return CACHELINEALIGN(this->header_length + MAXALIGN(this->key_length) + MAXALIGN(this->value_length));
    }

    /*
     * Returns the length of the key within a record based on this schema (not
     * including alignment padding).
     */
    PageOffset key_len() {
        return this->key_length;
    }

    /*
     * Returns the length of the value within a record based on this schema
     * (not including alignment padding)
     */
    PageOffset val_len() {
        return this->value_length;
    }

    /*
     * Returns true is comparison functions are defined within the 
     * schema, and false if not. The returned functions from
     * get_record_cmp and get_key_cmp are only valid if this
     * function returns true for a given schema object.
     */
    bool has_defined_comparators() {
        return this->has_cmps;
    }

    /*
     * Returns a comparison function for records created according to
     * this schema. The input should be pointers to the beginning of the
     * record (with any headers included). The output of this function is only
     * defined if has_defined_comparators returns true on this object.
     */
    RecordCmpFunc get_record_cmp() {
        return this->rec_cmp;
    }

    /*
     * Returns a comparison function for keys created according to this schema.
     * The input should be pointers to the beginning of the key data within a
     * record (or just an isolated string of bytes that represents a valid
     * key). The output of this function is only defined if has_defined_comparators
     * returns true on this object.
     */
    KeyCmpFunc get_key_cmp() {
        return this->key_cmp;
    }

    /*
     * Returns a comparison function for the values created according to this schema.
     * The inputs should be pointers to the beginning of the value data within a record 
     * (or just an isolated string of bytes that represents a valid value). The output of
     * this function is only defined if has_defined_comparators returns true on this object.
     */
    ValCmpFunc get_val_cmp() {
        return this->val_cmp;
    }


private:
    PageOffset key_length;
    PageOffset value_length;
    PageOffset header_length;
    KeyCmpFunc key_cmp;
    RecordCmpFunc rec_cmp;
    ValCmpFunc val_cmp;
    bool has_cmps;
};

}}
#endif
