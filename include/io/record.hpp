/* record.hpp
 * Douglas Rumbaugh
 *
 * A simple wrapper for a chunk of memory with a specified length, and a header
 * for use by the storage engine in concurrency control, deletion, etc.
 *
 */
#ifndef RECORD_H
#define RECORD_H

#include <cstdlib>
#include <cstring>
#include <memory>

#include "util/types.hpp"
#include "util/base.hpp"

namespace lsm { namespace io {

struct RecordHeader {
    Timestamp time : 32;
    bool is_tombstone : 1;
};

constexpr size_t RecordHeaderLength = MAXALIGN(sizeof(RecordHeader));

class Record {
public:
    /*
     * Create a default, invalid record, with all attributes zeroed out.
     */
    Record() : data_ref(nullptr), length(0) {}

    /*
     * Create a record referring to data located at *data of length len. Note
     * that the record object does not take ownership of the memory, and so it
     * must be managed elsewhere. If this record "should" own the memory, be
     * sure to manually call its free_data() method before destructing it,
     * otherwise the memory will leak.
     *
     * The header of the record will be initialized to all 0 values.
     */
    Record(byte *data, size_t len) : data_ref(data), length(len) {};


    /*
     * Create a record referring to data located at *data of length len. Note
     * that the record object does not take ownership of the memory, and so it
     * must be managed elsewhere. If this record "should" own the memory, be
     * sure to manually call its free_data() method before destructing it,
     * otherwise the memory will leak.
     *
     * The header of the record will be initialized with a timestamp of
     * time and a tombstone status of tombstone.
     */
    Record(byte *data, size_t len, Timestamp time, bool tombstone) {
        this->data_ref = data;
        this->length = len;

        this->get_header()->time = time;
        this->get_header()->is_tombstone = tombstone;
    }

    /*
     * Returns a reference to the ID of the record
     */
    inline RecordId &get_id() {
        return this->rid;
    }

    /*
     * Returns a reference to the length of this record.
     */
    inline PageOffset &get_length() {
        return this->length;
    }

    /*
     * Returns a reference to the data pointer of this record. This pointer
     * points to the start of the buffer, and so includes the header.
     */
    inline byte *&get_data() {
        return this->data_ref;
    }

    /*
     * Returns a pointer to the header of this record. The attributes of this
     * header may be updated if necessary.
     */
    inline RecordHeader *get_header() {
        return (RecordHeader *) this->data_ref;
    }


    /*
     * Returns the timestamp associated with this record, as stored in its
     * header.
     */
    inline Timestamp get_timestamp() {
        return this->get_header()->time;
    }

    /*
     * Returns whether this record is a deletion tombstone, as stored in 
     * its header.
     */
    inline bool is_tombstone() {
        return this->get_header()->is_tombstone;
    }

    /*
     * Returns true if the data pointer and length of this record are non-zero, and
     * false if the pointer is nullptr or the length is 0.
     */
    inline bool is_valid() {
        return !(this->length == 0 || this->data_ref == nullptr);
    }

    /*
     * Frees the memory referred to by the data_ref of this record. May be
     * necessary in certain circumstances when the record takes ownership of
     * the memory (such as when it is created using a deep copy, or using a
     * Schema's create() method). Do not call this method if the Record is
     * referring to memory managed by some other structure, such as if the
     * record was retrieved from a Page object or an iterator.
     */
    inline void free_data() {
        if (this->data_ref) {
            delete[] this->get_data();
        }

        this->data_ref = nullptr;
        this->length = 0;
    }

    /*
     * Create a deep copy of this record. The resulting record will own its own
     * memory, and so free_data() should be used on it prior to calling its
     * destructor.
     */
    inline Record deep_copy() {
        byte *new_buf = new byte[this->length]();
        memcpy(new_buf, this->data_ref, this->length);

        return Record(new_buf, this->length);
    }

    /*
     * Generally a Record does not own its own memory, and so a default
     * destructor is used.
     */
    ~Record() = default;
private:
    byte *data_ref;
    PageOffset length;
    RecordId rid;
};

struct alignas(64) CacheRecord {
    Record rec;
};

}}
#endif
