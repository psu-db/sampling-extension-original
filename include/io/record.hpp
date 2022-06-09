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
    Record(byte *data, size_t len, Timestamp time, bool tombstone);

    /*
     * Returns a reference to the ID of the record
     */
    RecordId &get_id();

    /*
     * Returns a reference to the length of this record.
     */
    PageOffset &get_length();

    /*
     * Returns a reference to the data pointer of this record. This pointer
     * points to the start of the buffer, and so includes the header.
     */
    byte *&get_data();

    /*
     * Returns a pointer to the header of this record. The attributes of this
     * header may be updated if necessary.
     */
    RecordHeader *get_header();


    /*
     * Returns the timestamp associated with this record, as stored in its
     * header.
     */
    Timestamp get_timestamp();

    /*
     * Returns whether this record is a deletion tombstone, as stored in 
     * its header.
     */
    bool is_tombstone();

    /*
     * Returns true if the data pointer and length of this record are non-zero, and
     * false if the pointer is nullptr or the length is 0.
     */
    bool is_valid();

    /*
     * Frees the memory referred to by the data_ref of this record. May be
     * necessary in certain circumstances when the record takes ownership of
     * the memory (such as when it is created using a deep copy, or using a
     * Schema's create() method). Do not call this method if the Record is
     * referring to memory managed by some other structure, such as if the
     * record was retrieved from a Page object or an iterator.
     */
    void free_data();

    /*
     * Create a deep copy of this record. The resulting record will own its own
     * memory, and so free_data() should be used on it prior to calling its
     * destructor.
     */
    Record deep_copy();

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

}}
#endif
