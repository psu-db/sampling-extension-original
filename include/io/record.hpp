#ifndef RECORD_H
#define RECORD_H

#include <cstdlib>
#include <cstring>
#include <memory>

#include "util/types.hpp"
#include "util/base.hpp"

namespace lsm {
namespace io {

struct RecordHeader {
    Timestamp time;
    bool is_tombstone : 1;
};

constexpr size_t RecordHeaderLength = MAXALIGN(sizeof(RecordHeader));

class Record {
public:
    static byte *create(byte *key, size_t key_len, byte *value, size_t value_len, Timestamp time=0, bool tombstone=false, size_t *reclen=nullptr);
    static std::unique_ptr<byte> create_uniq(byte *key, size_t key_len, byte *value, size_t value_len, Timestamp time=0, bool tombstone=false, size_t *reclen=nullptr);

    Record() : data_ref(nullptr), length(0), key_length(0) {}
    Record(byte *data, size_t len, size_t key_len) : data_ref(data), length(len), key_length(MAXALIGN(key_len)) {};

    PageOffset &get_length();
    byte *&get_data();
    RecordHeader *get_header();

    byte *get_key();
    byte *get_value();

    Timestamp get_timestamp();
    bool is_tombstone();
    bool is_valid();

    void free_data();
    Record deep_copy();

    ~Record() {};
private:
    byte *data_ref;
    PageOffset length;
    PageOffset key_length;
};

}
}

#endif
