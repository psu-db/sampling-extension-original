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
    Record() : data_ref(nullptr), length(0) {}
    Record(byte *data, size_t len) : data_ref(data), length(len) {};
    Record(byte *data, size_t len, Timestamp time, bool tombstone);

    PageOffset &get_length();
    byte *&get_data();
    RecordHeader *get_header();

    Timestamp get_timestamp();
    bool is_tombstone();
    bool is_valid();

    void free_data();
    Record deep_copy();

    ~Record() {};
private:
    byte *data_ref;
    PageOffset length;
};

}
}

#endif
