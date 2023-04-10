#pragma once

#include "util/base.h"
#include <cstring>

namespace lsm {

typedef uint32_t hdr_t;
typedef uint64_t key_t;
typedef uint32_t value_t;

struct record_t {
    key_t key;
    value_t value;
    hdr_t header;

    inline bool match(key_t k, value_t v, bool is_tombstone) const {
        return (key == k) && (value == v) && ((header & 1) == is_tombstone);
    }

    inline void set_delete_status() {
        header |= 2;
    }

    inline bool get_delete_status() const {
        return header & 2;
    }

    inline bool is_tombstone() const {
        return header & 1;
    }

    inline bool match(const record_t* other) const {
        return key == other->key && value == other->value;
    }

    inline bool operator<(const record_t& other) const {
        return key < other.key || (key == other.key && value < other.value);
    }

    inline bool lt(const key_t& k, const value_t& v) const {
        return key < k || (key == k && value < v);
    }
};

static_assert(sizeof(record_t) == 16, "Record is not 16 bytes long.");

static bool memtable_record_cmp(const record_t& a, const record_t& b) {
    return (a.key < b.key) || (a.key == b.key && a.value < b.value)
        || (a.key == b.key && a.value == b.value && a.header < b.header);
}

}
