#pragma once

#include "util/base.h"
#include <cstring>

namespace lsm {

typedef uint64_t rec_hdr;
typedef uint64_t key_type;
typedef uint64_t value_type;

constexpr size_t key_size = sizeof(key_type);
constexpr size_t value_size = sizeof(value_type);
constexpr size_t header_size = sizeof(rec_hdr);

// Layout ==> key | value | flags (each part padded to 8.)
constexpr size_t record_size = MAXALIGN(key_size) + MAXALIGN(value_size) + MAXALIGN(header_size);

inline void layout_record(char* buffer, const char* key, const char* value, bool tombstone) {
    memset(buffer, 0, record_size);
    memcpy(buffer, key, key_size);
    memcpy(buffer + MAXALIGN(key_size), value, value_size);
    *(uint64_t*)(buffer + MAXALIGN(key_size) + MAXALIGN(value_size)) |= tombstone;
}

inline const char *get_key(const char *buffer) {
    return buffer;
}

inline const char *get_val(const char *buffer) {
    return buffer + MAXALIGN(key_size);
}

inline const char* get_hdr(const char *buffer) {
    return buffer + MAXALIGN(key_size) + MAXALIGN(value_size);
}

inline bool is_tombstone(const char *buffer) {
    return *((rec_hdr *) buffer + MAXALIGN(key_size) + MAXALIGN(value_size)) & 1;
}

int record_match(const char* rec, const char* key, const char* value, bool tombstone) {
    return (*(key_type*)(get_key(rec)) == *(key_type*)key)
           && (*(value_type*)(get_val(rec)) == *(value_type*)value)
           && ((*(rec_hdr*)(get_hdr(rec)) & 1) == tombstone);
}


int key_cmp(const void *a, const void *b) {
    if (*((key_type*) a) > *((key_type*) b)) {
        return 1;
    } else if (*((key_type*) a) < *((key_type*) b)) {
        return -1;
    }

    return 0;
}

int val_cmp(const char *a, const char *b) {
    if (*((value_type*) a) > *((value_type*) b)) {
        return 1;
    } else if (*((value_type*) a) < *((value_type*) b)) {
        return -1;
    }

    return 0;
}

int record_cmp(const void *a, const void *b) {
    int cmp = key_cmp(get_key((char*) a), get_key((char*) b));

    if (cmp == 0) {
        bool tomb_a = is_tombstone((char*) a);
        bool tomb_b = is_tombstone((char*) b);
        if (tomb_a && tomb_b) {
            return 0;
        }

        return (tomb_a) ? -1 : 1;
    }

    return cmp;
}

}
