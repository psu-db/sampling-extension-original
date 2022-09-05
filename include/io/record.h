#pragma once

#include "util/base.h"
#include <cstring>

namespace lsm {

constexpr size_t key_size = 8;
constexpr size_t value_size = 8;

// Layout ==> key | value | flags (each part padded to 8.)
constexpr size_t record_size = MAXALIGN(key_size) + MAXALIGN(value_size) + 8;

void layout_record(char* buffer, const char* key, const char* value, bool tombstone) {
    memset(buffer, 0, record_size);
    memcpy(buffer, key, key_size);
    memcpy(buffer + MAXALIGN(key_size), value, value_size);
    *(uint64_t*)(buffer + MAXALIGN(key_size) + MAXALIGN(value_size)) |= 1;
}

int record_cmp(const void* rec1, const void* rec2) {
    auto rec1_key = *(uint64_t*)(rec1);
    auto rec2_key = *(uint64_t*)(rec2);
    if (rec1_key < rec2_key) return -1;
    else if (rec1_key == rec2_key) return 0;
    else return 1;
}

int record_match(const void* rec, const char* key, const char* value, bool tombstone) {
    return (*(uint64_t*)(rec) == *(uint64_t*)key)
           && (*(uint64_t*)(rec + MAXALIGN(key_size)) == *(uint64_t*)value)
           && (*(uint64_t*)(rec + MAXALIGN(key_size) + MAXALIGN(value_size)) & 1 == tombstone);
}

}