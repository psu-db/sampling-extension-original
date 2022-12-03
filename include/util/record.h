#pragma once

#include "util/base.h"
#include <cstring>

namespace lsm {

typedef uint32_t rec_hdr;
typedef uint64_t key_type;
typedef uint32_t value_type;

constexpr static size_t key_size = sizeof(key_type);
constexpr static size_t value_size = sizeof(value_type);
constexpr static size_t header_size = sizeof(rec_hdr);

// Layout ==> key | value | flags (each part padded to 8.)
// Adding one more thing -> weight.
constexpr static size_t record_size = MAXALIGN(key_size) + value_size + header_size + sizeof(double);

inline static void layout_record(char* buffer, const char* key, const char* value, bool tombstone, double weight = 1.0) {
    memset(buffer, 0, record_size);
    memcpy(buffer, key, key_size);
    memcpy(buffer + MAXALIGN(key_size), value, value_size);
    *(rec_hdr*)(buffer + MAXALIGN(key_size) + value_size) |= tombstone;
    *(double*)(buffer + MAXALIGN(key_size) + value_size + header_size) = tombstone ? 0.0: weight;
}

inline static void layout_memtable_record(char* buffer, const char* key, const char* value, bool tombstone, uint32_t ts, double weight = 1.0) {
    memset(buffer, 0, record_size);
    memcpy(buffer, key, key_size);
    memcpy(buffer + MAXALIGN(key_size), value, value_size);
    *(rec_hdr*)(buffer + MAXALIGN(key_size) + value_size) |= ((ts << 1) | (tombstone ? 1 : 0));
    *(double*)(buffer + MAXALIGN(key_size) + value_size + header_size) = tombstone ? 0.0: weight;
}

/*
 * Returns a pointer to a cache-aligned copy of the record. The 
 * freeing of this pointer is the responsibility of the caller.
 *
 * Record must point to the beginning of a valid record, or the
 * returned pointer is undefined.
 */
static inline char *copy_of(const char *record) {
    char *copy = (char *) aligned_alloc(CACHELINE_SIZE, record_size);
    memcpy(copy, record, record_size);
    return copy;
}

inline static const char *get_key(const char *buffer) {
    return buffer;
}

inline static const char *get_val(const char *buffer) {
    return buffer + MAXALIGN(key_size);
}

inline static const char *get_record(const char *buffer, size_t idx) {
    return buffer + record_size*idx;
}

inline static const char* get_hdr(const char *buffer) {
    return buffer + MAXALIGN(key_size) + value_size;
}

inline static bool is_tombstone(const char *buffer) {
    return *((rec_hdr *)get_hdr(buffer)) & 1;
}

inline static double get_weight(const char* buffer) {
    return *(double*)(get_hdr(buffer) + header_size);
}

static int record_match(const char* rec, const char* key, const char* value, bool tombstone) {
    return (*(key_type*)(get_key(rec)) == *(key_type*)key)
           && (*(value_type*)(get_val(rec)) == *(value_type*)value)
           && ((*(rec_hdr*)(get_hdr(rec)) & 1) == tombstone);
}


static int key_cmp(const void *a, const void *b) {
    if (*((key_type*) a) > *((key_type*) b)) {
        return 1;
    } else if (*((key_type*) a) < *((key_type*) b)) {
        return -1;
    }

    return 0;
}

static int val_cmp(const char *a, const char *b) {
    if (*((value_type*) a) > *((value_type*) b)) {
        return 1;
    } else if (*((value_type*) a) < *((value_type*) b)) {
        return -1;
    }

    return 0;
}


static int record_cmp(const void *a, const void *b) {
    int cmp = key_cmp(get_key((char*) a), get_key((char*) b));

    if (cmp == 0)
        return val_cmp(get_val((const char*)a), get_val((const char*)b));
    else return cmp;
}

// Fall back to the original record_cmp
/*
static int record_cmp(const void *a, const void *b) {
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
*/

static int memtable_record_cmp(const void *a, const void *b) {
    int cmp = key_cmp(get_key((char*) a), get_key((char*) b));

    if (cmp == 0) {
        int cmp2 = val_cmp(get_val((char*) a), get_val((char*) b));
        if (cmp2 == 0) {
            if (*(rec_hdr*)get_hdr((char*)a) < *(rec_hdr*)get_hdr((char*)b)) return -1;
            else return 1;
        } else return cmp2;
    } else return cmp;
}

}
