#include <functional>
#include <cstring>

#include "util/base.h"
#include "util/types.h"

namespace lsm {

constexpr size_t KEYLEN = MAXALIGN(8);
constexpr size_t VALLEN = MAXALIGN(8);
constexpr size_t HEADERLEN = MAXALIGN(8);
constexpr size_t RECORDLEN = KEYLEN + VALLEN + HEADERLEN;

inline byte *GETKEY(byte *buffer) {
    return buffer + HEADERLEN;
}


inline byte *GETVAL(byte *buffer) {
    return buffer + HEADERLEN + KEYLEN;
}


inline void FORMAT_RECORD(byte *buffer, byte *key, byte *val) {
    memset(buffer, 0, HEADERLEN);
    memcpy(buffer + HEADERLEN, key, KEYLEN);
    memcpy(buffer + HEADERLEN + KEYLEN, val, VALLEN);
}


inline bool ISTOMBSTONE(byte *buffer) {
    return (bool *) buffer;
}


inline void MAKE_TOMBSTONE(byte *buffer) {
    *((uint64_t *) buffer) |= 1;
}


int KEYCMP(byte *a, byte *b) {
    if (*((int64_t*) a) > *((int64_t*) b)) {
        return 1;
    } else if (*((int64_t*) a) < *((int64_t*) b)) {
        return -1;
    }

    return 0;
}


int VALCMP(byte *a, byte *b) {
    if (*((int64_t*) a) > *((int64_t*) b)) {
        return 1;
    } else if (*((int64_t*) a) < *((int64_t*) b)) {
        return -1;
    }

    return 0;
}


int RECCMP(byte *a, byte *b) {
    int cmp = KEYCMP(GETKEY(a), GETKEY(b));

    if (cmp == 0) {
        bool ts_a = ISTOMBSTONE(a);
        bool ts_b = ISTOMBSTONE(b);
        if (ts_a && ts_b) {
            return 0;
        }

        return (ts_a) ? -1 : 1;
    }

    return cmp;
}

}
