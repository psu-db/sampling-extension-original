/*
 *
 */

#ifndef H_SKIPLIST_CORE
#define H_SKIPLIST_CORE

#include "sl_map.h"

#include "util/base.hpp"
#include "util/types.hpp"
#include "catalog/schema.hpp"

namespace lsm { namespace ds {

extern catalog::KeyCmpFunc sl_global_key_cmp;

struct MapKey {
    byte *key;
    Timestamp time;

    friend bool operator<(const MapKey a, const MapKey b) {
        auto cmp = sl_global_key_cmp(a.key, b.key);

        if (cmp == 0) {
            return a.time < b.time;
        }

        return cmp == -1;
    }

    friend bool operator>(const MapKey a, const MapKey b) {
        auto cmp = sl_global_key_cmp(a.key, b.key);

        if (cmp == 0) {
            return a.time > b.time;
        }

        return cmp == 1;
    }

    friend bool operator!=(const MapKey a, const MapKey b) {
        auto cmp = sl_global_key_cmp(a.key, b.key);

        if (cmp == 0) {
            return a.time != b.time;
        }

        return cmp != 0;
    }

    friend bool operator==(const MapKey a, const MapKey b) {
        auto cmp = sl_global_key_cmp(a.key, b.key);

        if (cmp == 0) {
            return a.time == b.time;
        }

        return false;
    }

};

typedef sl_map_gc<MapKey, byte*> SkipList;

}}

#endif
