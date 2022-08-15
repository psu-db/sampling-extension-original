/*
 *
 */

#ifndef H_TOMBSTONE_CACHE
#define H_TOMBSTONE_CACHE

#include <map>

#include "util/base.hpp"
#include "util/types.hpp"
#include "catalog/schema.hpp"

namespace lsm { namespace util {

struct TombstoneKey {
    const byte *key;
    const byte *value;
};

struct TombstoneCmp {
    catalog::KeyCmpFunc key_cmp;
    catalog::ValCmpFunc val_cmp;

    bool operator()(const TombstoneKey a, const TombstoneKey b) const {
        auto key_res = key_cmp(a.key, b.key);

        if (key_res == 0) {
            auto val_res = val_cmp(a.value, b.value);
            return val_res == -1;
        }

        return key_res == -1;
    }
};

class TombstoneCache {
public:
    TombstoneCache() = default;
    TombstoneCache(ssize_t capacity, catalog::FixedKVSchema *schema);

    void insert(const byte *key, const byte *val, Timestamp time);
    void truncate();

    bool exists(const byte *key, const byte *val, Timestamp time);

private:
    ssize_t capacity;
    std::multimap<TombstoneKey, Timestamp, TombstoneCmp> table;
};

}
}

#endif
