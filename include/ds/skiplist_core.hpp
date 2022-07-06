/*
 *
 */

#ifndef H_SKIPLIST_CORE
#define H_SKIPLIST_CORE

#include <cds/init.h>
#include <cds/gc/hp.h>
#include <cds/container/details/skip_list_base.h>
#include <cds/container/skip_list_map_hp.h>
#include <cds/opt/compare.h>

#include "util/base.hpp"
#include "util/types.hpp"
#include "catalog/schema.hpp"

namespace lsm { namespace ds {

using cds::container::SkipListMap;

typedef std::pair<const byte*, const Timestamp> MapKey;

extern catalog::KeyCmpFunc sl_global_key_cmp;

struct MapCompareFunc {
    int operator()(const MapKey a, const MapKey b) const {
        extern catalog::KeyCmpFunc sl_global_key_cmp;
        auto rec_cmp = sl_global_key_cmp(a.first, b.first);
        if (rec_cmp == 0) {
            if (a.second == b.second) {
                return 0;
            } else if (a.second > b.second) {
                return 1;
            } else {
                return -1;
            }
        }
        
        return rec_cmp;
    }
};

struct MapCompareFuncLess {
    bool operator()(const MapKey a, const MapKey b) const {
        extern catalog::KeyCmpFunc sl_global_key_cmp;
        auto rec_cmp = sl_global_key_cmp(a.first, b.first);
        return (rec_cmp == 0) ? a.second < b.second : rec_cmp < 0;
    }
};


struct SkipListTraits : public cds::container::skip_list::make_traits <
    cds::opt::compare<MapCompareFunc>,
    cds::opt::less<MapCompareFuncLess>,
    cds::opt::item_counter<cds::atomicity::item_counter>
    >::type
{};


typedef SkipListMap<cds::gc::HP, MapKey, byte*, SkipListTraits> SkipList;

}}

#endif
