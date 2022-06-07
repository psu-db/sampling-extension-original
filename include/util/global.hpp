/*
 *
 */
#ifndef GLOBAL_H
#define GLOBAL_H

#include "io/readcache.hpp"
#include "catalog/schema.hpp"
#include "io/filemanager.hpp"
#include "util/types.hpp"

namespace lsm { namespace global {

struct g_state {
    io::ReadCache *cache;
    catalog::FixedKVSchema *record_schema;
    io::FileManager *file_manager;
};

}}
#endif
