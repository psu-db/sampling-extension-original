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
    std::unique_ptr<io::ReadCache> cache;
    std::unique_ptr<catalog::FixedKVSchema> record_schema;
    std::unique_ptr<io::FileManager> file_manager;
};

}}
#endif
