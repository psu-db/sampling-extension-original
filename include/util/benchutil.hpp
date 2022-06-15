/*
 *
 */
#ifndef H_BENCHUTIL
#define H_BENCHUTIL

#include <unordered_set>
#include <vector>

#include "util/global.hpp"
#include "catalog/schema.hpp"

using namespace std::placeholders;

namespace lsm { namespace bench {
    
catalog::RecordCmpFunc get_rec_cmp();
catalog::KeyCmpFunc get_key_cmp();
catalog::ValCmpFunc get_val_cmp();

std::unique_ptr<global::g_state> bench_state(std::string root_dir="");
std::vector<int64_t> random_unique_keys(size_t key_count, int64_t max_key, global::g_state *state);

}}

#endif
