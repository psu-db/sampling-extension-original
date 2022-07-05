/*
 *
 */

#include "util/benchutil.hpp"

namespace lsm { namespace bench {

static std::string default_dir = "benchmarks/data/default_bench/";

static int key_cmp_func(const byte * a, const byte *b)
{
    if (*((int64_t*) a) > *((int64_t*) b)) {
        return 1;
    } else if (*((int64_t*) a) < *((int64_t*) b)) {
        return -1;
    }

    return 0;
}


static int rec_cmp_func(const byte *a, const byte *b) 
{
    int64_t key1 = *((int64_t*) (a + io::RecordHeaderLength));
    int64_t key2 = *((int64_t*) (b + io::RecordHeaderLength));

    if (key1 < key2) {
        return -1;
    } else if (key1 > key2) {
        return 1;
    }

    Timestamp time1 = ((io::RecordHeader *) a)->time;
    Timestamp time2 = ((io::RecordHeader *) b)->time;

    if (time1 < time2) {
        return -1;
    } else if (time1 < time2) {
        return 1;
    }

    bool tombstone1 = ((io::RecordHeader *) a)->is_tombstone;
    bool tombstone2 = ((io::RecordHeader *) b)->is_tombstone;

    if (tombstone1 && !tombstone2) {
        return -1;
    } else if (tombstone2 && !tombstone1) {
        return 1;
    }

    return 0;
}


static int val_cmp_func(const byte *a, const byte *b)
{
    int64_t val1 = *((int64_t*) (a + io::RecordHeaderLength + sizeof(int64_t)));
    int64_t val2 = *((int64_t*) (b + io::RecordHeaderLength + sizeof(int64_t)));

    if (val1 < val2) {
        return -1;
    } else if (val1 > val2) {
        return 1;
    }

    return 0;
}


catalog::RecordCmpFunc rec_cmp()
{
    return std::bind(&rec_cmp_func, _1, _2);
}


catalog::KeyCmpFunc key_cmp()
{
    return std::bind(&key_cmp_func, _1, _2);
}


catalog::ValCmpFunc val_cmp()
{
    return std::bind(&val_cmp_func, _1, _2);
}


std::unique_ptr<global::g_state> bench_state(std::string root_dir)
{
    if (root_dir == "") {
        root_dir = default_dir;
    }

    auto new_state = new global::g_state();
    gsl_rng_env_setup();

    new_state->cache = std::make_unique<io::ReadCache>(1024);
    new_state->file_manager = std::make_unique<io::FileManager>(root_dir);
    new_state->record_schema = std::make_unique<catalog::FixedKVSchema>(sizeof(int64_t), sizeof(int64_t), io::RecordHeaderLength, key_cmp(), rec_cmp());
    new_state->rng = gsl_rng_alloc(gsl_rng_gfsr4);

    return std::unique_ptr<global::g_state>(new_state);
}


std::unique_ptr<global::g_state> bench_state(size_t key_size, size_t value_size, std::string root_dir)
{
    if (root_dir == "") {
        root_dir = default_dir;
    }

    auto new_state = new global::g_state();
    gsl_rng_env_setup();

    new_state->cache = std::make_unique<io::ReadCache>(1024);
    new_state->file_manager = std::make_unique<io::FileManager>(root_dir);
    new_state->record_schema = std::make_unique<catalog::FixedKVSchema>(key_size, value_size, io::RecordHeaderLength, key_cmp(), rec_cmp());
    new_state->rng = gsl_rng_alloc(gsl_rng_gfsr4);

    return std::unique_ptr<global::g_state>(new_state);
}

std::vector<int64_t> random_unique_keys(size_t key_count, int64_t max_key, global::g_state *state)
{
    auto keys = std::unordered_set<int64_t>();
    while (keys.size() < key_count) {
        auto key = gsl_rng_uniform_int(state->rng, max_key);
        if (keys.find(key) == keys.end()) {
            keys.insert(key);
        }
    }

    std::vector<int64_t> key_vec;
    key_vec.insert(key_vec.begin(), keys.begin(), keys.end());

    return key_vec;
}

}}
