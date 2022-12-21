#ifndef H_BENCH
#define H_BENCH
#include "lsm/LsmTree.h"

#include <cstdlib>
#include <cstdio>
#include <chrono>
#include <algorithm>
#include <numeric>
#include <memory>
#include <iostream>
#include <fstream>
#include <sstream>
#include <unordered_set>
#include <set>
#include <string>
#include <random>

typedef std::pair<lsm::key_type, lsm::key_type> key_range;
typedef std::pair<char*, char*> record;
typedef std::pair<std::shared_ptr<char[]>, std::shared_ptr<char[]>> shared_record;

static gsl_rng *g_rng;
static std::set<shared_record> *g_to_delete;

static constexpr unsigned int DEFAULT_SEED = 0;


static unsigned int get_random_seed()
{
    unsigned int seed = 0;
    std::fstream urandom;
    urandom.open("/dev/urandom", std::ios::in|std::ios::binary);
    urandom.read((char *) &seed, sizeof(seed));
    urandom.close();

    return seed;
}

static void init_bench_rng(unsigned int seed, const gsl_rng_type *type)
{
    g_rng = gsl_rng_alloc(type);

    gsl_rng_set(g_rng, seed);
}


static void init_bench_env(bool random_seed)
{
    unsigned int seed = (random_seed) ? get_random_seed() : DEFAULT_SEED;
    init_bench_rng(seed, gsl_rng_mt19937);
    g_to_delete = new std::set<shared_record>();
}


static void delete_bench_env()
{
    gsl_rng_free(g_rng);
    delete g_to_delete;
}

static record create_record()
{
    auto key_buf = new char[lsm::key_size];
    auto val_buf = new char[lsm::value_size];

    return {key_buf, val_buf};
}
static shared_record create_shared_record()
{
    auto key_buf = new char[lsm::key_size];
    auto val_buf = new char[lsm::value_size];
    auto key_ptr = std::shared_ptr<char[]>(key_buf);
    auto val_ptr = std::shared_ptr<char[]>(val_buf);

    return {key_ptr, val_ptr};
}

static bool next_record(std::fstream *file, char *key, char *val)
{
    std::string line;
    if (std::getline(*file, line, '\n')) {
        std::stringstream line_stream(line);
        std::string key_field;
        std::string value_field;

        std::getline(line_stream, value_field, '\t');
        std::getline(line_stream, key_field, '\t');

        *((lsm::key_type*) key) = atol(key_field.c_str());
        *((lsm::value_type*) val) = atol(value_field.c_str());
        return true;
    }

    key = nullptr;
    val = nullptr;
    return false;
}


static void warmup(std::fstream *file, lsm::LSMTree *lsmtree, size_t count, double delete_prop)
{
    std::string line;

    auto key_buf = std::make_unique<char[]>(lsm::key_size);
    auto val_buf = std::make_unique<char[]>(lsm::value_size);
    
    for (size_t i=0; i<count; i++) {
        if (!next_record(file, key_buf.get(), val_buf.get())) {
            break;
        }

        lsmtree->append(key_buf.get(), val_buf.get(), false, g_rng);

        if (gsl_rng_uniform(g_rng) < delete_prop + .15) {
            auto del_key_buf = new char[lsm::key_size]();
            auto del_val_buf = new char[lsm::value_size]();
            memcpy(del_key_buf, key_buf.get(), lsm::key_size);
            memcpy(del_val_buf, val_buf.get(), lsm::value_size);
            g_to_delete->insert({std::shared_ptr<char[]>(del_key_buf), std::shared_ptr<char[]>(del_val_buf)});
        }

        if (gsl_rng_uniform(g_rng) < delete_prop) {
            std::vector<std::pair<std::shared_ptr<char[]>, std::shared_ptr<char[]>>> del_vec;
            std::sample(g_to_delete->begin(), g_to_delete->end(), std::back_inserter(del_vec), 1, std::mt19937{std::random_device{}()});
            if (del_vec.size() > 0) {
                lsmtree->append(del_vec[0].first.get(), del_vec[0].second.get(), true, g_rng);
                g_to_delete->erase(del_vec[0]);
            }
        }
    }
}


static key_range get_key_range(lsm::key_type min, lsm::key_type max, double selectivity)
{
    size_t range_length = (max - min) * selectivity;

    lsm::key_type max_bottom = max - range_length;
    lsm::key_type bottom = gsl_rng_uniform_int(g_rng, max_bottom);

    return {bottom, bottom + range_length};
}


static void reset_lsm_perf_metrics() {
    lsm::sample_range_time = 0;
    lsm::alias_time = 0;
    lsm::alias_query_time = 0;
    lsm::memtable_sample_time = 0;
    lsm::memlevel_sample_time = 0;
    lsm::disklevel_sample_time = 0;
    lsm::rejection_check_time = 0;

    /*
     * rejection counters are zeroed automatically by the
     * sampling function itself.
     */

    RESET_IO_CNT(); 
}



#endif // H_BENCH
