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

/*
typedef std::pair<lsm::key_t, lsm::value_t> btree_record;
struct btree_key_extract {
    static const lsm::key_t &get(const btree_record &v) {
        return v.first;
    }
};
*/

//typedef tlx::BTree<lsm::key_t, btree_record, btree_key_extract> TreeMap;
//typedef dsimpl::avl_weighted_multiset<lsm::key_t, lsm::weight_t> AvlSet;

typedef struct record {
    lsm::key_t key;
    lsm::value_t value;
    lsm::weight_t weight;

    friend bool operator<(const struct record &first, const struct record &other) {
        return (first.key < other.key) || (first.key == other.key && first.value < other.value);
    }
} record;


typedef std::pair<lsm::key_t, lsm::key_t> key_range;

static gsl_rng *g_rng;
static std::set<record> *g_to_delete;
static bool g_osm_data;

static lsm::key_t g_min_key = UINT64_MAX;
static lsm::key_t g_max_key = 0;

static size_t g_max_record_cnt = 0;
static size_t g_reccnt = 0;

static constexpr unsigned int DEFAULT_SEED = 0;

typedef enum Operation {
    READ,
    WRITE,
    DELETE
} Operation;


static unsigned int get_random_seed()
{
    unsigned int seed = 0;
    std::fstream urandom;
    urandom.open("/dev/urandom", std::ios::in|std::ios::binary);
    urandom.read((char *) &seed, sizeof(seed));
    urandom.close();

    return seed;
}

static lsm::key_t osm_to_key(const char *key_field) {
    double tmp_key = (atof(key_field) + 180) * 10e6;
    return (lsm::key_t) tmp_key;
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
    g_to_delete = new std::set<record>();
}


static void delete_bench_env()
{
    gsl_rng_free(g_rng);
    delete g_to_delete;
}

static bool next_record(std::fstream *file, lsm::key_t *key, lsm::value_t *val, lsm::weight_t *weight)
{
    std::string line;
    if (std::getline(*file, line, '\n')) {
        std::stringstream line_stream(line);
        std::string key_field;
        std::string value_field;
        std::string weight_field;

        std::getline(line_stream, value_field, '\t');
        std::getline(line_stream, key_field, '\t');
        std::getline(line_stream, weight_field, '\t');

        *key = (g_osm_data) ? osm_to_key(key_field.c_str()) : atol(key_field.c_str());
        *val = atol(value_field.c_str());
        *weight = atof(weight_field.c_str());

        if (*key < g_min_key) g_min_key = *key;
        if (*key > g_max_key) g_max_key = *key;

        return true;
    }

    key = nullptr;
    val = nullptr;
    return false;
}


static bool build_insert_vec(std::fstream *file, std::vector<record> &vec, size_t n, double del_prop) {
    for (size_t i=0; i<n; i++) {
        record rec;
        if (!next_record(file, &rec.key, &rec.value, &rec.weight)) {
            if (i == 0) {
                return false;
            }

            break;
        }

        vec.push_back({rec.key, rec.value, rec.weight});

        if (gsl_rng_uniform(g_rng) < del_prop + .15) {
            g_to_delete->insert({rec.key, rec.value, rec.weight});
        }
    }

    return true;
}


static void warmup(std::fstream *file, lsm::LSMTree *lsmtree, size_t count, double delete_prop)
{
    std::string line;

    lsm::key_t key;
    lsm::value_t val;
    lsm::weight_t weight;

    size_t del_buf_size = 100;
    size_t del_buf_ptr = del_buf_size;
    lsm::record_t delbuf[del_buf_size];

    char *buf1 = (char *) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    char *buf2 = (char *) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);

    std::set<lsm::key_t> deleted_keys;
    
    for (size_t i=0; i<count; i++) {
        if (!next_record(file, &key, &val, &weight)) {
            break;
        }

        lsmtree->append(key, val, weight, false, g_rng);

        if (i > lsmtree->get_memtable_capacity() && del_buf_ptr == del_buf_size) {
            lsmtree->range_sample(delbuf, g_min_key, g_max_key, del_buf_size, buf1, buf2, g_rng);
            del_buf_ptr = 0;
        }

        if (i > lsmtree->get_memtable_capacity() && gsl_rng_uniform(g_rng) < delete_prop) {
            auto key = delbuf[del_buf_ptr].key;
            auto val = delbuf[del_buf_ptr].value;
            del_buf_ptr++;

            if (deleted_keys.find(key) == deleted_keys.end()) {
                lsmtree->append(key, val, 0, true, g_rng);
                deleted_keys.insert(key);
            }

        }

        if (i % 1000000 == 0) {
            fprintf(stderr, "Finished %ld operations...\n", i);
        }
    }
}


static bool insert_to(std::fstream *file, lsm::LSMTree *lsmtree, size_t count, double delete_prop) {
    std::string line;

    lsm::key_t key;
    lsm::value_t val;
    lsm::weight_t weight;
    
    for (size_t i=0; i<count; i++) {
        if (!next_record(file, &key, &val, &weight)) {
            return false;
        }

        lsmtree->append(key, val, weight, false, g_rng);

        if (gsl_rng_uniform(g_rng) < delete_prop + .15) {
            g_to_delete->insert({key, val, weight});
        }

        if (gsl_rng_uniform(g_rng) < delete_prop) {
            std::vector<record> del_vec;
            std::sample(g_to_delete->begin(), g_to_delete->end(), 
                        std::back_inserter(del_vec), 1, 
                        std::mt19937{std::random_device{}()});

            if (del_vec.size() == 0) {
                continue;
            }

            lsmtree->append(del_vec[0].key, del_vec[0].value, 0, true, g_rng); 
            g_to_delete->erase(del_vec[0]);
        }
    }

    return true;
}


static key_range get_key_range(lsm::key_t min, lsm::key_t max, double selectivity)
{
    size_t range_length = (max - min) * selectivity;

    lsm::key_t max_bottom = max - range_length;
    lsm::key_t bottom;

    while ((bottom = gsl_rng_get(g_rng)) > range_length) 
        ;

    return {min + bottom, min + bottom + range_length};
}


static void reset_lsm_perf_metrics() {
    lsm::sample_range_time = 0;
    lsm::alias_time = 0;
    lsm::alias_query_time = 0;
    lsm::memtable_sample_time = 0;
    lsm::memlevel_sample_time = 0;
    lsm::disklevel_sample_time = 0;
    lsm::rejection_check_time = 0;

    lsm::sampling_attempts = 0;
    lsm::sampling_rejections = 0;
    lsm::deletion_rejections = 0;
    lsm::bounds_rejections = 0;
    lsm::tombstone_rejections = 0;
    lsm::memtable_rejections = 0;

    lsm::sampling_bailouts = 0;

    RESET_IO_CNT(); 
}


static void build_lsm_tree(lsm::LSMTree *tree, std::fstream *file) {
    lsm::key_t key;
    lsm::value_t val;
    lsm::weight_t weight;

    size_t i=0;
    while (next_record(file, &key, &val, &weight)) {
        auto res = tree->append(key, val, weight, false, g_rng);
        assert(res);
    }
}
#endif // H_BENCH
