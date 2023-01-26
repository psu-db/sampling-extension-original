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

typedef struct record {
    char *key;
    char *value;
    lsm::weight_type weight;

    friend bool operator<(const struct record &first, const struct record &other) {
        if (*(lsm::key_type *) first.key == *(lsm::key_type *) other.key) {
            return *(lsm::value_type*) first.value < *(lsm::value_type*) other.value;
        }

        return *(lsm::key_type *) first.key < *(lsm::key_type *)  other.key;
    }
} record;

typedef struct shared_record {
    std::shared_ptr<char[]> key;
    std::shared_ptr<char[]> value;
    lsm::weight_type weight;

    friend bool operator<(const struct shared_record &first, const struct shared_record &other) {
        if (*(lsm::key_type *) first.key.get() == *(lsm::key_type *) other.key.get()) {
            return *(lsm::value_type*) first.value.get() < *(lsm::value_type*) other.value.get();
        }

        return *(lsm::key_type *) first.key.get() < *(lsm::key_type *)  other.key.get();
    }
} shared_record;

typedef std::pair<lsm::key_type, lsm::key_type> key_range;

static gsl_rng *g_rng;
static std::set<shared_record> *g_to_delete;

static lsm::key_type g_min_key = INT64_MAX;
static lsm::key_type g_max_key = INT64_MIN;

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


static lsm::key_type osm_to_key(const char *key_field) {
    double tmp_key = (atof(key_field) + 180) * 10e6;
    return (lsm::key_type) tmp_key;
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

    return {key_buf, val_buf, 0};
}


static shared_record create_shared_record()
{
    auto key_buf = new char[lsm::key_size];
    auto val_buf = new char[lsm::value_size];
    auto key_ptr = std::shared_ptr<char[]>(key_buf);
    auto val_ptr = std::shared_ptr<char[]>(val_buf);

    return {key_ptr, val_ptr, 0};
}


static bool next_record(std::fstream *file, char *key, char *val, lsm::weight_type *weight)
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

        *((lsm::key_type*) key) = osm_to_key(key_field.c_str());
        *((lsm::value_type*) val) = atol(value_field.c_str());
        *(weight) = atof(weight_field.c_str());

        if (*(lsm::key_type*) key < g_min_key) {
            g_min_key = *(lsm::key_type*) key;
        }

        if (*(lsm::key_type*) key > g_max_key) {
            g_max_key = *(lsm::key_type*) key;
        }

        return true;
    }

    key = nullptr;
    val = nullptr;
    return false;
}


static bool build_insert_vec(std::fstream *file, std::vector<shared_record> &vec, size_t n) {
    for (size_t i=0; i<n; i++) {
        auto rec = create_shared_record();
        if (!next_record(file, rec.key.get(), rec.value.get(), &rec.weight)) {
            if (i == 0) {
                return false;
            }

            break;
        }

        vec.push_back({rec.key, rec.value, rec.weight});
    }

    return true;
}


static bool warmup(std::fstream *file, lsm::LSMTree *lsmtree, size_t count, double delete_prop)
{
    std::string line;

    auto key_buf = std::make_unique<char[]>(lsm::key_size);
    auto val_buf = std::make_unique<char[]>(lsm::value_size);
    lsm::weight_type weight;

    size_t del_buf_size = 100;
    size_t del_buf_ptr = del_buf_size;
    char delbuf[del_buf_size * lsm::record_size];

    char *buf1 = (char *) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    char *buf2 = (char *) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);

    std::set<lsm::key_type> deleted_keys;
    
    for (size_t i=0; i<count; i++) {
        if (!next_record(file, key_buf.get(), val_buf.get(), &weight)) {
            return false;
        }

        lsmtree->append(key_buf.get(), val_buf.get(), weight, false, g_rng);

        if (i > lsmtree->get_memtable_capacity() && del_buf_ptr == del_buf_size) {
            lsmtree->range_sample(delbuf, del_buf_size, g_rng);
            del_buf_ptr = 0;
        }

        if (i > lsmtree->get_memtable_capacity() && gsl_rng_uniform(g_rng) < delete_prop) {
            auto key = lsm::get_key(delbuf + (del_buf_ptr * lsm::record_size));
            auto val = lsm::get_val(delbuf + (del_buf_ptr * lsm::record_size));
            del_buf_ptr++;

            if (deleted_keys.find(*(lsm::key_type *) key) == deleted_keys.end()) {
                lsmtree->append(key, val, 0, true, g_rng);
                deleted_keys.insert(*(lsm::key_type*) key);
            }

        }

        if (i % 1000000 == 0) {
            fprintf(stderr, "Finished %ld operations...\n", i);
        }
    }

    return true;
}


static bool insert_to(std::fstream *file, lsm::LSMTree *lsmtree, size_t count, double delete_prop) {
    std::string line;

    auto key_buf = std::make_unique<char[]>(lsm::key_size);
    auto val_buf = std::make_unique<char[]>(lsm::value_size);
    lsm::weight_type weight;
    
    for (size_t i=0; i<count; i++) {
        if (!next_record(file, key_buf.get(), val_buf.get(), &weight)) {
            return false;
        }

        lsmtree->append(key_buf.get(), val_buf.get(), weight, false, g_rng);

        if (gsl_rng_uniform(g_rng) < delete_prop + .15) {
            auto del_key_buf = new char[lsm::key_size]();
            auto del_val_buf = new char[lsm::value_size]();
            memcpy(del_key_buf, key_buf.get(), lsm::key_size);
            memcpy(del_val_buf, val_buf.get(), lsm::value_size);
            g_to_delete->insert({std::shared_ptr<char[]>(del_key_buf), std::shared_ptr<char[]>(del_val_buf), weight});
        }

        if (gsl_rng_uniform(g_rng) < delete_prop) {
            std::vector<shared_record> del_vec;
            std::sample(g_to_delete->begin(), g_to_delete->end(), 
                        std::back_inserter(del_vec), 1, 
                        std::mt19937{std::random_device{}()});

            if (del_vec.size() == 0) {
                continue;
            }

            lsmtree->append(del_vec[0].key.get(), del_vec[0].value.get(), 0, true, g_rng); 
            g_to_delete->erase(del_vec[0]);
        }
    }

    return true;
}


static key_range get_key_range(lsm::key_type min, lsm::key_type max, double selectivity)
{
    size_t range_length = (max - min) * selectivity;

    lsm::key_type max_bottom = max - range_length;
    lsm::key_type bottom;

    while ((bottom = gsl_rng_get(g_rng)) > range_length) 
        ;

    return {min + bottom, min + bottom + range_length};
}


static void reset_lsm_perf_metrics() {
    lsm::memtable_alias_time = 0;
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


static void build_lsm_tree(lsm::LSMTree *tree, std::fstream *file) {
    auto key_buf = new char[lsm::key_size]();
    auto val_buf = new char[lsm::value_size]();
    lsm::weight_type weight;

    size_t i=0;
    while (next_record(file, key_buf, val_buf, &weight)) {
        auto res = tree->append(key_buf, val_buf, weight, false, g_rng);
        assert(res);
    }
    delete[] key_buf;
    delete[] val_buf;
}
#endif // H_BENCH
