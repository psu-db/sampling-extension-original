#ifndef H_BENCH
#define H_BENCH
#include "lsm/LsmTree.h"
#include "ds/BTree.h"

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

typedef std::pair<lsm::key_type, lsm::value_type> btree_record;
struct btree_key_extract {
    static const lsm::key_type &get(const btree_record &v) {
        return v.first;
    }
};
typedef tlx::BTree<lsm::key_type, btree_record, btree_key_extract> TreeMap;

btree_record shared_to_btree(shared_record *rec) {
    lsm::key_type key = *(lsm::key_type*) rec->first.get(); 
    lsm::value_type val = *(lsm::value_type*) rec->second.get();

    return {key, val};
}


btree_record to_btree(record *rec) {
    lsm::key_type key = *(lsm::key_type*) rec->first; 
    lsm::value_type val = *(lsm::value_type*) rec->second;

    return {key, val};
}

static gsl_rng *g_rng;
static std::set<shared_record> *g_to_delete;
static lsm::key_type max_key = 0;
static lsm::key_type min_key = UINT64_MAX;

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
        lsm::key_type key_value = atol(key_field.c_str());
        lsm::value_type val_value = atol(value_field.c_str());


        *((lsm::key_type*) key) = key_value;
        *((lsm::value_type*) val) = val_value;

        if (key_value < min_key) {
            min_key = key_value;
        } 

        if (key_value > max_key) {
            max_key = key_value;
        }

        return true;
    }

    key = nullptr;
    val = nullptr;
    return false;
}


static bool build_insert_vec(std::fstream *file, std::vector<shared_record> &vec, size_t n, double del_prop) {
    for (size_t i=0; i<n; i++) {
        auto rec = create_shared_record();
        if (!next_record(file, rec.first.get(), rec.second.get())) {
            if (i == 0) {
                return false;
            }

            break;
        }

        vec.push_back({rec.first, rec.second});

        if (gsl_rng_uniform(g_rng) < del_prop + .15) {
            g_to_delete->insert({rec.first, rec.second});
        }
    }

    return true;
}

/*
 * "Warm up" the LSM Tree data structure by inserting `count` records from
 * `file` into it. If `delete_prop` is non-zero, then the global to-delete
 * record vector will be populated with records during the warmup process (the
 * probability of a record being added to it is delete_prop + .15). If
 * `initial_delete_prop` is non-zero, then records will be deleted from the
 * tree during the warmup as well with probability `initial_delete_prop`.
 */
static void warmup(std::fstream *file, lsm::LSMTree *lsmtree, size_t count, double delete_prop, double initial_delete_prop=0)
{
    std::string line;

    auto key_buf = std::make_unique<char[]>(lsm::key_size);
    auto val_buf = std::make_unique<char[]>(lsm::value_size);
    
    std::vector<shared_record> del_vec;

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

        if (gsl_rng_uniform(g_rng) < initial_delete_prop) {
            std::sample(g_to_delete->begin(), g_to_delete->end(), std::back_inserter(del_vec), 1, std::mt19937{std::random_device{}()});
            if (del_vec.size() > 0) {
                lsmtree->append(del_vec[0].first.get(), del_vec[0].second.get(), true, g_rng);
                g_to_delete->erase(del_vec[0]);
            }
            del_vec.clear();
        }
		
		if (i % 1000000 == 0) fprintf(stderr, "Finished %zu operations...\n", i);
    }
}

/*
 * When using a pre-warmed up tree, we still need to populate the list of future
 * deletes in such a way that it includes records from the original set. This
 * function advances fstream to the first new record (that isn't already in the
 * tree), selecting delete candidates while it goes. This will *only* work
 * properly if the warmed up tree doesn't have any deletes in it already.
 *
 * NOTE: See the commit message (888b69650d4ed1390dd5f06794ff2783562da1b6) for
 * an alternative, better solution to this problem.
 */
static void select_for_delete(std::fstream *file, size_t record_cnt, double delete_prop) {
    auto key_buf = std::make_unique<char[]>(lsm::key_size);
    auto val_buf = std::make_unique<char[]>(lsm::value_size);

    for (size_t i=0; i<record_cnt; i++) {
        if (!next_record(file, key_buf.get(), val_buf.get()))  {
            break;
        }

        if (gsl_rng_uniform(g_rng) < delete_prop + .15) {
            auto del_key_buf = new char[lsm::key_size]();
            auto del_val_buf = new char[lsm::value_size]();
            memcpy(del_key_buf, key_buf.get(), lsm::key_size);
            memcpy(del_val_buf, val_buf.get(), lsm::value_size);
            g_to_delete->insert({std::shared_ptr<char[]>(del_key_buf), std::shared_ptr<char[]>(del_val_buf)});
        }
    }
}


static bool insert_to(std::fstream *file, lsm::LSMTree *lsmtree, size_t count, double delete_prop) {
    std::string line;

    auto key_buf = std::make_unique<char[]>(lsm::key_size);
    auto val_buf = std::make_unique<char[]>(lsm::value_size);
    
    for (size_t i=0; i<count; i++) {
        if (!next_record(file, key_buf.get(), val_buf.get())) {
            return false;
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
            std::vector<shared_record> del_vec;
            std::sample(g_to_delete->begin(), g_to_delete->end(), 
                        std::back_inserter(del_vec), 1, 
                        std::mt19937{std::random_device{}()});

            if (del_vec.size() == 0) {
                continue;
            }

            lsmtree->append(del_vec[0].first.get(), del_vec[0].second.get(), true, g_rng); 
            g_to_delete->erase(del_vec[0]);
        }
    }

    return true;
}


static void warmup(std::fstream *file, TreeMap *btree, size_t count, double delete_prop)
{
    std::string line;

    auto key_buf = std::make_unique<char[]>(lsm::key_size);
    auto val_buf = std::make_unique<char[]>(lsm::value_size);
    
    for (size_t i=0; i<count; i++) {
        if (!next_record(file, key_buf.get(), val_buf.get())) {
            break;
        }

        lsm::key_type key = *(lsm::key_type*) key_buf.get();
        lsm::value_type val = *(lsm::value_type*) val_buf.get();
        auto res = btree->insert({key, val});
        assert(res.second);

        if (gsl_rng_uniform(g_rng) < delete_prop + .15) {
            auto del_key_buf = new char[lsm::key_size]();
            auto del_val_buf = new char[lsm::value_size]();
            memcpy(del_key_buf, key_buf.get(), lsm::key_size);
            memcpy(del_val_buf, val_buf.get(), lsm::value_size);
            g_to_delete->insert({std::shared_ptr<char[]>(del_key_buf), std::shared_ptr<char[]>(del_val_buf)});
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


static void build_lsm_tree(lsm::LSMTree *tree, std::fstream *file) {
    auto key_buf = new char[lsm::key_size]();
    auto val_buf = new char[lsm::value_size]();

    size_t i=0;
    while (next_record(file, key_buf, val_buf)) {
        auto res = tree->append(key_buf, val_buf, false, g_rng);
        assert(res);
    }
    delete[] key_buf;
    delete[] val_buf;
}


static void build_btree(TreeMap *tree, std::fstream *file) {
    // for looking at the insert time distribution
    auto key_buf = new char[lsm::key_size]();
    auto val_buf = new char[lsm::value_size]();

    size_t i=0;
    while (next_record(file, key_buf, val_buf)) {
        lsm::key_type key = *(lsm::key_type*) key_buf;
        lsm::value_type val = *(lsm::value_type*) val_buf;
        auto res = tree->insert({key, val});
        assert(res.second);
    }
    delete[] key_buf;
    delete[] val_buf;
}

#endif // H_BENCH
