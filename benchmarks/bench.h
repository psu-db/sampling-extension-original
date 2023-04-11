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

typedef std::pair<lsm::key_t, lsm::value_t> btree_record;
struct btree_key_extract {
    static const lsm::key_t &get(const btree_record &v) {
        return v.first;
    }
};
typedef tlx::BTree<lsm::key_t, btree_record, btree_key_extract> TreeMap;

static gsl_rng *g_rng;
static lsm::key_t g_max_key = 0;
static lsm::key_t g_min_key = UINT64_MAX;

static bool g_osm_data = false;
static size_t g_max_record_cnt = 0;
static size_t g_reccnt = 0;

static constexpr unsigned int DEFAULT_SEED = 0;

typedef enum Operation {
    READ,
    WRITE,
    DELETE
} Operation;


typedef struct {
    lsm::key_t lower;
    lsm::key_t upper;
} key_range;

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


static void init_bench_env(size_t max_reccnt, bool random_seed, bool osm_correction=true)
{
    unsigned int seed = (random_seed) ? get_random_seed() : DEFAULT_SEED;
    init_bench_rng(seed, gsl_rng_mt19937);
    g_osm_data = osm_correction;
    g_max_record_cnt = max_reccnt;
    g_reccnt = 0;
}


static lsm::key_t osm_to_key(const char *key_field) {
    double tmp_key = (atof(key_field) + 180) * 10e6;
    return (lsm::key_t) tmp_key;
}


static void delete_bench_env()
{
    gsl_rng_free(g_rng);
}


static bool next_record(std::fstream *file, lsm::key_t& key, lsm::value_t& val)
{

    if (g_reccnt >= g_max_record_cnt) return false;

    std::string line;
    if (std::getline(*file, line, '\n')) {
        std::stringstream line_stream(line);
        std::string key_field;
        std::string value_field;

        std::getline(line_stream, value_field, '\t');
        std::getline(line_stream, key_field, '\t');

        key = (g_osm_data) ? osm_to_key(key_field.c_str()) : atol(key_field.c_str());
        val = atol(value_field.c_str());

        if (key < g_min_key) g_min_key = key;
        if (key > g_max_key) g_max_key = key;

        g_reccnt++;

        return true;
    }

    return false;
}


static bool build_insert_vec(std::fstream *file, std::vector<lsm::record_t> &vec, size_t n) {
    vec.clear();

    for (size_t i=0; i<n; i++) {
        lsm::key_t key;
        lsm::value_t val;
        if (!next_record(file, key, val)) {
            if (i == 0) {
                return false;
            }

            break;
        }

        vec.push_back({key, val});
    }

    return true;
}

static bool build_btree_insert_vec(std::fstream *file, std::vector<btree_record> &vec, size_t n)
{
    lsm::key_t key;
    lsm::value_t val;

    vec.clear();
    for (size_t i=0; i<n; i++) {
        if (!next_record(file, key, val)) {
            if (i == 0) {
                return false;
            }

            break;
        }

        vec.push_back({key, val});
    }

    return true;

}


/*
 * helper routines for displaying progress bars to stderr
 */
static const char *g_prog_bar = "======================================================================";
static const size_t g_prog_width = 70;

static void progress_update(double percentage, std::string prompt) {
    int val = (int) (percentage * 100);
    int lpad = (int) (percentage * g_prog_width);
    int rpad = (int) (g_prog_width - lpad);
    fprintf(stderr, "\r(%3d%%) %s [%.*s%*s]", val, prompt.c_str(), lpad, g_prog_bar, rpad, "");
    fflush(stderr);   
}

/*
 * "Warm up" the LSM Tree data structure by inserting `count` records from
 * `file` into it. If `delete_prop` is non-zero, then on each insert following
 * the first memtable-full of inserts there is a `delete_prop` probability that
 * a record already inserted into the tree will be deleted (in addition to the
 * insert) by inserting a tombstone. Returns true if the warmup cycle finishes
 * without exhausting the file, and false if the warmup cycle exhausts the file
 * before inserting the requisite number of records.
 */
static bool warmup(std::fstream *file, lsm::LSMTree *lsmtree, size_t count, double delete_prop, bool progress=true)
{
    std::string line;

    lsm::key_t key;
    lsm::value_t val;
    
    size_t del_buf_size = 100;
    size_t del_buf_ptr = del_buf_size;
    lsm::record_t delbuf[del_buf_size];

    std::set<lsm::record_t> deleted_keys;

    bool ret = true;
    double last_percent = 0;

    for (size_t i=0; i<count; i++) {
        if (!next_record(file, key, val)) {
            ret = false;
            break;
        }

        lsmtree->append(key, val, false, g_rng);

        if (i > lsmtree->get_memtable_capacity() && del_buf_ptr == del_buf_size) {
            lsmtree->range_sample(delbuf, g_min_key, g_max_key, del_buf_size, g_rng);
            del_buf_ptr = 0;
            deleted_keys.clear();
        }

        if (i > lsmtree->get_memtable_capacity() && gsl_rng_uniform(g_rng) < delete_prop) {
            auto key = delbuf[del_buf_ptr].key;
            auto val = delbuf[del_buf_ptr].value;
            del_buf_ptr++;

            if (deleted_keys.find({key, val}) == deleted_keys.end()) {
                lsmtree->append(key, val, true, g_rng);
                deleted_keys.insert({key, val});
            }

        }

        if (progress && ((double) i / (double) count) - last_percent > .01) {
            progress_update((double) i / (double) count, "warming up:");
            last_percent = (double) i / (double) count;
        }
    }

    if (progress) {
        progress_update(1, "warming up:");
    }

    return ret;
}

static bool warmup(std::fstream *file, TreeMap *btree, size_t count, double delete_prop, bool progress=true)
{
    std::string line;

    lsm::key_t key;
    lsm::value_t val;

    size_t del_buf_size = 100;
    size_t del_buf_ptr = del_buf_size;
    std::vector<lsm::key_t> delbuf;
    delbuf.reserve(del_buf_size);
    bool ret = true;
    double last_percent = 0;

    for (size_t i=0; i<count; i++) {
        if (!next_record(file, key, val)) {
            ret = false;
            break;
        }

        auto res = btree->insert({key, val});
        assert(res.second);

        if (del_buf_size > btree->size() && del_buf_ptr == del_buf_size) {
            btree->range_sample(g_min_key, g_max_key, del_buf_size, delbuf, g_rng);
            del_buf_ptr = 0;
        }

        if (del_buf_size > btree->size() && gsl_rng_uniform(g_rng) < delete_prop) {
            del_buf_ptr++;
            btree->erase_one(key);
        }


        if (progress && ((double) i / (double) count) - last_percent > .01) {
            progress_update((double) i / (double) count, "warming up:");
            last_percent = (double) i / (double) count;
        }
    }

    if (progress) {
        progress_update(1, "warming up:");
    }

    return ret;
}


static key_range get_key_range(lsm::key_t min, lsm::key_t max, double selectivity)
{
    size_t range_length = (max - min) * selectivity;

    lsm::key_t max_bottom = max - range_length;
    assert(max >= range_length);

    lsm::key_t bottom = gsl_rng_uniform_int(g_rng, max_bottom);

    return {bottom, bottom + range_length};
}


static void reset_lsm_perf_metrics() {
    lsm::sample_range_time = 0;
    lsm::alias_time = 0;
    lsm::alias_query_time = 0;
    lsm::memtable_sample_time = 0;
    lsm::memlevel_sample_time = 0;
    lsm::rejection_check_time = 0;

    /*
     * rejection counters are zeroed automatically by the
     * sampling function itself.
     */

    RESET_IO_CNT(); 
}


static void build_lsm_tree(lsm::LSMTree *tree, std::fstream *file) {
    lsm::key_t key;
    lsm::value_t val;

    size_t i=0;
    while (next_record(file, key, val)) {
        auto res = tree->append(key, val, false, g_rng);
        assert(res);
    }
}


static void build_btree(TreeMap *tree, std::fstream *file) {
    // for looking at the insert time distribution
    lsm::key_t key;
    lsm::value_t val;

    size_t i=0;
    while (next_record(file, key, val)) {
        auto res = tree->insert({key, val});
        assert(res.second);
    }
}


static void scan_for_key_range(std::fstream *file, size_t record_cnt=0) {
    lsm::key_t key;
    lsm::value_t val;

    size_t processed_records = 0;
    while (next_record(file, key, val)) {
        processed_records++;
        // If record_cnt is 0, as default, this condition will
        // never be satisfied and the whole file will be processed.
        if (record_cnt == processed_records)  {
                break;
        }
    }
}

#endif // H_BENCH
