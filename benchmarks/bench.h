#ifndef H_BENCH
#define H_BENCH
#include "lsm/LsmTree.h"

#include <ds/avl_container.h>
#include <ds/BTree.h>

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
typedef dsimpl::avl_weighted_multiset<lsm::key_t, lsm::weight_t> AvlSet;

typedef struct record {
    lsm::key_t key;
    lsm::value_t value;
    lsm::weight_t weight;

    friend bool operator<(const struct record &first, const struct record &other) {
        return (first.key < other.key) || (first.key == other.key && first.value < other.value);
    }
} record;

typedef struct shared_record {
    std::shared_ptr<lsm::key_t> key;
    std::shared_ptr<lsm::value_t> value;
    lsm::weight_t weight;

    friend bool operator<(const struct shared_record &first, const struct shared_record &other) {
        return first.key.get() < other.key.get() || (first.key.get() == other.key.get() && first.value.get() < other.value.get());
    }
} shared_record;

typedef std::pair<lsm::key_t, lsm::key_t> key_range;

static gsl_rng *g_rng;
static std::set<shared_record> *g_to_delete;
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


static void init_bench_env(size_t max_reccnt, bool random_seed, bool osm_correction=true)
{
    unsigned int seed = (random_seed) ? get_random_seed() : DEFAULT_SEED;
    init_bench_rng(seed, gsl_rng_mt19937);
    g_to_delete = new std::set<shared_record>();
    g_osm_data = osm_correction;
    g_max_record_cnt = max_reccnt;
    g_reccnt = 0;
}


static void delete_bench_env()
{
    gsl_rng_free(g_rng);
    delete g_to_delete;
}

/*
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
*/


static bool next_record(std::fstream *file, lsm::key_t* key, lsm::value_t* val, lsm::weight_t& weight)
{
    if (g_reccnt >= g_max_record_cnt) return false;

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
        weight = atof(weight_field.c_str());

        if (*key < g_min_key) g_min_key = *key;

        if (*key > g_max_key) g_max_key = *key;

        g_reccnt++;

        return true;
    }

    //key = nullptr;
    //val = nullptr;
    return false;
}


static bool build_insert_vec(std::fstream *file, std::vector<shared_record> &vec, size_t n) {
    vec.clear();
    for (size_t i=0; i<n; i++) {
        shared_record rec;
        if (!next_record(file, rec.key.get(), rec.value.get(), rec.weight)) {
            if (i == 0) {
                return false;
            }

            break;
        }

        vec.push_back({rec.key, rec.value, rec.weight});
    }

    return true;
}


static bool build_btree_insert_vec(std::fstream *file, std::vector<std::pair<btree_record, lsm::weight_t>> &vec, size_t n)
{
    lsm::key_t key;
    lsm::value_t val;
    lsm::weight_t weight;

    vec.clear();
    for (size_t i=0; i<n; i++) {
        if (!next_record(file, &key, &val, weight)) {
            if (i == 0) {
                return false;
            }

            break;
        }

        btree_record rec = {key, val};
        vec.push_back({rec, weight});
    }

    return true;

}

static bool build_avl_insert_vec(std::fstream *file, std::vector<std::pair<lsm::key_t, lsm::weight_t>> &vec, size_t n)
{
    lsm::key_t key;
    lsm::value_t val;
    lsm::weight_t weight;

    vec.clear();
    for (size_t i=0; i<n; i++) {
        if (!next_record(file, &key, &val, weight)) {
            if (i == 0) {
                return false;
            }

            break;
        }

        vec.push_back({key, weight});
    }

    return true;

}
/*
 * helper routines for displaying progress bars to stderr
 */
static const char *g_prog_bar = "======================================================================";
static const size_t g_prog_width = 50;

static void progress_update(double percentage, std::string prompt) {
    int val = (int) (percentage * 100);
    int lpad = (int) (percentage * g_prog_width);
    int rpad = (int) (g_prog_width - lpad);
    fprintf(stderr, "\r(%3d%%) %20s [%.*s%*s]", val, prompt.c_str(), lpad, g_prog_bar, rpad, "");
    fflush(stderr);   

    if (percentage >= 1) fprintf(stderr, "\n");
}



static bool warmup(std::fstream *file, lsm::LSMTree *lsmtree, size_t count, double delete_prop, bool progress=true)
{
    std::string line;

    lsm::key_t key;
    lsm::value_t val;
    lsm::weight_t weight;

    size_t del_buf_size = 100;
    size_t del_buf_ptr = del_buf_size;
    lsm::record_t delbuf[del_buf_size];

    std::set<lsm::key_t> deleted_keys;

    size_t inserted = 0;
    
    double last_percent = 0;
    for (size_t i=0; i<count; i++) {
        if (!next_record(file, &key, &val, weight)) {
            return false;
        }

        inserted++;
        lsmtree->append(key, val, weight, false, g_rng);

        if (i > lsmtree->get_memtable_capacity() && del_buf_ptr == del_buf_size) {
            lsmtree->range_sample(delbuf, del_buf_size, g_rng);
            del_buf_ptr = 0;
        }

        if (i > lsmtree->get_memtable_capacity() && gsl_rng_uniform(g_rng) < delete_prop) {
            auto key = delbuf[del_buf_ptr].key;
            auto val = delbuf[del_buf_ptr].value;
            del_buf_ptr++;

            if (deleted_keys.find(key) == deleted_keys.end()) {
                if (lsm::DELETE_TAGGING) {
                    lsmtree->delete_record(key, val, g_rng);
                } else {
                    lsmtree->append(key, val, 0, true, g_rng);
                }
                deleted_keys.insert(key);
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

    return true;
}


static void avl_sample(AvlSet *tree, std::vector<lsm::key_t> &sampled_records, size_t k) {
    auto lb_rank = tree->get_rank(g_min_key);
    auto ub_rank = tree->get_rank(g_max_key);

    size_t total_weight = ub_rank - lb_rank;
    for (size_t i=0; i<k; i++) {
        auto r = lb_rank + gsl_rng_uniform_int(g_rng, total_weight);
        auto rec = tree->get_nth(r);

        sampled_records[i] = *rec;
    }

}

static bool warmup(std::fstream *file, AvlSet *tree, size_t count, double delete_prop, bool progress=true)
{
    std::string line;

    lsm::key_t key;
    lsm::value_t val;
    lsm::weight_t weight;

    size_t del_buf_size = 100;
    size_t del_buf_idx = del_buf_size;
    std::vector<lsm::key_t> delbuf(del_buf_size);

    
    double last_percent = 0;
    for (size_t i=0; i<count; i++) {
        if (!next_record(file, &key, &val, weight)) {
            return false;
        }

        tree->insert(key, weight);

        if ( i > 10*del_buf_size && del_buf_idx == del_buf_size) {
            avl_sample(tree, delbuf, del_buf_size);
            del_buf_idx = 0;
        }

        if (del_buf_idx != del_buf_size && gsl_rng_uniform(g_rng) < delete_prop) {
            tree->erase(delbuf[del_buf_idx++]);
        }

        if (progress && ((double) i / (double) count) - last_percent > .01) {
            progress_update((double) i / (double) count, "warming up: ");
            last_percent = (double) i / (double) count;
        }
    }

    if (progress) {
        progress_update(1, "warming up:");
    }
    return true;
}

static bool warmup(std::fstream *file, TreeMap *tree, size_t count, double delete_prop, bool progress=true)
{
    std::string line;

    lsm::key_t key;
    lsm::value_t val;
    lsm::weight_t weight;

    size_t del_buf_size = 100;
    size_t del_buf_idx = del_buf_size;
    std::vector<lsm::key_t> delbuf(del_buf_size);

    
    double last_percent = 0;
    for (size_t i=0; i<count; i++) {
        if (!next_record(file, &key, &val, weight)) {
            return false;
        }

        tree->insert({key, val}, weight);

        if ( i > 10*del_buf_size && del_buf_idx == del_buf_size) {
            tree->range_sample(g_min_key, g_max_key, del_buf_size, delbuf, g_rng);
            del_buf_idx = 0;
        }

        if (del_buf_idx != del_buf_size && gsl_rng_uniform(g_rng) < delete_prop) {
            tree->erase_one(delbuf[del_buf_idx++]);
        }

        if (progress && ((double) i / (double) count) - last_percent > .01) {
            progress_update((double) i / (double) count, "warming up: ");
            last_percent = (double) i / (double) count;
        }
    }

    if (progress) {
        progress_update(1, "warming up:");
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

    //auto key_buf = new char[lsm::key_size]();
    lsm::key_t key;
    lsm::value_t val;
    //auto val_buf = new char[lsm::value_size]();
    lsm::weight_t weight;

    size_t i=0;
    while (next_record(file, &key, &val, weight)) {
        auto res = tree->append(key, val, weight, false, g_rng);
        assert(res);
    }
}

static void build_btree(TreeMap *tree, std::fstream *file) {
    // for looking at the insert time distribution
    lsm::key_t key;
    lsm::value_t val;
    lsm::weight_t weight;

    size_t i=0;
    while (next_record(file, &key, &val, weight)) {
        auto res = tree->insert({key, val}, weight);
        assert(res.second);
    }
}

#endif // H_BENCH
