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
}


static void delete_bench_env()
{
    gsl_rng_free(g_rng);
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


static bool build_insert_vec(std::fstream *file, std::vector<shared_record> &vec, size_t n) {
    for (size_t i=0; i<n; i++) {
        auto rec = create_shared_record();
        if (!next_record(file, rec.first.get(), rec.second.get())) {
            if (i == 0) {
                return false;
            }

            break;
        }

        vec.push_back({rec.first, rec.second});
    }

    return true;
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
static bool warmup(std::fstream *file, lsm::LSMTree *lsmtree, size_t count, double delete_prop=0)
{
    std::string line;

    auto key_buf = std::make_unique<char[]>(lsm::key_size);
    auto val_buf = std::make_unique<char[]>(lsm::value_size);
    
    size_t del_buf_size = 100;
    size_t del_buf_ptr = del_buf_size;
    char delbuf[del_buf_size * lsm::record_size];

    char *buf1 = (char *) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    char *buf2 = (char *) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);

    std::set<lsm::key_type> deleted_keys;

    bool ret = true;

    for (size_t i=0; i<count; i++) {
        if (!next_record(file, key_buf.get(), val_buf.get())) {
            ret = false;
            break;
        }

        lsmtree->append(key_buf.get(), val_buf.get(), false, g_rng);

        if (i > lsmtree->get_memtable_capacity() && del_buf_ptr == del_buf_size) {
            lsmtree->range_sample(delbuf, (char *) &min_key, (char *) &max_key, del_buf_size, buf1, buf2, g_rng);
            del_buf_ptr = 0;
        }

        if (i > lsmtree->get_memtable_capacity() && gsl_rng_uniform(g_rng) < delete_prop) {
            auto key = lsm::get_key(delbuf + (del_buf_ptr * lsm::record_size));
            auto val = lsm::get_val(delbuf + (del_buf_ptr * lsm::record_size));
            del_buf_ptr++;

            if (deleted_keys.find(*(lsm::key_type *) key) == deleted_keys.end()) {
                lsmtree->append(key, val, true, g_rng);
                deleted_keys.insert(*(lsm::key_type*) key);
            }

        }

		if (i % 1000000 == 0) {
            fprintf(stderr, "Finished %zu operations...\n", i);
            //fprintf(stderr, "\tRecords: %ld\n\tTombstones: %ld\n", lsmtree->get_record_cnt(), lsmtree->get_tombstone_cnt());
        }
    }

    free(buf1);
    free(buf2);

    return ret;
}

static bool warmup(std::fstream *file, TreeMap *btree, size_t count, double delete_prop)
{
    std::string line;

    auto key_buf = std::make_unique<char[]>(lsm::key_size);
    auto val_buf = std::make_unique<char[]>(lsm::value_size);

    size_t del_buf_size = 100;
    size_t del_buf_ptr = del_buf_size;
    std::vector<lsm::key_type> delbuf;
    delbuf.reserve(del_buf_size);
    bool ret = true;

    for (size_t i=0; i<count; i++) {
        if (!next_record(file, key_buf.get(), val_buf.get())) {
            ret = false;
            break;
        }

        lsm::key_type key = *(lsm::key_type*) key_buf.get();
        lsm::value_type val = *(lsm::value_type*) val_buf.get();
        auto res = btree->insert({key, val});
        assert(res.second);

        if (del_buf_size > btree->size() && del_buf_ptr == del_buf_size) {
            btree->range_sample(min_key, max_key, del_buf_size, delbuf, g_rng);
            del_buf_ptr = 0;
        }

        if (del_buf_size > btree->size() && gsl_rng_uniform(g_rng) < delete_prop) {
            del_buf_ptr++;
            btree->erase_one(key);
        }
		if (i % 1000000 == 0) {
            fprintf(stderr, "Finished %zu operations...\n", i);
            //fprintf(stderr, "\tRecords: %ld\n\tTombstones: %ld\n", lsmtree->get_record_cnt(), lsmtree->get_tombstone_cnt());
        }
    }

    return ret;
}


static key_range get_key_range(lsm::key_type min, lsm::key_type max, double selectivity)
{
    size_t range_length = (max - min) * selectivity;

    lsm::key_type max_bottom = max - range_length;
    assert(max >= range_length);

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


static void scan_for_key_range(std::fstream *file, size_t record_cnt=0) {
    char key[lsm::key_size];
    char val[lsm::value_size];

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
