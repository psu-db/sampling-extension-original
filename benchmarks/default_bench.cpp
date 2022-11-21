#define ENABLE_TIMER

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

gsl_rng *g_rng;

static std::set<std::pair<std::shared_ptr<char[]>, std::shared_ptr<char[]>>> *to_delete;

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


static void load_data(std::fstream *file, lsm::LSMTree *lsmtree, size_t count, double delete_prop)
{
    std::string line;

    auto key_buf = std::make_unique<char[]>(lsm::key_size);
    auto val_buf = std::make_unique<char[]>(lsm::value_size);
    
    for (size_t i=0; i<count; i++) {
        if (!next_record(file, key_buf.get(), val_buf.get())) {
            break;
        }

        lsmtree->append(key_buf.get(), val_buf.get(), false, g_rng);

        if (gsl_rng_uniform(g_rng) < std::max(delete_prop, .25)) {
            auto del_key_buf = new char[lsm::key_size]();
            auto del_val_buf = new char[lsm::value_size]();
            memcpy(del_key_buf, key_buf.get(), lsm::key_size);
            memcpy(del_val_buf, val_buf.get(), lsm::value_size);
            to_delete->insert({std::shared_ptr<char[]>(del_key_buf), std::shared_ptr<char[]>(del_val_buf)});
        }
    }
}


static std::pair<lsm::key_type, lsm::key_type> sample_range(lsm::key_type min, lsm::key_type max, double selectivity)
{
    size_t range_length = (max - min) * selectivity;

    lsm::key_type max_bottom = max - range_length;
    lsm::key_type bottom = gsl_rng_uniform_int(g_rng, max_bottom);

    return std::pair<lsm::key_type, lsm::key_type> {bottom, bottom + range_length};
}


static void reset_lsm_perf_metrics() {
    lsm::sample_range_time = 0;
    lsm::alias_time = 0;
    lsm::alias_query_time = 0;
    lsm::memtable_sample_time = 0;
    lsm::memlevel_sample_time = 0;
    lsm::disklevel_sample_time = 0;
    lsm::rejection_check_time = 0;

    lsm::sampling_rejections = 0;
    lsm::sampling_attempts = 0;

    RESET_IO_CNT(); 
}

static bool benchmark(lsm::LSMTree *tree, std::fstream *file, 
                      size_t inserts, size_t samples, size_t sample_size, 
                      size_t min_key, size_t max_key, double selectivity,
                      double delete_prop) {
    // for looking at the insert time distribution
    size_t insert_batch_size = 100;
    std::vector<size_t> insert_times;
    insert_times.reserve(inserts / insert_batch_size);

    bool out_of_data = false;

    size_t inserted_records = 0;
    std::vector<std::pair<std::shared_ptr<char[]>, std::shared_ptr<char[]>>> to_insert(insert_batch_size);

    size_t deletes = inserts * delete_prop;
    std::vector<std::pair<std::shared_ptr<char[]>, std::shared_ptr<char[]>>> del_vec;
    std::sample(to_delete->begin(), to_delete->end(), std::back_inserter(del_vec), deletes, std::mt19937{std::random_device{}()});

    size_t applied_deletes = 0;
    while (inserted_records < inserts && !out_of_data) {
        size_t inserted_from_batch = 0;
        for (size_t i=0; i<insert_batch_size; i++) {
            auto key_buf = new char[lsm::key_size]();
            auto val_buf = new char[lsm::value_size]();
            if (!next_record(file, key_buf, val_buf)) {
                    // If no new records were loaded, there's no reason to duplicate
                    // the last round of sampling.
                    if (i == 0) {
                        delete[] key_buf;
                        delete[] val_buf;
                        return false;
                    }

                    // Otherwise, we'll mark that we've reached the end, and sample one
                    // last time before ending.
                    out_of_data = true;
                    delete[] key_buf;
                    delete[] val_buf;
                    break;
                }
            inserted_records++;
            inserted_from_batch++;
            to_insert[i] = {std::shared_ptr<char[]>(key_buf), std::shared_ptr<char[]>(val_buf)};

            if (gsl_rng_uniform(g_rng) < delete_prop) {
                to_delete->insert(to_insert[i]);
            }
        }

        auto insert_start = std::chrono::high_resolution_clock::now();
        for (size_t i=0; i<inserted_from_batch; i++) {
            if (applied_deletes<deletes && gsl_rng_uniform(g_rng) < delete_prop) {
                tree->append(del_vec[applied_deletes].first.get(), del_vec[applied_deletes].second.get(), true, g_rng); 
                to_delete->erase(del_vec[applied_deletes]);
                applied_deletes++;
                i--;
            } else {
                tree->append(to_insert[i].first.get(), to_insert[i].second.get(), false, g_rng);
            }
        }
        auto insert_stop = std::chrono::high_resolution_clock::now();

        insert_times.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(insert_stop - insert_start).count());
    }

    size_t per_insert = std::accumulate(insert_times.begin(), insert_times.end(), decltype(insert_times)::value_type(0)) / (inserts + applied_deletes);

    char* buffer1 = (char*) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    char* buffer2 = (char*) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    char sample_set[sample_size*lsm::record_size];

    auto sample_start = std::chrono::high_resolution_clock::now();
    for (size_t i=0; i<samples; i++) {
        auto range = sample_range(min_key, max_key, selectivity);

        tree->range_sample(sample_set, (char*) &range.first, (char*) &range.second, sample_size, buffer1, buffer2, g_rng);
    }
    auto sample_stop = std::chrono::high_resolution_clock::now();

    auto sample_time = std::chrono::duration_cast<std::chrono::nanoseconds>(sample_stop - sample_start).count() / samples;

    fprintf(stdout, "%ld %ld %ld %ld %ld %ld %ld %ld %ld\t", tree->get_record_cnt() - tree->get_tombstone_cnt(), tree->get_tombstone_cnt(), tree->get_height(), tree->get_memory_utilization(), tree->get_aux_memory_utilization(), lsm::sampling_attempts, lsm::sampling_rejections, per_insert, sample_time);
    fprintf(stdout, "%ld %ld %ld %ld %ld %ld %ld %ld\n", lsm::sample_range_time / samples, lsm::alias_time / samples, lsm::alias_query_time / samples, lsm::memtable_sample_time / samples, lsm::memlevel_sample_time / samples, lsm::disklevel_sample_time / samples, lsm::rejection_check_time / samples, lsm::pf_read_cnt / samples);

    reset_lsm_perf_metrics();

    free(buffer1);
    free(buffer2);

    return !out_of_data;
}


int main(int argc, char **argv)
{
    if (argc < 8) {
        fprintf(stderr, "Usage: insert_bench <filename> <record_count> <memtable_size> <scale_factor> <selectivity> <memory_levels> <delete_proportion>\n"); 
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    size_t memtable_size = atol(argv[3]);
    size_t scale_factor = atol(argv[4]);
    double selectivity = atof(argv[5]);
    size_t memory_levels = atol(argv[6]);
    double delete_prop = atof(argv[7]);

    std::string root_dir = "benchmarks/data/insert_bench";

    g_rng = gsl_rng_alloc(gsl_rng_mt19937);
    to_delete = new std::set<std::pair<std::shared_ptr<char[]>, std::shared_ptr<char[]>>>();

    // use for selectivity calculations
    lsm::key_type min_key = 0;
    lsm::key_type max_key = record_count - 1;

    // initialize the random number generator
    unsigned int seed = 0;
    {
        std::fstream urandom;
        urandom.open("/dev/urandom", std::ios::in|std::ios::binary);
        urandom.read((char *) &seed, sizeof(seed));
        urandom.close();
    }
    gsl_rng_set(g_rng, seed);

    auto sampling_lsm = lsm::LSMTree(root_dir, memtable_size, 10*1024*1024, scale_factor, memory_levels, 1.0, g_rng);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    // warm up the tree with initial_insertions number of initially inserted
    // records
    size_t initial_insertions = .1 * record_count;
    load_data(&datafile, &sampling_lsm, initial_insertions, delete_prop);

    size_t sample_size = 1000;
    size_t samples = 1000;
    size_t inserts = .1 * record_count;

    while (benchmark(&sampling_lsm, &datafile, inserts, samples,
                     sample_size, min_key, max_key, selectivity, delete_prop)) {
            ;
        }

    delete to_delete;
    exit(EXIT_SUCCESS);
}
