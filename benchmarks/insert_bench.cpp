#include "lsm/LsmTree.h"

#include <cstdlib>
#include <cstdio>
#include <chrono>
#include <algorithm>
#include <numeric>

#include <iostream>
#include <fstream>
#include <sstream>
#include <unordered_set>
#include <string>

gsl_rng *g_rng;

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


static void load_data(std::fstream *file, lsm::LSMTree *lsmtree, size_t count)
{
    std::string line;

    auto key_buf = std::make_unique<char[]>(lsm::key_size);
    auto val_buf = std::make_unique<char[]>(lsm::value_size);
    
    for (size_t i=0; i<count; i++) {
        if (!next_record(file, key_buf.get(), val_buf.get())) {
            break;
        }

        lsmtree->append(key_buf.get(), val_buf.get(), false, g_rng);
    }
}


static std::pair<lsm::key_type, lsm::key_type> sample_range(lsm::key_type min, lsm::key_type max, double selectivity)
{
    size_t range_length = (max - min) * selectivity;

    lsm::key_type max_bottom = max - range_length;
    lsm::key_type bottom = gsl_rng_uniform_int(g_rng, max_bottom);

    return std::pair<lsm::key_type, lsm::key_type> {bottom, bottom + range_length};
}


static bool benchmark(lsm::LSMTree *tree, std::fstream *file, 
                      size_t inserts, size_t samples, size_t sample_size, 
                      size_t min_key, size_t max_key, double selectivity) {
    // for looking at the insert time distribution
    std::vector<size_t> insert_times;
    insert_times.reserve(inserts);

    auto key_buf = std::make_unique<char[]>(lsm::key_size);
    auto val_buf = std::make_unique<char[]>(lsm::value_size);

    bool out_of_data = false;

    for (size_t i=0; i<inserts; i++) {
        if (!next_record(file, key_buf.get(), val_buf.get())) {
            // If no new records were loaded, there's no reason to duplicate
            // the last round of sampling.
            if (i == 0) {
                return false;
            }

            // Otherwise, we'll mark that we've reached the end, and sample one
            // last time before ending.
            out_of_data = true;
            break;
        }

        auto insert_start = std::chrono::high_resolution_clock::now();
        tree->append(key_buf.get(), val_buf.get(), false, g_rng);
        auto insert_stop = std::chrono::high_resolution_clock::now();

        insert_times.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(insert_stop - insert_start).count());
    }

    size_t per_insert = std::accumulate(insert_times.begin(), insert_times.end(), decltype(insert_times)::value_type(0)) / inserts;

    char* buffer1 = (char*) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    char* buffer2 = (char*) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    char sample_set[sample_size*lsm::record_size];

    auto sample_start = std::chrono::high_resolution_clock::now();
    for (size_t i=0; i<samples; i++) {
        auto range = sample_range(min_key, max_key, selectivity);

        tree->range_sample(sample_set, (char*) &range.first, (char*) &range.second, sample_size, buffer1, buffer2, g_rng);
    }
    auto sample_stop = std::chrono::high_resolution_clock::now();

    auto sample_time = std::chrono::duration_cast<std::chrono::nanoseconds>(sample_stop - sample_start).count() / sample_size;

    fprintf(stdout, "%ld %ld %ld %ld %ld\n", tree->get_record_cnt(), lsm::sampling_attempts, lsm::sampling_rejections, per_insert, sample_time);

    lsm::sampling_rejections = 0;
    lsm::sampling_attempts = 0;

    free(buffer1);
    free(buffer2);

    return !out_of_data;
}


int main(int argc, char **argv)
{
    if (argc < 7) {
        fprintf(stderr, "Usage: insert_bench <filename> <record_count> <memtable_size> <scale_factor> <selectivity> <memory_levels>\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    size_t memtable_size = atol(argv[3]);
    size_t scale_factor = atol(argv[4]);
    double selectivity = atof(argv[5]);
    size_t memory_levels = atol(argv[6]);

    std::string root_dir = "benchmarks/data/insert_bench";

    g_rng = gsl_rng_alloc(gsl_rng_mt19937);

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

    auto sampling_lsm = lsm::LSMTree(root_dir, memtable_size, 100, scale_factor, memory_levels, 1.0, g_rng);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    // warm up the tree with initial_insertions number of initially inserted
    // records
    size_t initial_insertions = .1 * record_count;
    load_data(&datafile, &sampling_lsm, initial_insertions);

    size_t sample_size = 1000;
    size_t samples = 1000;
    size_t inserts = .1 * record_count;

    while (benchmark(&sampling_lsm, &datafile, inserts, samples,
                     sample_size, min_key, max_key, selectivity)) {
            ;
        }

    exit(EXIT_SUCCESS);
}
