#include "sampling/lsmtree.hpp"
#include "util/benchutil.hpp"

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

static bool next_record(std::fstream *file, std::byte *key, std::byte *val)
{
    std::string line;
    if (std::getline(*file, line, '\n')) {
        std::stringstream line_stream(line);
        std::string key_field;
        std::string value_field;

        std::getline(line_stream, value_field, '\t');
        std::getline(line_stream, key_field, '\t');

        *((int64_t*) key) = atol(key_field.c_str());
        *((int64_t*) val) = atol(value_field.c_str());
        return true;
    }

    key = nullptr;
    val = nullptr;
    return false;
}


static void load_data(std::fstream *file, lsm::sampling::LSMTree *lsmtree, size_t count, size_t key_len, size_t val_len) 
{
    std::string line;

    auto key_buf = std::make_unique<std::byte[]>(key_len);
    auto val_buf = std::make_unique<std::byte[]>(val_len);
    
    for (size_t i=0; i<count; i++) {
        if (!next_record(file, key_buf.get(), val_buf.get())) {
            break;
        }

        lsmtree->insert(key_buf.get(), val_buf.get());
    }
}


static std::pair<int64_t, int64_t> sample_range(int64_t min, int64_t max, double selectivity, lsm::global::g_state *state)
{
    size_t range_length = (max - min) * selectivity;

    int64_t max_bottom = max - range_length;
    int64_t bottom = gsl_rng_uniform_int(state->rng, max_bottom);

    return std::pair<int64_t, int64_t> {bottom, bottom + range_length};
}


static bool benchmark(lsm::sampling::LSMTree *tree, std::fstream *file, 
                      size_t inserts, size_t samples, size_t sample_size, 
                      size_t min_key, size_t max_key, double selectivity) {
    // for looking at the insert time distribution
    std::vector<size_t> insert_times;
    insert_times.reserve(inserts);

    auto key_buf = std::make_unique<std::byte[]>(tree->schema()->key_len());
    auto val_buf = std::make_unique<std::byte[]>(tree->schema()->val_len());

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
        tree->insert(key_buf.get(), val_buf.get());
        auto insert_stop = std::chrono::high_resolution_clock::now();

        insert_times.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(insert_stop - insert_start).count());
    }

    size_t per_insert = std::accumulate(insert_times.begin(), insert_times.end(), decltype(insert_times)::value_type(0)) / inserts;

    long sample_time = 0;
    long walker_time = 0;
    long bounds_time = 0;
    long buffer_time = 0;
    long reject_time = 0;
    long io_time = 0;

    size_t rejections = 0;
    size_t attempts = 0;
    size_t cache_misses = 0;

    size_t total_rejections = 0;
    size_t total_attempts = 0;

    for (size_t i=0; i<samples; i++) {
        auto range = sample_range(min_key, max_key, selectivity, tree->global_state());

        tree->range_sample_bench((std::byte *) &range.first,
                                 (std::byte *) &range.second,
                                 sample_size, &rejections,
                                 &attempts, &buffer_time, &bounds_time,
                                 &walker_time, &sample_time, &reject_time);

        io_time += tree->cache()->io_time();
        cache_misses += tree->cache()->cache_misses();

        total_rejections += rejections;
        total_attempts += attempts;
    }

    fprintf(stdout, "%ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld\n",
            tree->record_count(), tree->depth(), sample_size, per_insert,
            total_rejections / samples, total_attempts / samples, cache_misses / samples,
            buffer_time / samples, bounds_time / samples, walker_time /
            samples, sample_time / samples, io_time / samples, reject_time /
            samples);

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

    // change these to enlarge the data size
    //size_t key_size = sizeof(int64_t);
    size_t key_size = 128;
    size_t val_size = 256;

    // use for selectivity calculations
    int64_t min_key = 0;
    int64_t max_key = record_count - 1;

    auto state = lsm::bench::bench_state(key_size, val_size);

    unsigned int seed = 0;
    {
        std::fstream urandom;
        urandom.open("/dev/urandom", std::ios::in|std::ios::binary);
        urandom.read((char *) &seed, sizeof(seed));
        urandom.close();
    }

    gsl_rng_set(state->rng, seed);
    auto flags = lsm::sampling::F_LSM_BLOOM;
    auto sampling_lsm = lsm::sampling::LSMTree::create(memtable_size, scale_factor, std::move(state), flags, lsm::sampling::LEVELING, memory_levels);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    // warm up the tree with initial_insertions number of initially inserted
    // records
    size_t initial_insertions = .1 * record_count;
    load_data(&datafile, sampling_lsm.get(), initial_insertions, key_size, val_size);

    size_t sample_size = 1000;
    size_t samples = 1000;
    size_t inserts = .1 * record_count;

    while (benchmark(sampling_lsm.get(), &datafile, inserts, samples,
                     sample_size, min_key, max_key, selectivity)) {
            ;
        }

    exit(EXIT_SUCCESS);
}
