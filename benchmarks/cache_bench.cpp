/*
 *
 */

#include <chrono>
#include <iostream>
#include <fstream>
#include <sstream>

#include "sampling/lsmtree.hpp"
#include "util/benchutil.hpp"

int main(int argc, char **argv) {

    if (argc < 4) {
        fprintf(stderr, "first_bench <record_count> <memtable_size> <scale_factor> [default_seed]\n");
        exit(EXIT_FAILURE);
    }

    size_t data_size = atoi(argv[1]);
    size_t memtable_size = atoi(argv[2]);
    size_t scale_factor = atoi(argv[3]);

    bool default_seed = true;
    if (argc > 5) {
        default_seed = atoi(argv[4]);
    }

    auto state = lsm::bench::bench_state(8, 8, 1024*200);

    if (!default_seed) {
        unsigned int seed = 0;
        std::fstream urandom;
        urandom.open("/dev/urandom", std::ios::in|std::ios::binary);
        urandom.read((char *) &seed, sizeof(seed));
        urandom.close();
        gsl_rng_set(state->rng, seed);
    }

    auto test_data = lsm::bench::random_unique_keys(data_size, 10*data_size, state.get());

    auto rng = state->rng;

    auto state_ptr = state.get();

    auto tree = lsm::sampling::LSMTree::create(memtable_size, scale_factor, std::move(state), lsm::sampling::LEVELING,
                                               true, false, 1, false);

    // load up the tree
    
    auto insert_start = std::chrono::high_resolution_clock::now();
    int64_t value = 0;
    for (size_t i=0; i<data_size; i++) {
        tree->insert((std::byte*) &test_data[i], (std::byte*) &value);
    }
    auto insert_stop = std::chrono::high_resolution_clock::now();

    size_t per_insert = std::chrono::duration_cast<std::chrono::nanoseconds>(insert_stop - insert_start).count() / data_size;

    tree->memory_utilization(true);

    size_t sample_size = 1000;
    size_t trials = 1000;

    int64_t first = gsl_rng_uniform_int(rng, 10*data_size);
    int64_t second = gsl_rng_uniform_int(rng, 10*data_size);

    int64_t lower = std::min(first, second);
    int64_t upper = std::max(first, second);

    for (size_t i=0; i<1000; i++) {
        tree->range_sample((std::byte *) &lower, (std::byte *) &upper, sample_size);
    }

    size_t total_rej = 0;
    size_t total_attempt = 0;
    long total_buffer = 0;
    long total_bounds = 0;
    long total_walker = 0;
    long total_sample = 0;
    long total_rejection = 0;
    long total_io = 0;
    size_t total_misses = 0;


    for (size_t i=0; i<trials; i++) {


        size_t rej = 0;
        size_t attempt = 0;
        long buffer_t = 0;
        long bounds_t = 0;
        long walker_t = 0;
        long sample_t = 0;
        long rejection_t = 0;

        auto sample = tree->range_sample_bench((std::byte*) &lower, (std::byte*) &upper, sample_size, &rej, &attempt, &buffer_t, &bounds_t, &walker_t, &sample_t, &rejection_t);

        total_rej += rej;
        total_attempt += attempt;
        total_buffer += buffer_t;
        total_bounds += bounds_t;
        total_walker += walker_t;
        total_sample += sample_t - state_ptr->cache->io_time();
        total_rejection += rejection_t;
        total_io += state_ptr->cache->io_time();

        total_misses += state_ptr->cache->cache_misses();
    }

    fprintf(stdout, "%ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld\n",
            data_size, tree->depth(), sample_size,
            per_insert, total_rej / trials,
            total_attempt / trials, total_misses / trials, total_buffer
            / trials, total_bounds / trials, total_walker / trials,
            total_sample / trials, total_io / trials, total_rejection / trials);
}
