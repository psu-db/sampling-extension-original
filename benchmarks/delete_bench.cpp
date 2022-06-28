/*
 *
 */

#include <chrono>

#include "sampling/lsmtree.hpp"
#include "util/benchutil.hpp"

int main(int argc, char **argv) {

    if (argc < 6) {
        fprintf(stderr, "delete_bench <record_count> <memtable_size> <scale_factor> <unsorted_memtable> <delete_prop>\n");
        exit(EXIT_FAILURE);
    }

    size_t data_size = atoi(argv[1]);
    size_t memtable_size = atoi(argv[2]);
    size_t scale_factor = atoi(argv[3]);
    bool unsorted_memtable = atoi(argv[4]);
    double delete_prop = atof(argv[5]);

    auto state = lsm::bench::bench_state();
    auto test_data = lsm::bench::random_unique_keys(data_size, 10*data_size, state.get());

    auto state_ptr = state.get();

    auto rng = state->rng;

    auto tree = lsm::sampling::LSMTree::create(memtable_size, scale_factor, std::move(state), lsm::sampling::LEVELING,
                                               false, false, 1, unsorted_memtable);

    // load up the tree, including deleting some elements

    auto insert_start = std::chrono::high_resolution_clock::now();
    int64_t value = 0;
    lsm::Timestamp time = 1;
    for (size_t i=0; i<data_size; i++) {
        auto op = gsl_rng_uniform(rng);
        if (op > delete_prop || i < memtable_size) {
            tree->insert((std::byte*) &test_data[i], (std::byte*) &value, time++);
        } else {
            auto idx = gsl_rng_uniform_int(rng, i-1);
            tree->remove((std::byte *) &test_data[idx], (std::byte*) &value, time++);
        }
    }
    auto insert_stop = std::chrono::high_resolution_clock::now();

    size_t per_insert = std::chrono::duration_cast<std::chrono::nanoseconds>(insert_stop - insert_start).count() / data_size;

    size_t sample_size = 1000;
    size_t trials = 1000;

    size_t total_rej = 0;
    size_t total_attempt = 0;
    long total_buffer = 0;
    long total_bounds = 0;
    long total_walker = 0;
    long total_sample = 0;
    long total_rejection = 0;

    for (size_t i=0; i<trials; i++) {
        int64_t first = gsl_rng_uniform_int(rng, 10*data_size);
        int64_t second = gsl_rng_uniform_int(rng, 10*data_size);

        int64_t lower = std::min(first, second);
        int64_t upper = std::max(first, second);

        size_t rej = 0;
        size_t attempt = 0;
        long buffer_t = 0;
        long bounds_t = 0;
        long walker_t = 0;
        long sample_t = 0;
        long reject_t = 0;

        auto sample = tree->range_sample_bench((std::byte*) &lower, (std::byte*) &upper, sample_size, &rej, &attempt, &buffer_t, &bounds_t, &walker_t, &sample_t, &reject_t);

        total_rej += rej;
        total_attempt += attempt;
        total_buffer += buffer_t;
        total_bounds += bounds_t;
        total_walker += walker_t;
        total_sample += sample_t;
        total_rejection += reject_t;
    }

    size_t avg_cache_miss = state_ptr->cache->cache_misses() / trials;

    fprintf(stdout, "%ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld\n", data_size, tree->depth(), sample_size, per_insert, total_rej / trials, total_attempt / trials, avg_cache_miss, total_buffer / trials, total_bounds / trials, total_walker / trials, total_sample / trials, total_rejection / trials);
}
