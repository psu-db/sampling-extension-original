/*
 *
 */

#include <chrono>

#include "sampling/lsmtree.hpp"
#include "util/benchutil.hpp"

int main(int argc, char **argv) {

    if (argc < 3) {
        fprintf(stderr, "first_bench <record_count> <memtable_size> <scale_factor>\n");
        exit(EXIT_FAILURE);
    }

    size_t data_size = atoi(argv[1]);
    size_t memtable_size = atoi(argv[2]);
    size_t scale_factor = atoi(argv[3]);

    auto state = lsm::bench::bench_state();
    auto test_data = lsm::bench::random_unique_keys(data_size, 10*data_size, state.get());

    auto rng = state->rng;

    auto tree = lsm::sampling::LSMTree::create(memtable_size, scale_factor, std::move(state));

    // load up the tree
    
    auto insert_start = std::chrono::high_resolution_clock::now();
    int64_t value = 0;
    for (size_t i=0; i<data_size; i++) {
        tree->insert((std::byte*) &test_data[i], (std::byte*) &value);
    }
    auto insert_stop = std::chrono::high_resolution_clock::now();

    fprintf(stderr, "%ld\n", tree->depth());

    size_t per_insert = std::chrono::duration_cast<std::chrono::nanoseconds>(insert_stop - insert_start).count() / data_size;
    fprintf(stderr, "%ld\n", per_insert);

    size_t sample_size = 1000;
    size_t trials = 1000;

    size_t total_rej = 0;
    size_t total_attempt = 0;
    long total_buffer = 0;
    long total_bounds = 0;
    long total_walker = 0;
    long total_sample = 0;

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

        auto sample = tree->range_sample_bench((std::byte*) &lower, (std::byte*) &upper, sample_size, &rej, &attempt, &buffer_t, &bounds_t, &walker_t, &sample_t);

        total_rej += rej;
        total_attempt += attempt;
        total_buffer += buffer_t;
        total_bounds += bounds_t;
        total_walker += walker_t;
        total_sample += sample_t;
    }

    fprintf(stdout, "%ld %ld %ld %ld %ld %ld\n", total_rej / trials, total_attempt / trials, total_buffer / trials, total_bounds / trials, total_walker / trials, total_sample / trials);
}
