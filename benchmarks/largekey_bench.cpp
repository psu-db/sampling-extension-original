/*
 *
 */

#include <chrono>
#include <numeric>

#include "sampling/lsmtree.hpp"
#include "util/benchutil.hpp"

int main(int argc, char **argv) {

    if (argc < 6) {
        fprintf(stderr, "largekey_bench <record_count> <memtable_size> <scale_factor> <unsorted_memtable> <bloom_filters>\n");
        exit(EXIT_FAILURE);
    }

    size_t data_size = atoi(argv[1]);
    size_t memtable_size = atoi(argv[2]);
    size_t scale_factor = atoi(argv[3]);
    bool unsorted_memtable = atoi(argv[4]);
    bool bloom_filters = atoi(argv[5]);

    size_t key_size = 256;
    size_t val_size = 512;

    auto state = lsm::bench::bench_state(key_size, val_size);
    auto test_keys = lsm::bench::random_unique_keys(data_size, 10*data_size, state.get());
    auto test_values = lsm::bench::random_unique_keys(data_size, 10*data_size, state.get());

    auto rng = state->rng;

    auto state_ptr = state.get();

    auto tree = lsm::sampling::LSMTree::create(memtable_size, scale_factor, std::move(state), lsm::sampling::LEVELING,
                                               bloom_filters, false, 1, unsorted_memtable);

    // load up the tree
    std::byte key_buffer[key_size];
    std::byte val_buffer[val_size];

    std::vector<size_t> insertion_times(data_size);

    for (size_t i=0; i<data_size; i++) {
        memcpy(key_buffer, &test_keys[i], sizeof(int64_t));
        memcpy(val_buffer, &test_values[i], sizeof(int64_t));

        auto insert_start = std::chrono::high_resolution_clock::now();
        tree->insert(key_buffer, val_buffer);
        auto insert_stop = std::chrono::high_resolution_clock::now();

        insertion_times[i] = std::chrono::duration_cast<std::chrono::nanoseconds>(insert_stop - insert_start).count();
    }

    size_t per_insert = std::accumulate(insertion_times.begin(), insertion_times.end(), decltype(insertion_times)::value_type(0)) / data_size;

    size_t sample_size = 1000;
    size_t trials = 1000;

    size_t total_rej = 0;
    size_t total_attempt = 0;
    long total_buffer = 0;
    long total_bounds = 0;
    long total_walker = 0;
    long total_sample = 0;
    long total_rejection = 0;
    size_t total_misses = 0;

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
        long rejection_t = 0;

        std::byte lower_buffer[key_size];
        std::byte upper_buffer[key_size];

        memcpy(lower_buffer, &lower, sizeof(int64_t));
        memcpy(upper_buffer, &upper, sizeof(int64_t));

        auto sample = tree->range_sample_bench(lower_buffer, upper_buffer, sample_size, &rej, &attempt, &buffer_t, &bounds_t, &walker_t, &sample_t, &rejection_t);

        total_rej += rej;
        total_attempt += attempt;
        total_buffer += buffer_t;
        total_bounds += bounds_t;
        total_walker += walker_t;
        total_sample += sample_t;
        total_rejection += rejection_t;

        total_misses += state_ptr->cache->cache_misses();
    }

    fprintf(stdout, "%ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld\n", data_size, tree->depth(), sample_size, per_insert, total_rej / trials, total_attempt / trials, total_misses / trials, total_buffer / trials, total_bounds / trials, total_walker / trials, total_sample / trials, total_rejection / trials);
}
