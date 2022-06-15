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
    size_t sample_total = 0;

    for (size_t i=0; i<1000; i++) {
        int64_t first = gsl_rng_uniform_int(rng, 10*data_size);
        int64_t second = gsl_rng_uniform_int(rng, 10*data_size);

        int64_t lower = std::min(first, second);
        int64_t upper = std::max(first, second);

        auto sample_start = std::chrono::high_resolution_clock::now();
        auto sample = tree->range_sample((std::byte*) &lower, (std::byte*) &upper, sample_size);
        auto sample_stop = std::chrono::high_resolution_clock::now();

        sample_total += std::chrono::duration_cast<std::chrono::nanoseconds>(sample_stop - sample_start).count();
    }

    size_t per_sample = sample_total / 1000;

    fprintf(stderr, "%ld\n", per_sample);
}
