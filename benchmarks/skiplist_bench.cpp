/*
 *
 */

#include <chrono>

#include "sampling/lsmtree.hpp"
#include "util/benchutil.hpp"

int main(int argc, char **argv) {

    if (argc < 2) {
        fprintf(stderr, "skip_bench <record_count>\n");
        exit(EXIT_FAILURE);
    }

    size_t data_size = atoi(argv[1]);

    size_t table_size = data_size + .1 * data_size;

    auto state = lsm::bench::bench_state();
    auto test_data = lsm::bench::random_unique_keys(data_size, 10*data_size, state.get());

    auto rng = state->rng;
    auto table = lsm::ds::MapMemTable(table_size, state.get());

    // load up the table
    auto insert_start = std::chrono::high_resolution_clock::now();
    int64_t value = 0;
    for (size_t i=0; i<data_size; i++) {
        table.insert((std::byte*) &test_data[i], (std::byte*) &value);
    }
    auto insert_stop = std::chrono::high_resolution_clock::now();

    size_t per_insert = std::chrono::duration_cast<std::chrono::nanoseconds>(insert_stop - insert_start).count() / data_size;

    size_t trials = 10000;
    auto sample_bounds = std::vector<std::pair<int64_t, int64_t>>(trials);

    // precalculate sample bounds
    for (size_t i=0; i<trials; i++) {
        int64_t first = gsl_rng_uniform_int(rng, 10*data_size);
        int64_t second = gsl_rng_uniform_int(rng, 10*data_size);

        sample_bounds[i] = { std::min(first, second), std::max(first, second)};
    }

    size_t iter_time = 0;
    size_t bounds_time = 0;

    // Generate sample range benchmark 
    for (size_t i=0; i<trials; i++) {
        size_t bounds_t = 0;
        size_t iter_t = 0;
        auto range = table.get_sample_range_bench((std::byte *) &sample_bounds[i].first, (std::byte *) &sample_bounds[i].second, &bounds_t, &iter_t);

        iter_time += iter_t;
        bounds_time += bounds_t;
    }

    fprintf(stdout, "%ld %ld %ld %ld %ld\n", data_size, table_size, per_insert, bounds_time / trials, iter_time / trials);
}
