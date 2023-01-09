#define ENABLE_TIMER

#include "bench.h"

static char *sample(char *lower, char *upper, size_t n, size_t k, char *data)
{
    char *result = (char*)malloc(k * lsm::record_size);

    size_t start = 0, end = 0;

    size_t low = 0, high = n - 1;
    while (low <= high) {
        size_t mid = (low + high) / 2;

        const char *key = lsm::get_key(data + mid * lsm::record_size);

        if (lsm::key_cmp(key, lower) < 0)
            low = mid + 1;
        else
            high = mid - 1;
    }

    start = low;

    low = 0, high = n - 1;
    while (low <= high) {
        size_t mid = (low + high) / 2;

        const char *key = lsm::get_key(data + mid * lsm::record_size);

        if (lsm::key_cmp(key, upper) > 0)
            high = mid - 1;
        else
            low = mid + 1;
    }

    end = high;

    for (size_t i = 0; i < k; i++) {
        size_t idx = gsl_rng_uniform_int(g_rng, end - start + 1) + start;
        memcpy(result + i * lsm::record_size, data + idx * lsm::record_size, lsm::record_size);
    }

    return result;
}


static void benchmark(char *data, size_t n, size_t k, size_t sample_attempts, size_t min, size_t max, double selectivity)
{
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < sample_attempts; i++) {
        auto range = get_key_range(min, max, selectivity);
        char *result = sample((char*) &range.first, (char *) &range.second, n, k, data);
        free(result);
    }

    auto stop = std::chrono::high_resolution_clock::now();

    auto total_latency = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    double avg_latency = (double) total_latency.count() / sample_attempts;

    fprintf(stderr, "Average Sample Latency (ns)");
    printf("%.0lf\n", avg_latency);
}


int main(int argc, char **argv)
{
    if (argc < 5) {
        fprintf(stderr, "Usage: static_bench <filename> <record_count> <selectivity> <sample_size>\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    double selectivity = atof(argv[3]);
    size_t sample_size = atol(argv[4]);

    init_bench_env(true);

    std::string root_dir = "benchmarks/data/static_bench";

    // use for selectivity calculations
    lsm::key_type min_key = 0;
    lsm::key_type max_key = record_count - 1;

    auto sampling_lsm = lsm::LSMTree(root_dir, 1000000, 3000000, 10, 100, 1, g_rng);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    build_lsm_tree(&sampling_lsm, &datafile);

    size_t n;
    auto data = sampling_lsm.get_sorted_array(&n, g_rng);

    benchmark(data, n, sample_size, 10000, min_key, max_key, selectivity);

    delete_bench_env();
    exit(EXIT_SUCCESS);
}
