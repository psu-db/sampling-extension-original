#define ENABLE_TIMER

#include "bench.h"

static void benchmark(lsm::LSMTree *tree, size_t k, size_t trial_cnt, size_t min, size_t max, double selectivity)
{
    char* buffer1 = (char*) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    char* buffer2 = (char*) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    char sample_set[k*lsm::record_size];

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < trial_cnt; i++) {
        auto range = get_key_range(min, max, selectivity);
        tree->range_sample(sample_set, (char*) &range.first, (char*) &range.second, k, buffer1, buffer2, g_rng);
    }

    auto stop = std::chrono::high_resolution_clock::now();

    auto total_latency = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    double avg_latency = (double) total_latency.count() / trial_cnt;

    free(buffer1);
    free(buffer2);

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

    std::string root_dir = "benchmarks/data/sample_bench";

    init_bench_env(true);

    // use for selectivity calculations
    lsm::key_type min_key = 0;
    lsm::key_type max_key = record_count - 1;

    auto sampling_tree = lsm::LSMTree(root_dir, 10000, 30000, 10, 1000, 1, g_rng);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    build_lsm_tree(&sampling_tree, &datafile);

    size_t n;
    benchmark(&sampling_tree, sample_size, 10000, min_key, max_key, selectivity);

    delete_bench_env();
    exit(EXIT_SUCCESS);
}
