#define ENABLE_TIMER

#include "bench.h"

static void benchmark(lsm::LSMTree *tree, size_t k, size_t trial_cnt)
{
    char sample_set[k*lsm::record_size];

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < trial_cnt; i++) {
        tree->range_sample(sample_set, k, g_rng);
    }

    auto stop = std::chrono::high_resolution_clock::now();

    auto total_latency = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    double avg_latency = (double) total_latency.count() / trial_cnt;

    //fprintf(stderr, "Average Sample Latency (ns)\n");
    printf("%zu %.0lf\n", k, avg_latency);
}


int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "Usage: lsm_sample <filename> <record_count>\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);

    std::string root_dir = "benchmarks/data/lsm_sample";

    init_bench_env(true);

    auto sampling_tree = lsm::LSMTree(root_dir, 15000, 45000, 10, 1000, 1, g_rng);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    //build_lsm_tree(&sampling_tree, &datafile);
	warmup(&datafile, &sampling_tree, record_count, .05);

    size_t n;
	for (size_t sample_size = 1; sample_size < 100000; sample_size *= 10)
	    benchmark(&sampling_tree, sample_size, 10000);

    delete_bench_env();
    exit(EXIT_SUCCESS);
}
