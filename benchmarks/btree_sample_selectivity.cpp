#define ENABLE_TIMER

#include "bench.h"

static void benchmark(TreeMap *tree, size_t n, size_t k, size_t sample_attempts, size_t min, size_t max, double selectivity)
{
    std::vector<lsm::key_type> sample;
    sample.reserve(k);

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < sample_attempts; i++) {
        auto range = get_key_range(min, max, selectivity);
        tree->range_sample(range.first, range.second, k, sample, g_rng);
    }

    auto stop = std::chrono::high_resolution_clock::now();

    auto total_latency = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    double avg_latency = (double) total_latency.count() / sample_attempts;

    //fprintf(stderr, "Average Latency (ns)\n");
    printf("%lf %.0lf\n", selectivity, avg_latency);
}


int main(int argc, char **argv)
{
    if (argc < 4) {
        fprintf(stderr, "Usage: btree_bench <filename> <record_count> <sample_size>\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    double sample_size = atof(argv[3]);
    //size_t sample_size = atol(argv[4]);


    init_bench_env(true);

    // use for selectivity calculations
    lsm::key_type min_key = 0;
    lsm::key_type max_key = record_count - 1;

    auto sampling_tree = TreeMap();

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    // warm up the tree with initial_insertions number of initially inserted
    // records
    build_btree(&sampling_tree, &datafile);

	std::vector<double> sel = {0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1};
    size_t n;
	for (auto selectivity: sel)
		benchmark(&sampling_tree, n, sample_size, 10000, min_key, max_key, selectivity);

    delete_bench_env();
    exit(EXIT_SUCCESS);
}
