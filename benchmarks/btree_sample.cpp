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
    fprintf(stdout, "%zu %.0lf\n", k, avg_latency);
}

static void benchmark(TreeMap *tree, size_t n, size_t k, double selectivity, const std::vector<std::pair<size_t, size_t>>& queries)
{
    std::vector<lsm::key_type> sample;
    sample.reserve(k);

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < queries.size(); i++) {
        tree->range_sample(queries[i].first, queries[i].second, k, sample, g_rng);
    }

    auto stop = std::chrono::high_resolution_clock::now();

    auto total_latency = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    double avg_latency = (double) total_latency.count() / queries.size();

    //fprintf(stderr, "Average Latency (ns)\n");
    printf("%zu %.0lf\n", k, avg_latency);
}


int main(int argc, char **argv)
{
    if (argc < 4) {
        fprintf(stderr, "Usage: btree_bench <filename> <record_count> <selectivity> <sample_size>\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    double selectivity = atof(argv[3]);
    //size_t sample_size = atol(argv[4]);

	std::vector<double> sel = {0.1, 0.05, 0.01, 0.001, 0.0005, 0.0001};
	std::vector<std::pair<size_t, size_t>> queries[6];
	size_t query_set = 6;

	if (argc == 5) {
		FILE* fp = fopen(argv[4], "r");
		size_t cnt = 0;
		size_t offset = 0;
		double selectivity;
		size_t start, end;
		while (EOF != fscanf(fp, "%zu%zu%lf", &start, &end, &selectivity)) {
			if (start < end && std::abs(selectivity - sel[offset]) / sel[offset] < 0.1)
				queries[offset].emplace_back(start, end);
			++cnt;
			if (cnt % 100 == 0) ++offset;
		}
		fclose(fp);
		for (size_t i = 0; i < 6; ++i)
			if (selectivity == sel[i]) query_set = i;
		if (query_set == 6) return -1; 
	}

    init_bench_env(true);

    // use for selectivity calculations
    lsm::key_type min_key = 0;
    lsm::key_type max_key = record_count - 1;

    auto sampling_tree = TreeMap();

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    // warm up the tree with initial_insertions number of initially inserted
    // records
	warmup(&datafile, &sampling_tree, record_count, 0.05);

    size_t n;
	if (argc == 5) {
		for (size_t sample_size = 1; sample_size < 100000; sample_size *= 10)
		    benchmark(&sampling_tree, n, sample_size, selectivity, queries[query_set]);
	} else {
		for (size_t sample_size = 1; sample_size < 100000; sample_size *= 10)
			benchmark(&sampling_tree, n, sample_size, 10000, min_key, max_key, selectivity);
	}

    delete_bench_env();
    exit(EXIT_SUCCESS);
}
