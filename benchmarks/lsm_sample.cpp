#define ENABLE_TIMER

#include "bench.h"

static void benchmark(lsm::LSMTree *tree, size_t k, size_t trial_cnt, double selectivity)
{
    char* buffer1 = (char*) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    char* buffer2 = (char*) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    char sample_set[k*lsm::record_size];

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < trial_cnt; i++) {
        auto range = get_key_range(min_key, max_key, selectivity);
        tree->range_sample(sample_set, (char*) &range.first, (char*) &range.second, k, buffer1, buffer2, g_rng);
    }

    auto stop = std::chrono::high_resolution_clock::now();

    auto total_latency = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    double avg_latency = (double) total_latency.count() / trial_cnt;

    free(buffer1);
    free(buffer2);

    //fprintf(stderr, "Average Sample Latency (ns)\n");
    printf("%zu %.0lf\n", k, avg_latency);
}

static void benchmark(lsm::LSMTree *tree, size_t k, double selectivity, const std::vector<std::pair<size_t, size_t>>& queries)
{
    char* buffer1 = (char*) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    char* buffer2 = (char*) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    char sample_set[k*lsm::record_size];
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < queries.size(); i++) {
        tree->range_sample(sample_set, (char*) &queries[i].first, (char *) &queries[i].second, k, buffer1, buffer2, g_rng);
    }

    auto stop = std::chrono::high_resolution_clock::now();

    auto total_latency = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    double avg_latency = (double) total_latency.count() / queries.size();

    //fprintf(stderr, "Average Sample Latency (ns)");
    printf("%zu %.0lf\n", k, avg_latency);
}


int main(int argc, char **argv)
{
    if (argc < 4) {
        fprintf(stderr, "Usage: static_bench <filename> <record_count> <selectivity>\n");
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
	

    std::string root_dir = "benchmarks/data/sample_bench";

    init_bench_env(true);

    auto sampling_tree = lsm::LSMTree(root_dir, 15000, 750, 10, 1000, 1, g_rng);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    //build_lsm_tree(&sampling_tree, &datafile);
	warmup(&datafile, &sampling_tree, record_count, .05);

    size_t n;
	if (argc == 5) {
		for (size_t sample_size = 1; sample_size < 100000; sample_size *= 10)
		    benchmark(&sampling_tree, sample_size, selectivity, queries[query_set]);
	} else {
		for (size_t sample_size = 1; sample_size < 100000; sample_size *= 10)
		    benchmark(&sampling_tree, sample_size, 10000, selectivity);
	}

    delete_bench_env();
    exit(EXIT_SUCCESS);
}
