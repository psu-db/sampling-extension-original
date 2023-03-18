#define ENABLE_TIMER

#include "bench.h"

size_t rejections = 0;

static lsm::record_t *sample(lsm::key_t lower, lsm::key_t upper, size_t n, size_t k, lsm::WIRSRun *data)
{
    auto state = data->get_sample_run_state(lower, upper);
    lsm::record_t *result = nullptr;
    size_t sampled = 0;
    rejections = 0;

    if (state->tot_weight == 0) {
        goto end;
    }

    result = (lsm::record_t*)malloc(k * sizeof(lsm::record_t));

    while (sampled < k) {
        size_t s = data->get_samples(state, result + sampled, lower, upper, k-sampled, nullptr, g_rng);
        rejections += (k-sampled) - s;
        sampled += s;
    }

end:
    delete state;
    return result;
}


static void benchmark(lsm::WIRSRun *data, size_t n, size_t k, size_t sample_attempts, size_t min, size_t max, double selectivity)
{
	//printf("n = %zu\n", n);
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < sample_attempts; i++) {
        auto range = get_key_range(min, max, selectivity);
        lsm::record_t *result = sample(range.first, range.second, n, k, data);
        free(result);
    }

    auto stop = std::chrono::high_resolution_clock::now();

    auto total_latency = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    double avg_latency = (double) total_latency.count() / sample_attempts;
    size_t avg_rejections = rejections / k;

    //fprintf(stderr, "Average Sample Latency (ns), Average Sample Rejections");
    printf("%zu %.0lf %ld\n\n", k, avg_latency, avg_rejections);
}

static void benchmark(lsm::WIRSRun *data, size_t n, size_t k, double selectivity, const std::vector<std::pair<size_t, size_t>>& queries)
{
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < queries.size(); i++) {
        lsm::record_t *result = sample(queries[i].first, queries[i].second, n, k, data);
        free(result);
    }

    auto stop = std::chrono::high_resolution_clock::now();

    auto total_latency = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    double avg_latency = (double) total_latency.count() / queries.size();
    size_t avg_rejections = rejections / k;

    //fprintf(stderr, "Average Sample Latency (ns)");
    printf("%zu %.0lf %ld\n", k, avg_latency, avg_rejections);
}

int main(int argc, char **argv)
{
    if (argc < 4) {
        fprintf(stderr, "Usage: static_bench <filename> <record_count> <selectivity> [query_file]\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    double selectivity = atof(argv[3]);
	
	std::vector<double> sel = {0.1, 0.05, 0.01, 0.005, 0.001, 0.0005, 0.0001};
	std::vector<std::pair<size_t, size_t>> queries[7];
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
		if (query_set == 7) return -1; 
	}

    init_bench_env(true);

    std::string root_dir = "benchmarks/data/static_bench";

    auto sampling_lsm = new lsm::LSMTree(root_dir, 10000, 10000, 10, 100, 1, 1, g_rng);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

	warmup(&datafile, sampling_lsm, record_count, 0.05);

    size_t n;
    auto data = sampling_lsm->get_flattened_wirs_run();

    delete sampling_lsm;

    fprintf(stderr, "Flattened structure obtained\n");

	if (argc == 5) {
		for (size_t sample_size = 1; sample_size < 100000; sample_size *= 10)
		    benchmark(data, n, sample_size, selectivity, queries[query_set]);
	} else {
		for (size_t sample_size = 1; sample_size < 100000; sample_size *= 10)
			benchmark(data, n, sample_size, 10000, g_min_key, g_max_key, selectivity);
	}

    delete_bench_env();
    exit(EXIT_SUCCESS);
}
