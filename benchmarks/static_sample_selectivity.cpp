#define ENABLE_TIMER

#include "bench.h"

static lsm::record_t *sample(const lsm::key_t& lower, const lsm::key_t& upper, size_t n, size_t k, lsm::record_t *data)
{
    //char *result = (char*)malloc(k * lsm::record_size);
    lsm::record_t* result = (lsm::record_t*)malloc(k * sizeof(lsm::record_t));

    size_t start = 0, end = 0;

	//printf("first binary search...\n");
    size_t low = 0, high = n;
    while (low < high) {
        size_t mid = (low + high) / 2;
		//printf("low = %zu high = %zu mid = %zu\n", low, high, mid);

        if (data[mid].key < lower)
            low = mid + 1;
        else
            high = mid;
    }

    start = low;

	//printf("second binary search...\n");
    low = 0, high = n;
    while (low < high) {
        size_t mid = (low + high) / 2;
		//printf("low = %zu high = %zu mid = %zu\n", low, high, mid);
		//printf("mid = %zu\n", mid);

        if (data[mid].key <= upper)
			low = mid + 1;
        else
            high = mid;
    }

    end = high;

	//printf("start = %zu, end = %zu\n", start, end);
	//printf("lower = %zu, upper = %zu\n", *((size_t*)lower), *((size_t*)upper));
	if (end == start) return result;

    for (size_t i = 0; i < k; i++) {
        size_t idx = gsl_rng_uniform_int(g_rng, end - start) + start;
		//printf("idx = %zu\n", idx);
        result[i] = data[idx];
    }

    return result;
}


static void benchmark(lsm::record_t *data, size_t n, size_t k, size_t sample_attempts, size_t min, size_t max, double selectivity)
{
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < sample_attempts; i++) {
        auto range = get_key_range(min, max, selectivity);
        lsm::record_t *result = sample(range.first, range.second, n, k, data);
        free(result);
    }

    auto stop = std::chrono::high_resolution_clock::now();

    auto total_latency = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    double avg_latency = (double) total_latency.count() / sample_attempts;

    //fprintf(stderr, "Average Sample Latency (ns)");
    printf("%lf %.0lf\n", selectivity, avg_latency);
}

static void benchmark(lsm::record_t *data, size_t n, size_t k, double selectivity, const std::vector<std::pair<size_t, size_t>>& queries)
{
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < queries.size(); i++) {
        lsm::record_t *result = sample(queries[i].first, queries[i].second, n, k, data);
        free(result);
    }

    auto stop = std::chrono::high_resolution_clock::now();

    auto total_latency = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    double avg_latency = (double) total_latency.count() / queries.size();

    //fprintf(stderr, "Average Sample Latency (ns)");
    printf("%lf %.0lf\n", selectivity, avg_latency);
}


int main(int argc, char **argv)
{
    if (argc < 4) {
        fprintf(stderr, "Usage: static_bench <filename> <record_count> <sample_size>\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    //double selectivity = atof(argv[3]);
    size_t sample_size = atol(argv[3]);
	std::vector<double> sel = {0.1, 0.05, 0.01, 0.001, 0.0005, 0.0001};
	std::vector<std::pair<size_t, size_t>> queries[6];

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

		//for (size_t i = 0; i < 6; ++i) printf("%zu\n", queries[i].size());
	}

    init_bench_env(true);

    std::string root_dir = "benchmarks/data/static_bench";

    // use for selectivity calculations
    lsm::key_t min_key = 0;
    lsm::key_t max_key = record_count - 1;

    auto sampling_lsm = lsm::LSMTree(root_dir, 1000000, 50000, 10, 100, 1, g_rng);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    //build_lsm_tree(&sampling_lsm, &datafile);
	warmup(&datafile, &sampling_lsm, record_count, 0.05);

    size_t n;
    auto data = sampling_lsm.get_sorted_array(&n, g_rng);

	if (argc == 5) {
		for (size_t i = 0; i < 6; ++i)
			benchmark(data, n, sample_size, sel[i], queries[i]);
	} else {
		for (auto selectivity: sel)
			benchmark(data, n, sample_size, 10000, min_key, max_key, selectivity);
	}

    delete_bench_env();
    exit(EXIT_SUCCESS);
}
