#define ENABLE_TIMER

#include "bench.h"

static void benchmark(lsm::WIRSRun *data, size_t k, const std::vector<std::pair<size_t, size_t>>& queries)
{
    auto start = std::chrono::high_resolution_clock::now();

    lsm::record_t *result = new lsm::record_t[k];
    for (int i = 0; i < queries.size(); i++) {
        auto state = data->get_sample_run_state(queries[i].first, queries[i].second);
        data->get_samples(state, result, queries[i].first, queries[i].second, k, nullptr, g_rng);
        delete state;
    }
    delete[] result;

    auto stop = std::chrono::high_resolution_clock::now();

    auto total_latency = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
    size_t throughput = (((double)(queries.size() * k) / (double) total_latency) * 1e9);

    fprintf(stdout, "%.0ld\n", throughput);
}

int main(int argc, char **argv)
{
    if (argc < 4) {
        fprintf(stderr, "Usage: static_throughput <filename> <record_count> <query_file> [osm_data]\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    bool use_osm = (argc == 5) ? atoi(argv[4]) : 0;
	
	std::vector<double> sel = {0.1, 0.05, 0.01, 0.005, 0.001, 0.0005, 0.0001};
	std::vector<std::pair<size_t, size_t>> queries[7];
	size_t query_set = 4;

    FILE* fp = fopen(argv[3], "r");
    size_t cnt = 0;
    size_t offset = 0;
    size_t start, end;
    double selectivity;
    while (EOF != fscanf(fp, "%zu%zu%lf", &start, &end, &selectivity)) {
        if (start < end && std::abs(selectivity - sel[offset]) / sel[offset] < 0.1)
            queries[offset].emplace_back(start, end);
        ++cnt;
        if (cnt % 100 == 0) ++offset;
    }
    fclose(fp);

    /*
    selectivity = 0.001;
    for (size_t i = 0; i < 6; ++i)
        if (selectivity == sel[i]) query_set = i;
    if (query_set == 6) return -1; 
    */

    init_bench_env(record_count, true, use_osm);

    std::string root_dir = "benchmarks/data/static_throughput";

    auto sampling_lsm = lsm::LSMTree(root_dir, 1000000, 50000, 10, 100, 1, 1, g_rng);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

	warmup(&datafile, &sampling_lsm, record_count, 0.05);

    size_t n;
    auto data = sampling_lsm.get_flattened_wirs_run();
    benchmark(data, 1000, queries[query_set]);

    delete data;
    delete_bench_env();
    exit(EXIT_SUCCESS);
}
