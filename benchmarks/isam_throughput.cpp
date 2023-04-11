#define ENABLE_TIMER

#include "bench.h"

static void benchmark(lsm::ISAMTree *data, size_t k, const std::vector<std::pair<size_t, size_t>>& queries)
{
    char *buf = (char*) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);

    lsm::record_t *sample_buff = (lsm::record_t *) std::aligned_alloc(lsm::SECTOR_SIZE, k*sizeof(lsm::record_t));
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < queries.size(); i++) {
        auto lb = data->get_lower_bound(queries[i].first, buf);
        auto ub = data->get_upper_bound(queries[i].first, buf);
        size_t range_len = (ub - lb) * (lsm::PAGE_SIZE / sizeof(lsm::record_t));

        lsm::PageNum buffered_page = lsm::INVALID_PNUM;
        size_t j=0;
        while (j < k) {
            size_t idx = gsl_rng_uniform_int(g_rng, range_len);
            const lsm::record_t *rec = data->sample_record(lb, idx, buf, buffered_page);

            if (rec->key >= queries[i].first && rec->key <= queries[i].second) {
                sample_buff[j++] = *rec;
            }
        }
    }

    auto stop = std::chrono::high_resolution_clock::now();

    free(buf);
    free(sample_buff);

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
    bool use_osm = (argc == 5) ? atoi(argv[4]) : false;
	
	std::vector<double> sel = {0.1, 0.05, 0.01, 0.005, 0.001, 0.0005, 0.0001};
	std::vector<std::pair<size_t, size_t>> queries[6];
	size_t query_set = 6;

    FILE* fp = fopen(argv[3], "r");
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

    init_bench_env(record_count, true, use_osm);

    std::string root_dir = "benchmarks/data/isam_throughput";

    // use for selectivity calculations
    lsm::key_t min_key = 0;
    lsm::key_t max_key = record_count - 1;

    auto sampling_lsm = lsm::LSMTree(root_dir, 1000000, 50000, 10, 100, 1, g_rng);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    //build_lsm_tree(&sampling_lsm, &datafile);
	warmup(&datafile, &sampling_lsm, record_count, 0.05);

    auto data = sampling_lsm.get_flat_isam_tree(g_rng);
    benchmark(data, 1000, queries[4]);

    /*
	if (argc == 5) {
		for (size_t sample_size = 1; sample_size < 100000; sample_size *= 10)
		    benchmark(data, n, sample_size, selectivity, queries[query_set]);
	} else {
		for (size_t sample_size = 1; sample_size < 100000; sample_size *= 10)
			benchmark(data, n, sample_size, 10000, min_key, max_key, selectivity);
	}
       */

    delete_bench_env();
    exit(EXIT_SUCCESS);
}
