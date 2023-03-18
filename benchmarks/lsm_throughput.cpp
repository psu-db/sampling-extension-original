#include "bench.h"

size_t g_insert_batch_size = 1000;

static bool insert_benchmark(lsm::LSMTree *tree, std::fstream *file, 
                      size_t insert_cnt, double delete_prop) {

    size_t delete_cnt = insert_cnt * delete_prop;
    size_t delete_batch_size = g_insert_batch_size * delete_prop * 15;
    size_t delete_idx = delete_batch_size;

    //char *delbuf = new char[delete_batch_size * lsm::record_size]();
    lsm::record_t* delbuf = new lsm::record_t[delete_batch_size]();

    std::set<lsm::key_t> deleted;

    size_t applied_deletes = 0;
    size_t applied_inserts = 0;

    std::vector<record> insert_vec;
    insert_vec.reserve(g_insert_batch_size);
    bool continue_benchmark = true;
    char *buf1 = (char *) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    char *buf2 = (char *) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);

    size_t total_time = 0;

    while (applied_inserts < insert_cnt && continue_benchmark) { 
        continue_benchmark = build_insert_vec(file, insert_vec, g_insert_batch_size);

        if (insert_vec.size() == 0) {
            break;
        }

        // if we've fully processed the delete vector, sample a new
        // set of records to delete.
        if (delete_idx > delete_batch_size) {
            tree->range_sample(delbuf, min_key, max_key, delete_batch_size, buf1, buf2, g_rng);
            deleted.clear();
        }

        progress_update((double) applied_inserts / (double) insert_cnt, "inserting:");
        size_t local_inserted = 0;
        size_t local_deleted = 0;
        
        auto insert_start = std::chrono::high_resolution_clock::now();
        for (size_t i=0; i<insert_vec.size(); i++) {
            // process a delete if necessary
            if (applied_deletes < delete_cnt && delete_idx < delete_batch_size && gsl_rng_uniform(g_rng) < delete_prop) {
                auto key = delbuf[delete_idx].key;
                auto val = delbuf[delete_idx].value;

                delete_idx++;

                if (deleted.find(key) == deleted.end()) {
                    tree->append(key, val, true, g_rng);
                    deleted.insert(key);
                    local_deleted++;
                }
            }
            // insert the record;
            tree->append(insert_vec[i].first, insert_vec[i].second, false, g_rng);
            local_inserted++;
        }
        auto insert_stop = std::chrono::high_resolution_clock::now();

        applied_deletes += local_deleted;
        applied_inserts += local_inserted;
        total_time += std::chrono::duration_cast<std::chrono::nanoseconds>(insert_stop - insert_start).count();
    } 

    progress_update(1.0, "inserting:");
    size_t throughput = (((double) (applied_inserts + applied_deletes) / (double) total_time) * 1e9);

    fprintf(stdout, "%ld\n", throughput);

    reset_lsm_perf_metrics();
    delete[] delbuf;
    free(buf1);
    free(buf2);

    return continue_benchmark;
}

static void sample_benchmark(lsm::LSMTree *tree, size_t k, size_t trial_cnt, double selectivity)
{
    char* buffer1 = (char*) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    char* buffer2 = (char*) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);

    lsm::record_t sample_set[k];
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < trial_cnt; i++) {
        auto range = get_key_range(min_key, max_key, selectivity);
        tree->range_sample(sample_set, range.first, range.second, k, buffer1, buffer2, g_rng);
    }

    auto stop = std::chrono::high_resolution_clock::now();
    auto total_latency = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
    size_t throughput = ((double)(trial_cnt * k) / (double) total_latency) * 1e9;

    free(buffer1);
    free(buffer2);
    fprintf(stdout, "%zu %.0ld\n", k, throughput);
}

static void sample_benchmark(lsm::LSMTree *tree, size_t k, double selectivity, const std::vector<std::pair<size_t, size_t>>& queries)
{
    char progbuf[25];
    sprintf(progbuf, "sampling (%ld):", k);
    char* buffer1 = (char*) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    char* buffer2 = (char*) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);

    lsm::record_t sample_set[k];
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < queries.size(); i++) {
        tree->range_sample(sample_set, queries[i].first, queries[i].second, k, buffer1, buffer2, g_rng);
    }

    auto stop = std::chrono::high_resolution_clock::now();

    size_t total_latency = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();

    size_t throughput = ((double)(queries.size() * k) / (double) total_latency) * 1e9;

    free(buffer1);
    free(buffer2);
    fprintf(stdout, "%zu %.0ld\n", k, throughput);
}

int main(int argc, char **argv)
{
    if (argc < 8) {
        fprintf(stderr, "Usage: lsm_insert_sample <filename> <record_count> <memtable_size> <scale_factor> <memory_levels> <delete_proportion> <max_delete_proportion> [osm_data]\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    size_t memtable_size = atol(argv[3]);
    size_t scale_factor = atol(argv[4]);
    size_t memory_levels = atol(argv[5]);
    double delete_prop = atof(argv[6]);
    double max_delete_prop = atof(argv[7]);
    
    std::vector<double> sel = {0.1, 0.05, 0.01, 0.001, 0.0005, 0.0001};
    std::vector<std::pair<size_t, size_t>> queries[6];
    size_t query_set = 6;

    if (argc == 9) {
        FILE* fp = fopen(argv[8], "r");
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

    double insert_batch = 0.1; 

    std::string root_dir = "benchmarks/data/lsm_insert_sample";

    init_bench_env(record_count);

    auto sampling_lsm = lsm::LSMTree(root_dir, memtable_size, memtable_size*max_delete_prop, scale_factor, memory_levels, max_delete_prop, g_rng);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    // warm up the tree with initial_insertions number of initially inserted
    // records
    size_t warmup_cnt = insert_batch * record_count;
    warmup(&datafile, &sampling_lsm, warmup_cnt, delete_prop);

    size_t insert_cnt = record_count - warmup_cnt;

    insert_benchmark(&sampling_lsm, &datafile, insert_cnt, delete_prop);

    if (argc == 9) {
        sample_benchmark(&sampling_lsm, 1000, 0.001, queries[query_set]);
    } else {
        sample_benchmark(&sampling_lsm, 1000, 10000, 0.001);
    }
    

    /*
    size_t max_sample_size = 1000000;
    for (size_t sample_size =1; sample_size < max_sample_size; sample_size *= 10) {
        sample_benchmark(&sampling_lsm, sample_size, 10000);
    }
    */

    delete_bench_env();
    fflush(stdout);
    fflush(stderr);

    exit(EXIT_SUCCESS);
}
