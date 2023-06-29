#include "bench.h"

size_t g_insert_batch_size = 10000;

static bool delete_benchmark(lsm::LSMTree *tree, std::fstream *file, size_t delete_cnt) {

    lsm::record_t* delbuf = new lsm::record_t[delete_cnt]();

    std::set<lsm::record_t> to_delete;

    size_t applied_deletes = 0;
    size_t applied_inserts = 0;

    size_t total_time = 0;

    char *buf1 = (char *) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    char *buf2 = (char *) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);

    tree->range_sample(delbuf, g_min_key, g_max_key, delete_cnt, buf1, buf2, g_rng);

    // de-duplicate delete buffer
    for (size_t i=0; i<delete_cnt; i++) {
        to_delete.insert(delbuf[i]);
    }

    auto delete_start = std::chrono::high_resolution_clock::now();
    for (auto &rec : to_delete) {
        if (lsm::DELETE_TAGGING) {
            tree->delete_record(rec.key, rec.value, g_rng);
        } else {
            tree->append(rec.key, rec.value, true, g_rng);
        }
    }
    auto delete_stop = std::chrono::high_resolution_clock::now();
    total_time = std::chrono::duration_cast<std::chrono::nanoseconds>(delete_stop - delete_start).count();

    auto latency = total_time / to_delete.size();

    fprintf(stdout, "%ld\n", latency);
    fflush(stdout);

    free(buf1);
    free(buf2);

    reset_lsm_perf_metrics();
    delete[] delbuf;

    return false;
}


int main(int argc, char **argv)
{
    if (argc < 7) {
        fprintf(stderr, "Usage: lsm_throughput <filename> <record_count> <memtable_size> <scale_factor> <delete_cnt> <delete_proportion> <max_delete_proportion> [osm_data]\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    size_t memtable_size = atol(argv[3]);
    size_t scale_factor = atol(argv[4]);
    size_t delete_cnt = atol(argv[5]);
    double delete_prop = atof(argv[6]);
    double max_delete_prop = atof(argv[7]);
    bool use_osm = (argc == 9) ? atoi(argv[8]) : 0;
    
    std::string root_dir = "benchmarks/data/lsm_delete";

    init_bench_env(record_count, true, use_osm);

    auto sampling_lsm = lsm::LSMTree(root_dir, memtable_size, memtable_size*max_delete_prop, scale_factor, 1000, max_delete_prop, g_rng);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    warmup(&datafile, &sampling_lsm, record_count, delete_prop);

    delete_benchmark(&sampling_lsm, &datafile, delete_cnt);
    
    delete_bench_env();
    fflush(stdout);
    fflush(stderr);

    exit(EXIT_SUCCESS);
}
