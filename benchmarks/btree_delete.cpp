#include "bench.h"

size_t g_insert_batch_size = 10000;

static bool delete_benchmark(TreeMap *tree, std::fstream *file, size_t delete_cnt) {

    std::vector<lsm::key_t> delbuf;

    std::set<lsm::key_t> to_delete;

    size_t applied_deletes = 0;
    size_t applied_inserts = 0;

    size_t total_time = 0;

    char *buf1 = (char *) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    char *buf2 = (char *) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);

    tree->range_sample(g_min_key, g_max_key, delete_cnt, delbuf, g_rng);

    // de-duplicate delete buffer
    for (size_t i=0; i<delete_cnt; i++) {
        to_delete.insert(delbuf[i]);
    }

    auto delete_start = std::chrono::high_resolution_clock::now();
    for (auto &key : to_delete) {
        tree->erase_one(key);
    }
    auto delete_stop = std::chrono::high_resolution_clock::now();
    total_time = std::chrono::duration_cast<std::chrono::nanoseconds>(delete_stop - delete_start).count();

    auto latency = total_time / to_delete.size();

    fprintf(stdout, "%ld\n", latency);
    fflush(stdout);

    free(buf1);
    free(buf2);

    reset_lsm_perf_metrics();

    return false;
}

int main(int argc, char **argv)
{
    if (argc < 5) {
        fprintf(stderr, "Usage: btree_delete <filename> <record_count> <delete_cnt> <delete_proportion> [osm_data]\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    size_t delete_count = atol(argv[3]);
    double delete_prop = atof(argv[4]);
    bool use_osm = (argc == 6) ? atoi(argv[5]) : 0;

    init_bench_env(record_count, true, use_osm);

    auto sampling_tree = TreeMap();

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    // warm up the tree with initial_insertions number of initially inserted
    // records
    warmup(&datafile, &sampling_tree, record_count, delete_prop);

    delete_benchmark(&sampling_tree, &datafile, delete_count);

    delete_bench_env();
    fflush(stdout);
    fflush(stderr);

    exit(EXIT_SUCCESS);
}
