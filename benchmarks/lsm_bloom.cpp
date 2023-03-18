//#define ENABLE_TIMER

#include "bench.h"
#include "util/bf_config.h"

static void sample_benchmark(lsm::LSMTree *tree, double fpr, size_t trial_cnt, double selectivity) {
    size_t k = 10000;

    //char sample_set[k*lsm::record_size];
    lsm::record_t sample_set[k];
    
    char* buffer1 = (char*) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    char* buffer2 = (char*) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < trial_cnt; i++) {
        auto range = get_key_range(min_key, max_key, selectivity);
        tree->range_sample(sample_set, range.first, range.second, k, buffer1, buffer2, g_rng);
    }

    auto stop = std::chrono::high_resolution_clock::now();

    assert(stop > start);
    auto total_latency = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    double avg_latency = (double) total_latency.count() / trial_cnt;

    fprintf(stdout, "%lf %lf %ld %ld\n", fpr, avg_latency, tree->get_tombstone_cnt(), tree->get_aux_memory_utilization());

    free(buffer1);
    free(buffer2);
}


int main(int argc, char **argv)
{
    if (argc < 8) {
        fprintf(stderr, "Usage: lsm_bloom <filename> <record_count> <memtable_size> <scale_factor> <memory_levels> <delete_proportion> <max_delete_proportion> [osm_data]\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    size_t memtable_size = atol(argv[3]);
    size_t scale_factor = atol(argv[4]);
    size_t memory_levels = atol(argv[5]);
    double delete_prop = atof(argv[6]);
    double max_delete_prop = atof(argv[7]);
    bool use_osm = (argc == 9) ? atoi(argv[8]) : 0;

    std::string root_dir = "benchmarks/data/lsm_insert_sample";


    std::vector<double> bf_fprs = {.001, .01, .05, .1, .15, .25, .5, .75, .90};

    for (auto fpr: bf_fprs) {
        init_bench_env(record_count);
        lsm::BF_SET_FPR(fpr);
        //auto sampling_lsm = lsm::LSMTree(root_dir, memtable_size, memtable_size*max_delete_prop, scale_factor, memory_levels, max_delete_prop, 1, g_rng);
        auto sampling_lsm = lsm::LSMTree(root_dir, memtable_size, memtable_size*max_delete_prop, scale_factor, memory_levels, max_delete_prop, g_rng);

        std::fstream datafile;
        datafile.open(filename, std::ios::in);

        warmup(&datafile, &sampling_lsm, .4*record_count, 0);
        warmup(&datafile, &sampling_lsm, .6*record_count, delete_prop);
        sample_benchmark(&sampling_lsm, fpr, 10000, 0.001);

        fflush(stdout);
        fflush(stderr);

        datafile.close();
        delete_bench_env();
    }


    exit(EXIT_SUCCESS);
}
