#define ENABLE_TIMER

#include "bench.h"

static bool benchmark(lsm::LSMTree *tree, std::fstream *file, size_t inserts,
                      size_t samples, size_t sample_size, double selectivity,
                      double delete_prop) { 
    // for looking at the insert time distribution
    size_t insert_batch_size = 100;
    std::vector<size_t> insert_times;
    insert_times.reserve(inserts / insert_batch_size);

    // determine records to be deleted over the course of the
    // run. NOTE: This setup means that records inserted during
    // this benchmark phase won't be deleted. So it's a bit of a
    // "worst case" situation in terms of tombstone proportions.
    size_t deletes = inserts * delete_prop;
    char *delbuf = new char[deletes * lsm::record_size]();
    tree->range_sample(delbuf, deletes, g_rng);
    std::set<lsm::key_type> deleted;
    size_t applied_deletes = 0;

    std::vector<shared_record> insert_vec;
    bool continue_benchmark = build_insert_vec(file, insert_vec, inserts);

    /* 
     * Benchmark Timing variables
     */
    size_t avg_insert_latency = 0;
    size_t avg_sample_latency = 0;

    // If there aren't any new records to insert, the sampling
    // bench will be identical to the last iteration, so we can
    // end early.
    if (insert_vec.size() == 0) {
        goto end;
    }

    // Insertion benchmarking
    {
        auto insert_start = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < insert_vec.size(); i++) {
            // process a delete if necessary
            if (applied_deletes < deletes && gsl_rng_uniform(g_rng) < delete_prop) {
                auto key = lsm::get_key(delbuf + (applied_deletes * lsm::record_size));
                auto val = lsm::get_val(delbuf + (applied_deletes * lsm::record_size));
                auto weight = lsm::get_weight(delbuf + (applied_deletes * lsm::record_size));

                if (deleted.find(*(lsm::key_type *)key) == deleted.end()) {
                    tree->append(key, val, weight, true, g_rng);
                    deleted.insert(*(lsm::key_type *)key);
                    applied_deletes++;
                }
            }

            // insert the record;
            tree->append(insert_vec[i].key.get(), insert_vec[i].value.get(), insert_vec[i].weight, false, g_rng);
        }

        auto insert_stop = std::chrono::high_resolution_clock::now();
        insert_times.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>
                             (insert_stop - insert_start).count());

        avg_insert_latency = std::accumulate(insert_times.begin(), insert_times.end(),
                                          decltype(insert_times)::value_type(0)) /
                          (insert_vec.size() + applied_deletes);
    }

    // Sample benchmarking
    {
        char sample_set[sample_size * lsm::record_size];

        auto sample_start = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < samples; i++) {
            tree->range_sample(sample_set, sample_size, g_rng);
        }

        auto sample_stop = std::chrono::high_resolution_clock::now();

        avg_sample_latency = std::chrono::duration_cast<std::chrono::nanoseconds>
                                       (sample_stop - sample_start).count() / samples;
    }

    fprintf(stdout, "%ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %zu\t",
          tree->get_record_cnt() - tree->get_tombstone_cnt(),
          tree->get_tombstone_cnt(), tree->get_height(),
          tree->get_memory_utilization(), tree->get_aux_memory_utilization(),
          lsm::sampling_attempts, lsm::sampling_rejections,
          lsm::bounds_rejections, lsm::tombstone_rejections,
          lsm::deletion_rejections, avg_insert_latency, avg_sample_latency);

    fprintf(stdout, "%ld %ld %ld %ld %ld %ld %ld %ld\n",
          lsm::memtable_alias_time / samples, lsm::alias_time / samples,
          lsm::alias_query_time / samples, lsm::memtable_sample_time / samples,
          lsm::memlevel_sample_time / samples,
          lsm::disklevel_sample_time / samples,
          lsm::rejection_check_time / samples, lsm::pf_read_cnt / samples);

end:
    reset_lsm_perf_metrics();
    delete[] delbuf;

    return continue_benchmark;
}

int main(int argc, char **argv)
{
    if (argc < 9) {
        fprintf(stderr, "Usage: lsm_bench <filename> <record_count> <memtable_size> <scale_factor> <selectivity> <memory_levels> <delete_proportion> <max_delete_proportion> [insert_batch_proportion]\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    size_t memtable_size = atol(argv[3]);
    size_t scale_factor = atol(argv[4]);
    double selectivity = atof(argv[5]);
    size_t memory_levels = atol(argv[6]);
    double delete_prop = atof(argv[7]);
    double max_delete_prop = atof(argv[8]);
    double insert_batch = (argc == 10) ? atof(argv[9]) : 0.1;

    std::string root_dir = "benchmarks/data/lsm_bench";
    init_bench_env(record_count, true);

    auto sampling_lsm = lsm::LSMTree(root_dir, memtable_size, memtable_size*3, scale_factor, memory_levels, max_delete_prop, 100, g_rng);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    // warm up the tree with initial_insertions number of initially inserted
    // records
    size_t initial_insertions = insert_batch * record_count;
    warmup(&datafile, &sampling_lsm, initial_insertions, delete_prop);

    size_t sample_size = 1000;
    size_t samples = 1000;
    size_t inserts = insert_batch * record_count;

    fprintf(stderr, "Record Count, Tombstone Count, Tree Height, Memory Utilization, Auxiliary Memory Utilization, Average Sample Attempts, Average Sample Rejections, ");
    fprintf(stderr, "Average Bounds Rejections, Average Tombstone Rejections, Average Deletion Rejections, Average Insert Latency (ns), Average Sample Latency (ns), ");
    fprintf(stderr, "Average Memtable Alias Construction Latency (ns), Average Alias Query Latency (ns), Average MemTable Sampling Latency (ns), Average MemLevel Sampling Latency (ns), ");
    fprintf(stderr, "Average DiskLevel Sampling Latency (ns), Average Rejection Check Time (ns)\n");
    while (benchmark(&sampling_lsm, &datafile, inserts, samples, sample_size, selectivity, delete_prop)) 
        ;

    delete_bench_env();
    exit(EXIT_SUCCESS);
}
