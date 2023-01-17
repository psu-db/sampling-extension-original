#define ENABLE_TIMER

#include "bench.h"

static bool benchmark(lsm::LSMTree *tree, std::fstream *file, 
                      size_t inserts, size_t samples, size_t sample_size, 
                      size_t min_key, size_t max_key, double selectivity,
                      double delete_prop) {
    // for looking at the insert time distribution
    size_t insert_batch_size = 100;
    std::vector<size_t> insert_times;
    insert_times.reserve(inserts / insert_batch_size);

    bool out_of_data = false;

    char* buffer1 = (char*) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    char* buffer2 = (char*) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);

    size_t inserted_records = 0;
    std::vector<shared_record> to_insert(insert_batch_size);

    size_t deletes = inserts * delete_prop;
    char delbuf[deletes*lsm::record_size];
    tree->range_sample(delbuf, (char*) &min_key, (char*) &max_key, deletes, buffer1, buffer2, g_rng);
    std::set<lsm::key_type> deleted;
    size_t applied_deletes = 0;

    while (inserted_records < inserts && !out_of_data) {
        size_t inserted_from_batch = 0;
        for (size_t i=0; i<insert_batch_size; i++) {
            auto rec = create_shared_record();
            if (!next_record(file, rec.first.get(), rec.second.get())) {
                    // If no new records were loaded, there's no reason to duplicate
                    // the last round of sampling.
                    if (i == 0) {
                        free(buffer1);
                        free(buffer2);
                        return false;
                    }

                    // Otherwise, we'll mark that we've reached the end, and sample one
                    // last time before ending.
                    out_of_data = true;
                    break;
                }
            inserted_records++;
            inserted_from_batch++;
            to_insert[i] = {rec.first, rec.second};
        }

        auto insert_start = std::chrono::high_resolution_clock::now();
        for (size_t i=0; i<inserted_from_batch; i++) {
            if (applied_deletes<deletes && gsl_rng_uniform(g_rng) < delete_prop) {
                auto key = lsm::get_key(delbuf + (applied_deletes * lsm::record_size));
                auto val = lsm::get_val(delbuf + (applied_deletes * lsm::record_size));

                if (deleted.find(*(lsm::key_type*) key) == deleted.end()) {
                    tree->append(key, val, true, g_rng); 
                    deleted.insert(*(lsm::key_type*) key);
                    applied_deletes++;
                }
            } 

            tree->append(to_insert[i].first.get(), to_insert[i].second.get(), false, g_rng);
        }
        auto insert_stop = std::chrono::high_resolution_clock::now();

        insert_times.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(insert_stop - insert_start).count());
    }

    size_t per_insert = std::accumulate(insert_times.begin(), insert_times.end(), decltype(insert_times)::value_type(0)) / (inserts + applied_deletes);

    char sample_set[sample_size*lsm::record_size];

    auto sample_start = std::chrono::high_resolution_clock::now();
    for (size_t i=0; i<samples; i++) {
        auto range = get_key_range(min_key, max_key, selectivity);
        tree->range_sample(sample_set, (char*) &range.first, (char*) &range.second, sample_size, buffer1, buffer2, g_rng);
    }

    auto sample_stop = std::chrono::high_resolution_clock::now();

    auto sample_time = std::chrono::duration_cast<std::chrono::nanoseconds>(sample_stop - sample_start).count() / samples;


    fprintf(stdout, "%ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %zu\t",
            tree->get_record_cnt() - tree->get_tombstone_cnt(),
            tree->get_tombstone_cnt(), tree->get_height(),
            tree->get_memory_utilization(), tree->get_aux_memory_utilization(),
            lsm::sampling_attempts, lsm::sampling_rejections,
            lsm::bounds_rejections, lsm::tombstone_rejections,
            lsm::deletion_rejections, per_insert, sample_time);

    fprintf(stdout, "%ld %ld %ld %ld %ld %ld %ld %ld\n",
            lsm::sample_range_time / samples, lsm::alias_time / samples,
            lsm::alias_query_time / samples,
            lsm::memtable_sample_time / samples,
            lsm::memlevel_sample_time / samples,
            lsm::disklevel_sample_time / samples,
            lsm::rejection_check_time / samples, lsm::pf_read_cnt / samples);

    reset_lsm_perf_metrics();

    free(buffer1);
    free(buffer2);

    return !out_of_data;
}


int main(int argc, char **argv)
{
    if (argc < 9) {
        fprintf(stderr, "Usage: insert_bench <filename> <record_count> <memtable_size> <scale_factor> <selectivity> <memory_levels> <delete_proportion> <max_delete_proportion> [insert_batch_proportion]\n");
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

    std::string root_dir = "benchmarks/data/default_bench";
    init_bench_env(true);

    // use for selectivity calculations
    lsm::key_type min_key = 0;
    lsm::key_type max_key = record_count - 1;

    auto sampling_lsm = lsm::LSMTree(root_dir, memtable_size, memtable_size*3, scale_factor, memory_levels, max_delete_prop, g_rng);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    // warm up the tree with initial_insertions number of initially inserted
    // records
    size_t initial_insertions = insert_batch * record_count;
    warmup(&datafile, &sampling_lsm, initial_insertions, delete_prop);

    size_t sample_size = 1000;
    size_t samples = 1000;
    size_t inserts = insert_batch * record_count;

    size_t total_inserts = initial_insertions;

    fprintf(stderr, "Record Count, Tombstone Count, Tree Height, Memory Utilization, Auxiliary Memory Utilization, Average Sample Attempts, Average Sample Rejections, ");
    fprintf(stderr, "Average Bounds Rejections, Average Tombstone Rejections, Average Deletion Rejections, Average Insert Latency (ns), Average Sample Latency (ns), ");
    fprintf(stderr, "Average Sample Range Construction Latency (ns), Average Alias Query Latency (ns), Average MemTable Sampling Latency (ns), Average MemLevel Sampling Latency (ns), ");
    fprintf(stderr, "Average DiskLevel Sampling Latency (ns), Average Rejection Check Time (ns)\n");
    while (benchmark(&sampling_lsm, &datafile, inserts, samples,
                     sample_size, min_key, max_key, selectivity, delete_prop)) {
        total_inserts += inserts;

        if (total_inserts + inserts > record_count) {
            inserts = record_count - total_inserts;
        }

        if (total_inserts >= record_count) {
            break;
        }
    }

    delete_bench_env();
    exit(EXIT_SUCCESS);
}
