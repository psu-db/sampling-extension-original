#define ENABLE_TIMER

#include "bench.h"

size_t g_insert_batch_size = 100;
size_t g_insert_phase = 0;
size_t g_sample_phase = 0;

static bool insert_benchmark(lsm::LSMTree *tree, std::fstream *file, 
                      size_t inserts, double delete_prop) {
    // for looking at the insert time distribution
    std::vector<size_t> insert_times;
    insert_times.reserve(inserts / g_insert_batch_size);

    bool out_of_data = false;

    char *buf1 = (char *) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    char *buf2 = (char *) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);

    size_t inserted_records = 0;
    std::vector<record> to_insert(g_insert_batch_size);

    size_t deletes = inserts * delete_prop;
    //char *delbuf = new char[deletes * lsm::record_size]();
    lsm::record_t delbuf[deletes];
    tree->range_sample(delbuf, min_key, max_key, deletes, buf1, buf2, g_rng);
    std::set<lsm::key_t> deleted;
    size_t applied_deletes = 0;


    while (inserted_records < inserts && !out_of_data) {
        progress_update((double) inserted_records / (double) inserts, "Insert Phase " + std::to_string(g_insert_phase));
        size_t inserted_from_batch = 0;
        for (size_t i=0; i<g_insert_batch_size; i++) {
            record rec;
            if (!next_record(file, rec.first, rec.second)) {
                    // If no new records were loaded, there's no reason to duplicate
                    // the last round of sampling.
                    if (i == 0) {
                        free(buf1);
                        free(buf2);
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
            size_t batch_deletes = 0;
            if (applied_deletes<deletes && gsl_rng_uniform(g_rng) < delete_prop) {
                auto key = delbuf[applied_deletes].key;
                auto val = delbuf[applied_deletes].value;

                if (deleted.find(*(lsm::key_t*) key) == deleted.end()) {
                    tree->append(key, val, true, g_rng); 
                    deleted.insert(*(lsm::key_t*) key);
                    applied_deletes++;
                    batch_deletes++;
                }
            } 

            tree->append(to_insert[i].first, to_insert[i].second, false, g_rng);
        }
        auto insert_stop = std::chrono::high_resolution_clock::now();

        insert_times.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(insert_stop - insert_start).count() / (inserted_from_batch + applied_deletes));
    }

    size_t per_insert = std::accumulate(insert_times.begin(), insert_times.end(), decltype(insert_times)::value_type(0)) / (insert_times.size());

    for (size_t i=0; i<insert_times.size(); i++) {
        fprintf(stdout, "%ld\n", insert_times[i]);
    }

    g_insert_phase++;
    reset_lsm_perf_metrics();

    free(buf1);
    free(buf2);
    return !out_of_data;
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

    auto total_latency = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    double avg_latency = (double) total_latency.count() / trial_cnt;

    free(buffer1);
    free(buffer2);

    //fprintf(stderr, "Average Sample Latency (ns)\n");
    printf("%zu %.0lf\n", k, avg_latency);
}

static void sample_benchmark(lsm::LSMTree *tree, size_t k, double selectivity, const std::vector<std::pair<size_t, size_t>>& queries)
{
    char* buffer1 = (char*) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    char* buffer2 = (char*) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    //char sample_set[k*lsm::record_size];
    lsm::record_t sample_set[k];
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < queries.size(); i++) {
        tree->range_sample(sample_set, queries[i].first, queries[i].second, k, buffer1, buffer2, g_rng);
    }

    auto stop = std::chrono::high_resolution_clock::now();

    auto total_latency = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    double avg_latency = (double) total_latency.count() / queries.size();

    //fprintf(stderr, "Average Sample Latency (ns)");
    printf("%zu %.0lf\n", k, avg_latency);
}



int main(int argc, char **argv)
{
    if (argc < 8) {
        fprintf(stderr, "Usage: insert_bench <filename> <record_count> <memtable_size> <scale_factor> <memory_levels> <delete_proportion> <max_delete_proportion> <selectivity> [insert_batch_proportion]\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    size_t memtable_size = atol(argv[3]);
    size_t scale_factor = atol(argv[4]);
    size_t memory_levels = atol(argv[5]);
    double delete_prop = atof(argv[6]);
    double max_delete_prop = atof(argv[7]);
    double selectivity = atof(argv[8]);
    double insert_batch = (argc == 10) ? atof(argv[9]) : 0.1;

    std::string root_dir = "benchmarks/data/lsm_insert_sample_bench";

    init_bench_env(true);

    // use for selectivity calculations
    lsm::key_t min_key = 0;
    lsm::key_t max_key = record_count - 1;

    auto sampling_lsm = lsm::LSMTree(root_dir, memtable_size, memtable_size*0.05, scale_factor, memory_levels, max_delete_prop, g_rng);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    // warm up the tree with initial_insertions number of initially inserted
    // records
    size_t initial_insertions = insert_batch * record_count;
    warmup(&datafile, &sampling_lsm, initial_insertions, delete_prop);

    size_t inserts = insert_batch * record_count;

    size_t total_inserts = initial_insertions;

    fprintf(stderr, "Insert Latency (ns) [Averaged over %ld inserts]\n", g_insert_batch_size);

    while (insert_benchmark(&sampling_lsm, &datafile, inserts, delete_prop)) {
        total_inserts += inserts;

        if (total_inserts + inserts > record_count) {
            inserts = record_count - total_inserts;
        }

        if (total_inserts >= record_count) {
            break;
        }
    }


    size_t max_sample_size = 100000;
    for (size_t sample_size = 1; sample_size < 100000; sample_size *= 10) {
        sample_benchmark(&sampling_lsm, sample_size, 10000, selectivity);
    }

    delete_bench_env();
    exit(EXIT_SUCCESS);
}
