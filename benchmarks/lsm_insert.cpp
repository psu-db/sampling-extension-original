#define ENABLE_TIMER

#include "bench.h"

size_t g_insert_batch_size = 100;

static bool benchmark(lsm::LSMTree *tree, std::fstream *file, 
                      size_t inserts, double delete_prop) {
    // for looking at the insert time distribution
    std::vector<size_t> insert_times;
    insert_times.reserve(inserts / g_insert_batch_size);

    bool out_of_data = false;

    size_t deletes = inserts * delete_prop;
    lsm::record_t *delbuf = new lsm::record_t[deletes]();
    tree->range_sample(delbuf, deletes, g_rng);
    std::set<lsm::key_t> deleted;
    size_t applied_deletes = 0;

    std::vector<record> insert_vec;
    bool continue_benchmark = build_insert_vec(file, insert_vec, inserts);

    /*
     * Benchmark timing variables
     */
    size_t avg_insert_latency;

    if (insert_vec.size() == 0) {
        goto end;
    }

    {
        size_t inserted = 0;
        while (inserted < insert_vec.size()) {
            auto insert_start = std::chrono::high_resolution_clock::now();
            for (size_t i=0; i<g_insert_batch_size; i++) {
                // process a delete if necessary
                if (applied_deletes < deletes && gsl_rng_uniform(g_rng) < delete_prop) {
                    //auto key = lsm::get_key(delbuf + (applied_deletes * lsm::record_size));
                    auto key = delbuf[applied_deletes].key;
                    //auto val = lsm::get_val(delbuf + (applied_deletes * lsm::record_size));
                    auto val = delbuf[applied_deletes].value;
                    //auto weight = lsm::get_weight(delbuf + (applied_deletes * lsm::record_size));
                    auto weight = delbuf[applied_deletes].weight;

                    if (deleted.find(key) == deleted.end()) {
                        tree->append(key, val, weight, true, g_rng);
                        deleted.insert(key);
                        applied_deletes++;
                    }
                }
                // insert the record;
                tree->append(insert_vec[i].key, insert_vec[i].value, insert_vec[i].weight, false, g_rng);
                inserted++;
            }
            auto insert_stop = std::chrono::high_resolution_clock::now();
            insert_times.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(insert_stop - insert_start).count() / g_insert_batch_size );
        }
    }

    avg_insert_latency = std::accumulate(insert_times.begin(), insert_times.end(), decltype(insert_times)::value_type(0)) / (inserts + applied_deletes);

    for (size_t i=0; i<insert_times.size(); i++) {
        fprintf(stdout, "%ld\n", insert_times[i]);
    }

end:
    reset_lsm_perf_metrics();
    delete[] delbuf;

    return continue_benchmark;
}


int main(int argc, char **argv)
{
    if (argc < 8) {
        fprintf(stderr, "Usage: lsm_insert <filename> <record_count> <memtable_size> <scale_factor> <memory_levels> <delete_proportion> <max_delete_proportion> [insert_batch_proportion]\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    size_t memtable_size = atol(argv[3]);
    size_t scale_factor = atol(argv[4]);
    size_t memory_levels = atol(argv[5]);
    double delete_prop = atof(argv[6]);
    double max_delete_prop = atof(argv[7]);
    double insert_batch = (argc == 9) ? atof(argv[8]) : 0.1;

    std::string root_dir = "benchmarks/data/insert_bench";

    init_bench_env(record_count, true);

    auto sampling_lsm = lsm::LSMTree(root_dir, memtable_size, memtable_size*max_delete_prop, scale_factor, memory_levels, max_delete_prop, 100, g_rng);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    // warm up the tree with initial_insertions number of initially inserted
    // records
    size_t initial_insertions = insert_batch * record_count;
    warmup(&datafile, &sampling_lsm, initial_insertions, delete_prop);

    size_t inserts = insert_batch * record_count;

    size_t total_inserts = initial_insertions;

    fprintf(stderr, "Insert Latency (ns) [Averaged over %ld inserts]\n", g_insert_batch_size);

    while (benchmark(&sampling_lsm, &datafile, inserts, delete_prop))
        ;

    delete_bench_env();
    exit(EXIT_SUCCESS);
}
