#define ENABLE_TIMER
#define INSTRUMENT_MERGING

#include "bench.h"

size_t g_insert_batch_size = 1000;

static bool insert_benchmark(lsm::LSMTree *tree, std::fstream *file, 
                      size_t insert_cnt, double delete_prop) {

    size_t delete_cnt = insert_cnt * delete_prop;
    size_t delete_batch_size = g_insert_batch_size * delete_prop * 15;
    size_t delete_idx = delete_batch_size;

    lsm::record_t* delbuf = new lsm::record_t[delete_batch_size]();

    std::set<lsm::key_t> deleted;

    size_t applied_deletes = 0;
    size_t applied_inserts = 0;

    std::vector<record> insert_vec;
    insert_vec.reserve(g_insert_batch_size);
    bool continue_benchmark = true;

    size_t total_time = 0;

    while (applied_inserts < insert_cnt && continue_benchmark) { 
        continue_benchmark = build_insert_vec(file, insert_vec, g_insert_batch_size);

        if (insert_vec.size() == 0) {
            break;
        }

        // if we've fully processed the delete vector, sample a new
        // set of records to delete.
        if (delete_idx > delete_batch_size) {
            tree->range_sample(delbuf, delete_batch_size, g_rng);
            deleted.clear();
        }

        size_t local_inserted = 0;
        size_t local_deleted = 0;
        
        auto insert_start = std::chrono::high_resolution_clock::now();
        for (size_t i=0; i<insert_vec.size(); i++) {
            // process a delete if necessary
            if (applied_deletes < delete_cnt && delete_idx < delete_batch_size && gsl_rng_uniform(g_rng) < delete_prop) {
                auto key = delbuf[delete_idx].key;
                auto val = delbuf[delete_idx].value;
                auto weight = delbuf[delete_idx].weight;
                delete_idx++;

                if (deleted.find(key) == deleted.end()) {
                    if (lsm::DELETE_TAGGING) {
                        tree->delete_record(key, val, g_rng);
                    } else {
                        tree->append(key, val, weight, true, g_rng);
                    }
                    deleted.insert(key);
                    local_deleted++;
                }
            }
            // insert the record;
            tree->append(insert_vec[i].key, insert_vec[i].value, insert_vec[i].weight, false, g_rng);
            local_inserted++;
        }
        auto insert_stop = std::chrono::high_resolution_clock::now();

        applied_deletes += local_deleted;
        applied_inserts += local_inserted;
        total_time += std::chrono::duration_cast<std::chrono::nanoseconds>(insert_stop - insert_start).count();
    } 

    size_t throughput = (((double) (applied_inserts + applied_deletes) / (double) total_time) * 1e9);

    fprintf(stderr, "%ld\n", throughput);

    reset_lsm_perf_metrics();
    delete[] delbuf;

    return continue_benchmark;
}


int main(int argc, char **argv)
{
    if (argc < 8) {
        fprintf(stderr, "Usage: lsm_merge <filename> <record_count> <memtable_size> <scale_factor> <memory_levels> <delete_proportion> <max_delete_proportion> [osm_data]\n");
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

    double insert_batch = 0.1; 

    std::string root_dir = "benchmarks/data/lsm_insert_sample";

    init_bench_env(record_count, true, use_osm);

    auto sampling_lsm = lsm::LSMTree(root_dir, memtable_size, memtable_size*max_delete_prop, scale_factor, memory_levels, max_delete_prop, 100, g_rng);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    // warm up the tree with initial_insertions number of initially inserted
    // records
    size_t warmup_cnt = insert_batch * record_count;
    warmup(&datafile, &sampling_lsm, warmup_cnt, delete_prop, false);

    size_t insert_cnt = record_count - warmup_cnt;

    insert_benchmark(&sampling_lsm, &datafile, insert_cnt, delete_prop);

    delete_bench_env();
    fflush(stdout);
    fflush(stderr);

    exit(EXIT_SUCCESS);
}
