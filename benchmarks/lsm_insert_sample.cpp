#define ENABLE_TIMER

#include "bench.h"

size_t g_insert_batch_size = 100;
size_t g_insert_phase = 0;

static bool insert_benchmark(lsm::LSMTree *tree, std::fstream *file, 
                      size_t insert_cnt, double delete_prop) {
    // for looking at the insert time distribution
    std::vector<size_t> insert_times;
    insert_times.reserve(insert_cnt / g_insert_batch_size);

    size_t deletes = insert_cnt * delete_prop;
    char *delbuf = new char[deletes * lsm::record_size]();
    tree->range_sample(delbuf, deletes, g_rng);
    std::set<lsm::key_type> deleted;
    size_t applied_deletes = 0;

    size_t applied_inserts = 0;
    std::vector<shared_record> insert_vec;
    insert_vec.reserve(g_insert_batch_size);
    bool continue_benchmark = true;

    while (applied_inserts < insert_cnt && continue_benchmark) { 
        continue_benchmark = build_insert_vec(file, insert_vec, g_insert_batch_size);
        if (insert_vec.size() == 0) {
            break;
        }

        progress_update((double) applied_inserts / (double) insert_cnt, 
                        "Insert Phase " + std::to_string(g_insert_phase));
        size_t local_inserted = 0;
        size_t local_deleted = 0;
        
        auto insert_start = std::chrono::high_resolution_clock::now();
        for (size_t i=0; i<insert_vec.size(); i++) {
            // process a delete if necessary
            if (applied_deletes < deletes && gsl_rng_uniform(g_rng) < delete_prop) {
                auto key = lsm::get_key(delbuf + (applied_deletes * lsm::record_size));
                auto val = lsm::get_val(delbuf + (applied_deletes * lsm::record_size));
                auto weight = lsm::get_weight(delbuf + (applied_deletes * lsm::record_size));

                if (deleted.find(*(lsm::key_type *)key) == deleted.end()) {
                    tree->append(key, val, weight, true, g_rng);
                    deleted.insert(*(lsm::key_type *)key);
                    local_deleted++;
                }
            }
            // insert the record;
            tree->append(insert_vec[i].key.get(), insert_vec[i].value.get(), insert_vec[i].weight, false, g_rng);
            local_inserted++;
        }
        auto insert_stop = std::chrono::high_resolution_clock::now();

        applied_deletes += local_deleted;
        applied_inserts += local_inserted;
        insert_times.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(insert_stop - insert_start).count() / (local_inserted + local_deleted) );
    } 

    for (size_t i=0; i<insert_times.size(); i++) {
        fprintf(stdout, "%ld\n", insert_times[i]);
    }

    g_insert_phase++;
    reset_lsm_perf_metrics();
    delete[] delbuf;

    return continue_benchmark;
}


static void sample_benchmark(lsm::LSMTree *tree, size_t k, size_t trial_cnt)
{
    char sample_set[k*lsm::record_size];

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < trial_cnt; i++) {
        tree->range_sample(sample_set, k, g_rng);
    }

    auto stop = std::chrono::high_resolution_clock::now();

    auto total_latency = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    double avg_latency = (double) total_latency.count() / trial_cnt;

    printf("%zu %.0lf\n", k, avg_latency);
}


int main(int argc, char **argv)
{
    if (argc < 8) {
        fprintf(stderr, "Usage: lsm_insert_sample <filename> <record_count> <memtable_size> <scale_factor> <memory_levels> <delete_proportion> <max_delete_proportion> [insert_batch_proportion]\n");
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

    std::string root_dir = "benchmarks/data/lsm_insert_sample";

    init_bench_env(true);

    auto sampling_lsm = lsm::LSMTree(root_dir, memtable_size, memtable_size*max_delete_prop, scale_factor, memory_levels, max_delete_prop, 100, g_rng);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    // warm up the tree with initial_insertions number of initially inserted
    // records
    size_t insert_cnt = insert_batch * record_count;
    warmup(&datafile, &sampling_lsm, insert_cnt, delete_prop);

    fprintf(stderr, "Insert Latency (ns) [Averaged over %ld inserts]\n", g_insert_batch_size);

    while (insert_benchmark(&sampling_lsm, &datafile, insert_cnt, delete_prop))
        ;

    size_t max_sample_size = 100000;
    for (size_t sample_size =1; sample_size < max_sample_size; sample_size *= 10) {
        sample_benchmark(&sampling_lsm, sample_size, 10000);
    }

    delete_bench_env();
    exit(EXIT_SUCCESS);
}
