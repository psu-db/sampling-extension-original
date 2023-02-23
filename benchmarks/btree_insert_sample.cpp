#define ENABLE_TIMER

#include "bench.h"

size_t g_insert_batch_size = 100;
size_t g_insert_phase = 0;

static bool insert_benchmark(TreeMap *tree, std::fstream *file, 
                      size_t insert_cnt, double delete_prop) {
    // for looking at the insert time distribution
    std::vector<size_t> insert_times;
    insert_times.reserve(insert_cnt / g_insert_batch_size);

    size_t deletes = insert_cnt * delete_prop;
    std::vector<lsm::key_type> delbuf;
    tree->range_sample(g_min_key, g_max_key, deletes, delbuf, g_rng);

    std::set<lsm::key_type> deleted;
    size_t applied_deletes = 0;

    size_t applied_inserts = 0;
    std::vector<std::pair<btree_record, double>> insert_vec;
    insert_vec.reserve(g_insert_batch_size);
    bool continue_benchmark = true;

    while (applied_inserts < insert_cnt && continue_benchmark) { 
        continue_benchmark = build_btree_insert_vec(file, insert_vec, g_insert_batch_size);
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
                tree->erase_one(delbuf[applied_deletes]);
                local_deleted++;
                applied_deletes++;
            }

            // insert the record;
            tree->insert(insert_vec[local_inserted].first, insert_vec[local_inserted].second);
            local_inserted++;
        }
        auto insert_stop = std::chrono::high_resolution_clock::now();

        applied_inserts += local_inserted;
        insert_times.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(insert_stop - insert_start).count() / (local_inserted + local_deleted) );
    } 

    for (size_t i=0; i<insert_times.size(); i++) {
        fprintf(stdout, "%ld\n", insert_times[i]);
    }

    g_insert_phase++;

    return continue_benchmark;
}


static void sample_benchmark(TreeMap *tree, size_t k, size_t trial_cnt)
{
    std::vector<lsm::key_type> sample_set;

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < trial_cnt; i++) {
        tree->range_sample(g_min_key, g_max_key, k, sample_set, g_rng);
    }

    auto stop = std::chrono::high_resolution_clock::now();

    auto total_latency = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    double avg_latency = (double) total_latency.count() / trial_cnt;

    printf("%zu %.0lf\n", k, avg_latency);
}


int main(int argc, char **argv)
{
    if (argc < 4) {
        fprintf(stderr, "Usage: btree_insert_sample <filename> <record_count> <delete_proportion> [osm_data]\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    double delete_prop = atof(argv[3]);
    bool use_osm = (argc == 5) ? atoi(argv[4]) : 0;

    double insert_batch = 0.1; 

    std::string root_dir = "benchmarks/data/btree_insert_sample";

    init_bench_env(record_count, true, use_osm);

    auto sampling_tree = TreeMap();

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    // warm up the tree with initial_insertions number of initially inserted
    // records
    size_t insert_cnt = insert_batch * record_count;
    warmup(&datafile, &sampling_tree, insert_cnt, delete_prop);

    fprintf(stderr, "Insert Latency (ns) [Averaged over %ld inserts]\n", g_insert_batch_size);

    while (insert_benchmark(&sampling_tree, &datafile, insert_cnt, delete_prop))
        ;

    size_t max_sample_size = 100000;
    for (size_t sample_size =1; sample_size < max_sample_size; sample_size *= 10) {
        sample_benchmark(&sampling_tree, sample_size, 10000);
    }

    delete_bench_env();
    exit(EXIT_SUCCESS);
}
