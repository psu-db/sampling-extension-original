#define ENABLE_TIMER

#include "bench.h"

size_t g_insert_batch_size = 100;

static bool benchmark(lsm::LSMTree *tree, std::fstream *file, 
                      size_t inserts, double delete_prop) {
    // for looking at the insert time distribution
    std::vector<size_t> insert_times;
    insert_times.reserve(inserts / g_insert_batch_size);

    bool out_of_data = false;

    size_t inserted_records = 0;
    std::vector<shared_record> to_insert(g_insert_batch_size);

    size_t deletes = inserts * delete_prop;
    std::vector<shared_record> del_vec;
    std::sample(g_to_delete->begin(), g_to_delete->end(), std::back_inserter(del_vec), deletes, std::mt19937{std::random_device{}()});

    size_t applied_deletes = 0;
    while (inserted_records < inserts && !out_of_data) {
        size_t inserted_from_batch = 0;
        for (size_t i=0; i<g_insert_batch_size; i++) {
            auto rec = create_shared_record();
            if (!next_record(file, rec.first.get(), rec.second.get())) {
                    // If no new records were loaded, there's no reason to duplicate
                    // the last round of sampling.
                    if (i == 0) {
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

            if (gsl_rng_uniform(g_rng) < delete_prop + .15) {
                g_to_delete->insert({rec.first, rec.second});
            }
        }

        auto insert_start = std::chrono::high_resolution_clock::now();
        for (size_t i=0; i<inserted_from_batch; i++) {
            if (applied_deletes<deletes && gsl_rng_uniform(g_rng) < delete_prop && del_vec[applied_deletes].first.get() != nullptr) {
                tree->append(del_vec[applied_deletes].first.get(), del_vec[applied_deletes].second.get(), true, g_rng); 
                g_to_delete->erase(del_vec[applied_deletes]);
                applied_deletes++;
                i--;
            } else {
                tree->append(to_insert[i].first.get(), to_insert[i].second.get(), false, g_rng);
            }
        }
        auto insert_stop = std::chrono::high_resolution_clock::now();

        insert_times.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(insert_stop - insert_start).count() / g_insert_batch_size );
    }

    size_t per_insert = std::accumulate(insert_times.begin(), insert_times.end(), decltype(insert_times)::value_type(0)) / (inserts + applied_deletes);

    for (size_t i=0; i<insert_times.size(); i++) {
        fprintf(stdout, "%ld\n", insert_times[i]);
    }

    reset_lsm_perf_metrics();

    return !out_of_data;
}


int main(int argc, char **argv)
{
    if (argc < 8) {
        fprintf(stderr, "Usage: insert_bench <filename> <record_count> <memtable_size> <scale_factor> <memory_levels> <delete_proportion> <max_delete_proportion> [insert_batch_proportion]\n");
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

    size_t inserts = insert_batch * record_count;

    size_t total_inserts = initial_insertions;

    fprintf(stderr, "Insert Latency (ns) [Averaged over %ld inserts]\n", g_insert_batch_size);

    while (benchmark(&sampling_lsm, &datafile, inserts, delete_prop)) {
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