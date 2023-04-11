#include "bench.h"

#include <ctime>

size_t g_insert_batch_size = 10000;

std::mutex latency_lock;
size_t g_total_latency;

static void insert_records(lsm::LSMTree *tree, std::vector<lsm::record_t> *vec, size_t start_idx, size_t stop_idx, gsl_rng *rng) {
    struct timespec t { 0, 100};
    size_t total_latency = 0;
    for (size_t i=start_idx; i<stop_idx; i++) {

        auto insert_start = std::chrono::high_resolution_clock::now();
        for (size_t j=i; j<i+100 && j<stop_idx; j++) {
            tree->append((*vec)[i].key, (*vec)[i].value, false, rng);
        }
        auto insert_stop = std::chrono::high_resolution_clock::now();
        total_latency += std::chrono::duration_cast<std::chrono::nanoseconds>(insert_stop - insert_start).count();

        nanosleep(&t, nullptr);
    }

    latency_lock.lock();
    g_total_latency += total_latency;
    latency_lock.unlock();
}


static void multithread_insert_bench(lsm::LSMTree *tree, std::fstream *file, size_t insert_cnt, size_t thrd_cnt) {
    gsl_rng *rngs[thrd_cnt];

    for (size_t i=0; i<thrd_cnt; i++) {
        rngs[i] = gsl_rng_alloc(gsl_rng_mt19937);
        gsl_rng_set(rngs[i], get_random_seed());
    }

    std::vector<lsm::record_t> insert_vector;
    size_t inserted = 0;

    std::vector<std::thread> thrds(thrd_cnt);

    int64_t total_time = 0;

    build_insert_vec(file, insert_vector, g_insert_batch_size);
    while (build_insert_vec(file, insert_vector, g_insert_batch_size) && inserted < insert_cnt) {

        progress_update((double) inserted / (double) insert_cnt, "inserting:");
        size_t per_thread = insert_vector.size() / thrd_cnt;
        if (per_thread == 0) return;

        size_t start = 0;
        size_t stop = start + per_thread;


        auto insert_start = std::chrono::high_resolution_clock::now();
        for (size_t i=0; i<thrd_cnt; i++) {
            thrds[i] = std::thread(insert_records, tree, &insert_vector, start, stop, rngs[i]);
            start = stop;
            stop = std::min(start + per_thread, insert_vector.size());
        }

        for (size_t i=0; i<thrd_cnt; i++) {
            if (thrds[i].joinable()) thrds[i].join();
        }
        auto insert_stop = std::chrono::high_resolution_clock::now();

        total_time += std::chrono::duration_cast<std::chrono::nanoseconds>(insert_stop - insert_start).count();
        inserted += insert_vector.size();
    }

    size_t throughput = (((double) (inserted) / (double) total_time) * 1e9);
    size_t latency = ((double) (g_total_latency) / (double) inserted);

    progress_update(1.0, "inserting:");

    fprintf(stdout, "%ld\t%ld\n", throughput, latency);
    fflush(stdout);

    reset_lsm_perf_metrics();
    for (size_t i=0; i<thrd_cnt; i++) {
        gsl_rng_free(rngs[i]);
    }
}


int main(int argc, char **argv)
{
    if (argc < 6) {
        fprintf(stderr, "Usage: lsm_insert <filename> <record_count> <memtable_size> <scale_factor> <thread_cnt> [osm_data]\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    size_t memtable_size = atol(argv[3]);
    size_t scale_factor = atol(argv[4]);
    size_t thrd_cnt = atol(argv[5]);
    bool use_osm = (argc == 7) ? atoi(argv[6]) : 0;
    
    std::string root_dir = "benchmarks/data/lsm_insert";

    init_bench_env(record_count, true, use_osm);

    auto sampling_lsm = lsm::LSMTree(root_dir, memtable_size, memtable_size, scale_factor, 1, g_rng);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    // warm up the tree with initial_insertions number of initially inserted
    // records
    size_t warmup_cnt =  0.1 * record_count;
    warmup(&datafile, &sampling_lsm, warmup_cnt, 0);

    size_t insert_cnt = record_count - warmup_cnt;
    multithread_insert_bench(&sampling_lsm, &datafile, insert_cnt, thrd_cnt);

    delete_bench_env();
    fflush(stdout);
    fflush(stderr);

    exit(EXIT_SUCCESS);
}
