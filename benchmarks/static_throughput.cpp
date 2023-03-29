#include "bench.h"

size_t g_insert_batch_size = 1000;

static void sample_benchmark(lsm::WIRSRun *run, size_t k, size_t trial_cnt)
{
    char progbuf[25];
    sprintf(progbuf, "sampling (%ld):", k);

    size_t batch_size = 100;
    size_t batches = trial_cnt / batch_size;
    size_t total_time = 0;

    lsm::record_t sample_set[k];

    for (int i=0; i<batches; i++) {
        progress_update((double) (i * batch_size) / (double) trial_cnt, progbuf);
        auto start = std::chrono::high_resolution_clock::now();
        for (int j=0; j < batch_size; j++) {
            run->get_samples(sample_set, k, nullptr, g_rng);
        }
        auto stop = std::chrono::high_resolution_clock::now();

        total_time += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
    }

    progress_update(1.0, progbuf);

    size_t throughput = (((double)(trial_cnt * k) / (double) total_time) * 1e9);

    fprintf(stdout, "%.0ld\n", throughput);
}


int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "Usage: static_throughput <filename> <record_count> [osm_data]\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    bool use_osm = (argc == 4) ? atoi(argv[3]) : 0;

    std::string root_dir = "benchmarks/data/static_throughput";

    init_bench_env(record_count, true, use_osm);

    auto sampling_lsm = lsm::LSMTree(root_dir, 12000, 12000, 6, 10000, 1, 100, g_rng);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    // warm up the tree with initial_insertions number of initially inserted
    // records
    warmup(&datafile, &sampling_lsm, record_count, 0);

    auto static_run = sampling_lsm.create_static_structure();

    size_t max_sample_size = 1000000;
    for (size_t sample_size =1; sample_size < max_sample_size; sample_size *= 10) {
        sample_benchmark(static_run, sample_size, 10000);
    }

    delete_bench_env();
    fflush(stdout);
    fflush(stderr);

    delete static_run;

    exit(EXIT_SUCCESS);
}
