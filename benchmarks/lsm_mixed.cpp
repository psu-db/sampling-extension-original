#define ENABLE_TIMER

#include "bench.h"

static void benchmark(lsm::LSMTree *tree, std::fstream *file, size_t k, size_t trial_cnt, 
                      size_t min, size_t max, double selectivity, double write_prop, 
                      double del_prop) {
    char* buffer1 = (char*) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    char* buffer2 = (char*) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    char sample_set[k*lsm::record_size];

    unsigned long total_sample_time = 0;
    unsigned long total_insert_time = 0;
    unsigned long total_delete_time = 0;

    size_t sample_cnt;
    size_t insert_cnt;
    size_t delete_cnt;

    size_t operation_cnt = 200;

    size_t reccnt = tree->get_record_cnt();

    size_t ops = 0;
    while (ops < operation_cnt) {
        double op = gsl_rng_uniform(g_rng);
        Operation operation;

        if (op < write_prop) {
            operation = WRITE;
            // write records
            std::vector<shared_record> insert_vec;
            if (!build_insert_vec(file, insert_vec, trial_cnt, del_prop))  {
                continue;
            }

            ops++;
            auto start = std::chrono::high_resolution_clock::now();
            for (int i=0; i<insert_vec.size(); i++) {
                tree->append(insert_vec[i].first.get(), insert_vec[i].second.get(), false, g_rng);
            }
            auto stop = std::chrono::high_resolution_clock::now();

            insert_cnt += insert_vec.size();
            total_insert_time += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
        } else if (op < (write_prop + del_prop)) {
            operation = DELETE;
            std::vector<shared_record> del_vec;
            std::sample(g_to_delete->begin(), g_to_delete->end(), 
                        std::back_inserter(del_vec), trial_cnt, 
                        std::mt19937{std::random_device{}()});

            if (del_vec.size() == 0) {
                continue;
            }

            ops++;
            auto start = std::chrono::high_resolution_clock::now();
            for (int i=0; i<del_vec.size(); i++) {
                tree->append(del_vec[i].first.get(), del_vec[i].second.get(), true, g_rng); 
                g_to_delete->erase(del_vec[i]);
            }
            auto stop = std::chrono::high_resolution_clock::now();

            delete_cnt += del_vec.size();
            total_delete_time += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
        } else {
            operation = READ;
            //sample
            ops++;
            auto start = std::chrono::high_resolution_clock::now();
            for (int i=0; i<trial_cnt; i++) {
                auto range = get_key_range(min, max, selectivity);
                tree->range_sample(sample_set, (char*) &range.first, (char*) &range.second, k, buffer1, buffer2, g_rng);
            }
            auto stop = std::chrono::high_resolution_clock::now();

            sample_cnt += trial_cnt;
            total_sample_time += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
        }
    }

    size_t avg_insert_latency = total_insert_time / insert_cnt;
    size_t avg_sample_latency = total_sample_time / sample_cnt;
    size_t avg_delete_latency = total_delete_time / delete_cnt;

    size_t avg_insert_tput = (double) (1.0 / (double) avg_insert_latency) * 1e9;
    size_t avg_sample_tput = (double) (1.0 / (double) avg_sample_latency) * 1e9;
    size_t avg_delete_tput = (double) (1.0 / (double) avg_delete_latency) * 1e9;

    fprintf(stdout, "%ld %ld %ld %ld\n", reccnt, avg_sample_latency, avg_insert_latency, avg_delete_tput);

    free(buffer1);
    free(buffer2);
}


int main(int argc, char **argv)
{
    if (argc < 7) {
        fprintf(stderr, "Usage: lsm_mixed <filename> <record_count> <selectivity> <sample_size> <write_prop> <delete_prop>\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    double selectivity = atof(argv[3]);
    size_t sample_size = atol(argv[4]);
    double write_prop = atof(argv[5]);
    double del_prop = atof(argv[6]);

    std::string root_dir = "benchmarks/data/lsm_mixed";

    init_bench_env(true);

    // use for selectivity calculations
    lsm::key_type min_key = 0;
    lsm::key_type max_key = record_count - 1;

    auto sampling_tree = lsm::LSMTree(root_dir, 10000, 30000, 10, 1000, 1, g_rng);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    double warmup_prop = 0.1;
    warmup(&datafile, &sampling_tree, record_count * warmup_prop, del_prop); 

    double phase_insert_prop = .1;
    size_t phase_insert_cnt = phase_insert_prop * record_count;

    bool records_to_insert = true;
    while (records_to_insert) {
        benchmark(&sampling_tree, &datafile, sample_size, 10000, min_key, max_key, selectivity, write_prop, del_prop);
        records_to_insert = insert_to(&datafile, &sampling_tree, phase_insert_cnt, del_prop);
    }

    benchmark(&sampling_tree, &datafile, sample_size, 10000, min_key, max_key, selectivity, write_prop, del_prop);

    delete_bench_env();
    exit(EXIT_SUCCESS);
}
