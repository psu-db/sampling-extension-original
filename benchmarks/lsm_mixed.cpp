#define ENABLE_TIMER

#include "bench.h"

static void benchmark(lsm::LSMTree *tree, std::fstream *file, size_t k, size_t trial_cnt, 
                      double write_prop, double del_prop) {
    char sample_set[k*lsm::record_size];

    unsigned long total_sample_time = 0;
    unsigned long total_insert_time = 0;
    unsigned long total_delete_time = 0;

    size_t sample_cnt = 0;
    size_t insert_cnt = 0;
    size_t delete_cnt = 0;

    size_t operation_cnt = 10000;

    size_t reccnt = tree->get_record_cnt();
    char *delbuf = new char[trial_cnt*lsm::record_size]();

    size_t ops = 0;
    while (ops < operation_cnt) {
        double op = gsl_rng_uniform(g_rng);
        Operation operation;

        if (op < write_prop) {
            operation = WRITE;
            // write records
            std::vector<shared_record> insert_vec;
            if (!build_insert_vec(file, insert_vec, trial_cnt))  {
                continue;
            }

            ops++;
            auto start = std::chrono::high_resolution_clock::now();
            for (int i=0; i<insert_vec.size(); i++) {
                tree->append(insert_vec[i].key.get(), insert_vec[i].value.get(), insert_vec[i].weight, false, g_rng);
            }
            auto stop = std::chrono::high_resolution_clock::now();

            insert_cnt += insert_vec.size();
            total_insert_time += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
        } else if (op < (write_prop + del_prop)) {
            operation = DELETE;
            tree->range_sample(delbuf, trial_cnt, g_rng);
            std::set<lsm::key_type> deleted;
            ops++;

            auto start = std::chrono::high_resolution_clock::now();
            for (int i=0; i<trial_cnt; i++) {
                auto key = lsm::get_key(delbuf + (i * lsm::record_size));
                auto val = lsm::get_val(delbuf + (i * lsm::record_size));
                auto weight = lsm::get_weight(delbuf + (i * lsm::record_size));

                if (deleted.find(*(lsm::key_type *) key) == deleted.end()) {
                    tree->append(key, val, weight, true, g_rng); 
                    deleted.insert(*(lsm::key_type *) key);
                    delete_cnt += 1;
                }
            }
            auto stop = std::chrono::high_resolution_clock::now();

            total_delete_time += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
        } else {
            operation = READ;
            //sample
            ops++;
            auto start = std::chrono::high_resolution_clock::now();
            for (int i=0; i<trial_cnt/10; i++) {
                tree->range_sample(sample_set, k, g_rng);
            }
            auto stop = std::chrono::high_resolution_clock::now();

            sample_cnt += trial_cnt;
            total_sample_time += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
        }
    }

    size_t avg_insert_latency = (insert_cnt) ? total_insert_time / insert_cnt : 0;
    size_t avg_sample_latency = (sample_cnt) ? total_sample_time / sample_cnt : 0;
    size_t avg_delete_latency = (delete_cnt) ? total_delete_time / delete_cnt : 0;

    size_t avg_insert_tput = (avg_insert_latency) ? (double) (1.0 / (double) avg_insert_latency) * 1e9 : 0;
    size_t avg_sample_tput = (avg_sample_latency) ? (double) (1.0 / (double) avg_sample_latency) * 1e9 : 0;
    size_t avg_delete_tput = (avg_delete_latency) ? (double) (1.0 / (double) avg_delete_latency) * 1e9 : 0;;

    fprintf(stdout, "%ld %ld %ld %ld\n", reccnt, avg_sample_tput, avg_insert_tput, avg_delete_tput);

    delete[] delbuf;
}


int main(int argc, char **argv)
{
    if (argc < 7) {
        fprintf(stderr, "Usage: lsm_mixed <filename> <record_count> <selectivity> <sample_size> <write_prop> <delete_prop> [existing_root_dir]\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    double selectivity = atof(argv[3]);
    size_t sample_size = atol(argv[4]);
    double write_prop = atof(argv[5]);
    double del_prop = atof(argv[6]);

    std::string root_dir = (argc == 8) ? std::string(argv[7]) : "benchmarks/data/lsm_mixed";

    init_bench_env(record_count, true);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    lsm::LSMTree *sampling_lsm;

    if (argc == 8) {
        std::string meta_fname = root_dir + "/meta/lsmtree.dat";
        assert(false);
        //sampling_lsm = new lsm::LSMTree(root_dir, 20000, 30000, 10, 1000, 1, meta_fname, g_rng);
    } else {
        sampling_lsm = new lsm::LSMTree(root_dir, 20000, 30000, 10, 1000, 1, 100, g_rng);

        double warmup_prop = 0.6;
        warmup(&datafile, sampling_lsm, record_count * warmup_prop, del_prop); 
    }

    double phase_insert_prop = .1;
    size_t phase_insert_cnt = phase_insert_prop * record_count;

    fprintf(stderr, "Record Count, Average Sampling Throughput (sample/s), Average Insertion Throughput (insert/s), Average Deletion Throughput (delete/s)\n");

    bool records_to_insert = true;
    while (records_to_insert) {
        benchmark(sampling_lsm, &datafile, sample_size, 1000, write_prop, del_prop);
        records_to_insert = warmup(&datafile, sampling_lsm, phase_insert_cnt, del_prop);
    }

    benchmark(sampling_lsm, &datafile, sample_size, 1000, write_prop, del_prop);

    delete_bench_env();

    delete sampling_lsm;
    exit(EXIT_SUCCESS);
}
