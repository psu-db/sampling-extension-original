#define ENABLE_TIMER

#include "bench.h"

int main(int argc, char **argv)
{
    if (argc < 9) {
        fprintf(stderr, "Usage: lsm_warmup <filename> <target> <record_count> <memtable_size> <scale_factor> <memory_levels> <delete_proportion> <max_delete_proportion>\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    std::string root_dir = std::string(argv[2]);
    size_t record_count = atol(argv[3]);
    size_t memtable_size = atol(argv[4]);
    size_t scale_factor = atol(argv[5]);
    size_t memory_levels = atol(argv[6]);
    double delete_prop = atof(argv[7]);
    double max_delete_prop = atof(argv[8]);
    double insert_batch = (argc == 10) ? atof(argv[9]) : 0.6;

    init_bench_env(true);

    auto sampling_lsm = lsm::LSMTree(root_dir, memtable_size, memtable_size*3, scale_factor, memory_levels, max_delete_prop, g_rng);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    // warm up the tree with initial_insertions number of initially inserted
    // records
    size_t initial_insertions = insert_batch * record_count;
    warmup(&datafile, &sampling_lsm, initial_insertions, delete_prop);

    sampling_lsm.persist_tree(g_rng);

    delete_bench_env();
    exit(EXIT_SUCCESS);
}
