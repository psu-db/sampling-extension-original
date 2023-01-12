#define ENABLE_TIMER

#include "bench.h"

int main(int argc, char **argv)
{
    if (argc < 5) {
        fprintf(stderr, "Usage: insert_bench <filename> <record_count> <memtable_size> <scale_factor> <memory_levels> <delete_proportion> <max_delete_proportion>\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    size_t memtable_size = atol(argv[3]);
    double delete_prop = atof(argv[4]);
    // size_t scale_factor = atol(argv[4]);
    // //double selectivity = atof(argv[5]);
    // size_t memory_levels = atol(argv[5]);
    
    // double max_delete_prop = atof(argv[7]);
    double insert_batch = (argc == 6) ? atof(argv[5]) : 0.8;

    std::string root_dir = "benchmarks/data/default_bench";
    init_bench_env(true);

    auto sampling_tree = lsm::LSMTree(root_dir, 15000, 15000*3, 10, 1000, 0.05, g_rng);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    // warm up the tree with initial_insertions number of initially inserted
    // records
    size_t initial_insertions = insert_batch * record_count;
    warmup(&datafile, &sampling_tree, initial_insertions, delete_prop);

    sampling_tree.persist_tree(g_rng);

    delete_bench_env();
    exit(EXIT_SUCCESS);
}
