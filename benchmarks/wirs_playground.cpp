#include "bench.h"
#include "lsm/MemTable.h"

int main(int argc, char **argv)
{
    init_bench_env(true);
    lsm::MemTable* mtable = new lsm::MemTable(1000000, true, 10, g_rng);
    
    for (size_t i = 0; i < 1000000; ++i) {
        mtable->append((const char*)&i, (const char*)&i, (double)(i / 5) + 1.0);
    }

    lsm::BloomFilter* bf = new lsm::BloomFilter(100, lsm::BF_HASH_FUNCS, g_rng);
    lsm::WIRSRun* run = new lsm::WIRSRun(mtable, bf);

    size_t k = 10;
    char sample_set[k * lsm::record_size];
    size_t low = 4;
    size_t high = 100;
    auto sz = run->get_samples(sample_set, (const char*)&low, (const char*)&high, k, g_rng);
    printf("actual sample size: %zu\n", sz);

    delete mtable;
    delete bf;
    delete run;
    delete_bench_env();
    /*
    std::string root_dir = "benchmarks/data/default_bench";

    init_bench_env(true);

    // use for selectivity calculations
    lsm::key_type min_key = 0;
    lsm::key_type max_key = record_count - 1;

    auto sampling_lsm = lsm::LSMTree(root_dir, memtable_size, memtable_size*3, scale_factor, memory_levels, max_delete_prop, 100, g_rng);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    // warm up the tree with initial_insertions number of initially inserted
    // records
    size_t initial_insertions = insert_batch * record_count;
    warmup(&datafile, &sampling_lsm, initial_insertions, delete_prop);

    size_t inserts = insert_batch * record_count;

    size_t total_inserts = initial_insertions;

    while (benchmark(&sampling_lsm, &datafile, inserts, delete_prop)) {
        total_inserts += inserts;

        if (total_inserts + inserts > record_count) {
            inserts = record_count - total_inserts;
        }

        if (total_inserts >= record_count) {
            break;
        }
    }

    delete_bench_env();*/

    exit(EXIT_SUCCESS);
}
