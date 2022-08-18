#include "ds/bloomfilter.hpp"
#include "util/benchutil.hpp"

using namespace lsm::ds;

int main(int argc, char **argv) 
{
    if (argc < 3) {
        fprintf(stderr, "bloom_bench <filter_size> <hash_func_cnt>\n");
        exit(EXIT_FAILURE);
    }

    size_t size = atol(argv[1]);
    size_t k = atol(argv[2]);

    auto state = lsm::bench::bench_state(sizeof(int64_t), sizeof(int64_t));
    auto pfile = state->file_manager->create_indexed_pfile();

    auto first_page = pfile->allocate_page();

    auto filter = BloomFilter::create_persistent(size, sizeof(int64_t), k, first_page, state.get());

    int64_t key = 8;

    size_t trials = 100000000;
    auto start = std::chrono::high_resolution_clock::now();
    for (size_t i=0; i<trials; i++) {
        filter->lookup((std::byte *) &key);
    }
    auto stop = std::chrono::high_resolution_clock::now();


    size_t per_lookup = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count() / trials;

    fprintf(stdout, "%ld\n", per_lookup);
}
