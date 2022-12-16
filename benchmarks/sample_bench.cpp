#define ENABLE_TIMER

#include <memory>
#include <cstdlib>
#include <cstdio>
#include <chrono>
#include <algorithm>
#include <numeric>
#include <iostream>
#include <fstream>
#include <sstream>
#include <unordered_set>
#include <set>
#include <string>
#include <random>

#include "lsm/LsmTree.h"

gsl_rng *g_rng;

typedef std::pair<lsm::key_type, lsm::value_type> tree_rec;

struct key_extract {
    static const lsm::key_type &get(const tree_rec &v) {
        return v.first;
    }
};

static bool next_record(std::fstream *file, char *key, char *val)
{
    std::string line;
    if (std::getline(*file, line, '\n')) {
        std::stringstream line_stream(line);
        std::string key_field;
        std::string value_field;

        std::getline(line_stream, value_field, '\t');
        std::getline(line_stream, key_field, '\t');

        *((lsm::key_type*) key) = atol(key_field.c_str());
        *((lsm::value_type*) val) = atol(value_field.c_str());
        return true;
    }

    key = nullptr;
    val = nullptr;
    return false;
}


static std::pair<lsm::key_type, lsm::key_type> sample_range(lsm::key_type min, lsm::key_type max, double selectivity)
{
    size_t range_length = (max - min) * selectivity;

    lsm::key_type max_bottom = max - range_length;
    lsm::key_type bottom = gsl_rng_uniform_int(g_rng, max_bottom);

    return std::pair<lsm::key_type, lsm::key_type> {bottom, bottom + range_length};
}


static void build_tree(lsm::LSMTree *tree, std::fstream *file) {
    // for looking at the insert time distribution
    auto key_buf = new char[lsm::key_size]();
    auto val_buf = new char[lsm::value_size]();

    size_t i=0;
    while (next_record(file, key_buf, val_buf)) {
        auto res = tree->append(key_buf, val_buf, false, g_rng);
        assert(res);
    }
    delete[] key_buf;
    delete[] val_buf;
}


static void benchmark(lsm::LSMTree *tree, size_t n, size_t k, size_t sample_attempts, size_t min, size_t max, double selectivity)
{
    char* buffer1 = (char*) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    char* buffer2 = (char*) std::aligned_alloc(lsm::SECTOR_SIZE, lsm::PAGE_SIZE);
    char sample_set[k*lsm::record_size];

    // Start the high-resolution clock
    auto start = std::chrono::high_resolution_clock::now();

    // Perform the sampling operation multiple times
    for (int i = 0; i < sample_attempts; i++)
    {
        // Lower and upper bounds for the sample
        auto range = sample_range(min, max, selectivity);

        tree->range_sample(sample_set, (char*) &range.first, (char*) &range.second, k, buffer1, buffer2, g_rng);
    }

    // Stop the high-resolution clock
    auto stop = std::chrono::high_resolution_clock::now();

    // Compute the total latency of the sampling operation
    auto total_latency = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);

    // Average the latency over all the iterations
    double avg_latency = (double) total_latency.count() / sample_attempts;

    // Print the average latency to stdout
    printf("%.0lf\n", avg_latency);
}


int main(int argc, char **argv)
{
    if (argc < 4) {
        fprintf(stderr, "Usage: static_bench <filename> <record_count> <selectivity>\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    double selectivity = atof(argv[3]);

    std::string root_dir = "benchmarks/data/default_bench";

    g_rng = gsl_rng_alloc(gsl_rng_mt19937);

    // use for selectivity calculations
    lsm::key_type min_key = 0;
    lsm::key_type max_key = record_count - 1;

    // initialize the random number generator
    unsigned int seed = 0;
    {
        std::fstream urandom;
        urandom.open("/dev/urandom", std::ios::in|std::ios::binary);
        urandom.read((char *) &seed, sizeof(seed));
        urandom.close();
    }
    gsl_rng_set(g_rng, seed);

    auto sampling_tree = lsm::LSMTree(root_dir, 10000, 30000, 10, 1000, 1, g_rng);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    // warm up the tree with initial_insertions number of initially inserted
    // records
    build_tree(&sampling_tree, &datafile);

    size_t n;
    benchmark(&sampling_tree, n, 1000, 10000, min_key, max_key, selectivity);

    exit(EXIT_SUCCESS);
}
