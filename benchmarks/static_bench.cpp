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

static std::set<std::pair<std::shared_ptr<char[]>, std::shared_ptr<char[]>>> *to_delete;

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


static void load_data(std::fstream *file, lsm::LSMTree *lsmtree, size_t count, double delete_prop)
{
    std::string line;

    auto key_buf = std::make_unique<char[]>(lsm::key_size);
    auto val_buf = std::make_unique<char[]>(lsm::value_size);
    
    for (size_t i=0; i<count; i++) {
        if (!next_record(file, key_buf.get(), val_buf.get())) {
            break;
        }

        lsmtree->append(key_buf.get(), val_buf.get(), false, g_rng);

        if (gsl_rng_uniform(g_rng) < delete_prop + .15) {
            auto del_key_buf = new char[lsm::key_size]();
            auto del_val_buf = new char[lsm::value_size]();
            memcpy(del_key_buf, key_buf.get(), lsm::key_size);
            memcpy(del_val_buf, val_buf.get(), lsm::value_size);
            to_delete->insert({std::shared_ptr<char[]>(del_key_buf), std::shared_ptr<char[]>(del_val_buf)});
        }
    }
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

    while (next_record(file, key_buf, val_buf)) {
        tree->append(key_buf, val_buf, false, g_rng);
    }
    delete[] key_buf;
    delete[] val_buf;
}


static char *sample(char *lower, char *upper, size_t n, size_t k, char *data)
{
    // Allocate memory for the result array
    char *result = (char*)malloc(k * lsm::record_size);

    // Start and end indices of the range of records
    // between lower and upper (inclusive)
    int start = 0, end = 0;

    // Binary search for the start index
    int low = 0, high = n - 1;
    while (low <= high)
    {
        int mid = (low + high) / 2;

        // Get the key for the record at the current index
        const char *key = lsm::get_key(data + mid * lsm::record_size);

        if (lsm::key_cmp(key, lower) < 0)
            low = mid + 1;
        else
            high = mid - 1;
    }

    // Set the start index to the first element greater than or equal to lower
    // (from the start of the array)
    start = low;

    // Binary search for the end index
    low = 0, high = n - 1;
    while (low <= high)
    {
        int mid = (low + high) / 2;

        // Get the key for the record at the current index
        const char *key = lsm::get_key(data + mid * lsm::record_size);

        if (lsm::key_cmp(key, upper) > 0)
            high = mid - 1;
        else
            low = mid + 1;
    }

    // Set the end index to the last element less than or equal to upper
    // (from the start of the array)
    end = high;

    // Sample k records from the range of records between start and end (inclusive)
    // with replacement and store the results in the result array
    for (int i = 0; i < k; i++)
    {
        // Generate a random index between start and end (inclusive)
        int idx = gsl_rng_uniform_int(g_rng, end - start + 1) + start;

        // Copy the record at the generated index to the result array
        memcpy(result + i * lsm::record_size, data + idx * lsm::record_size, lsm::record_size);
    }

    // Return a pointer to the result array
    return result;
}


static void benchmark(char *data, size_t n, size_t k, size_t sample_attempts, size_t min, size_t max, double selectivity)
{
    // Start the high-resolution clock
    auto start = std::chrono::high_resolution_clock::now();

    // Perform the sampling operation multiple times
    for (int i = 0; i < sample_attempts; i++)
    {
        // Lower and upper bounds for the sample
        auto range = sample_range(min, max, selectivity);

        // Call the sample function
        char *result = sample((char*) &range.first, (char *) &range.second, n, k, data);

        // Free the memory allocated for the result array
        free(result);
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

    auto sampling_lsm = lsm::LSMTree(root_dir, 1000000, 3000000, 10, 100, 1, g_rng);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    // warm up the tree with initial_insertions number of initially inserted
    // records
    build_tree(&sampling_lsm, &datafile);

    size_t n;
    auto data = sampling_lsm.get_sorted_array(&n, g_rng);

    benchmark(data, n, 1000, 10000, min_key, max_key, selectivity);

    exit(EXIT_SUCCESS);
}
