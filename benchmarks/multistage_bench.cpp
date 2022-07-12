
#include "sampling/lsmtree.hpp"
#include "util/benchutil.hpp"
#include <cstdlib>
#include <cstdio>
#include <chrono>

#include <iostream>
#include <fstream>
#include <sstream>
#include <unordered_set>

#include <gsl/gsl_rng.h>

#include <random>

std::random_device rd;
std::mt19937 gen(rd());

#define MAX_KEY_VAL 100000000

static gsl_rng *rng;

int64_t max_key = INT64_MIN;
int64_t min_key = INT64_MAX;

std::vector<int64_t> inserted_keys;
std::vector<int64_t> inserted_values;

static void get_sample_bounds(int64_t *min, int64_t *max)
{
    size_t idx1 = gsl_rng_uniform_int(rng, inserted_keys.size());
    size_t idx2 = gsl_rng_uniform_int(rng, inserted_keys.size());

    int64_t s_min = inserted_keys[idx1]; 
    int64_t s_max = inserted_keys[idx2];

    if (s_min > s_max) {
        auto t = s_min;
        s_min = s_max;
        s_max = t;
    }

    *min = s_min;
    *max = s_max;
}


static void initialize_rng(void)
{
    gsl_rng_env_setup();
    rng = gsl_rng_alloc(gsl_rng_gfsr4);

    gsl_rng_set(rng, std::chrono::steady_clock::now().time_since_epoch().count());
}


static void load_data(std::fstream *file)
{
    std::string line;
    while (getline(*file, line, '\n')) {
        std::stringstream line_stream(line);
        std::string key_field;
        std::string value_field;

        std::getline(line_stream, value_field, ' ');
        std::getline(line_stream, key_field, ' ');

        double key = std::stod(key_field);
        int64_t val = std::stoi(value_field);


        int64_t key_int = *((int64_t *) &key);

        if (key_int > max_key) {
            max_key = key_int;
        }

        if (key_int < min_key) {
            min_key = key_int;
        }

        inserted_keys.push_back(key_int);
        inserted_values.push_back(val);
    }
}


static std::vector<size_t> select_for_deletion(lsm::sampling::LSMTree *tree, size_t deletion_count)
{
    std::unordered_set<int64_t> selected_values;
    std::vector<size_t> selected_records;

    size_t selected = 0;
    while (selected < deletion_count && selected < tree->record_count()) {
        size_t idx = gsl_rng_uniform_int(rng, tree->record_count());
        if (selected_values.find(inserted_values[idx]) == selected_values.end()) {
            selected_values.insert(inserted_values[idx]);
            selected_records.push_back(idx);
            selected++;
        }
    }

    return selected_records;
}


static void benchmarking_phase(lsm::sampling::LSMTree *tree, size_t inserts=1000000, 
                               double deletion_proportion=.1, size_t sample_size=1000, bool silent=false)
{
    // first step: perform insertions
    std::string line;
    size_t insertion_count = 0;
    size_t starting_rec_count = tree->record_count();
    auto insert_start = std::chrono::high_resolution_clock::now();
    for (size_t i=0; i<inserts && i<inserted_keys.size(); i++) {
        size_t idx = i + starting_rec_count;
        if (idx >= inserted_keys.size()) {
            break;
        }

        auto result = tree->insert((std::byte *) &inserted_keys[idx],
                                   (std::byte *) &inserted_values[idx]);
        insertion_count++;
        assert(result);
    }
    auto insert_stop = std::chrono::high_resolution_clock::now();
    
    // second step: perform deletions
    std::vector<size_t> deleted_records;
    size_t deletes = tree->record_count() * deletion_proportion;
    size_t deleted = 0;

    auto to_delete = select_for_deletion(tree, deletes);
    assert(to_delete.size() == deletes);
    auto delete_start = std::chrono::high_resolution_clock::now();
    for (size_t i=0; i<to_delete.size(); i++) {
        deleted += tree->remove((std::byte *) &inserted_keys[to_delete[i]],
                                (std::byte *) &inserted_values[to_delete[i]]);
    }
    auto delete_stop = std::chrono::high_resolution_clock::now();

    // third step: benchmark sampling performance
    size_t sample_count = 10000;
    size_t total_rejections = 0;
    size_t total_attempts = 0;
    int64_t data_range = max_key - min_key;
    std::vector<double> selectivities;
    std::vector<std::pair<int64_t, int64_t>> sample_ranges;

    for (size_t i=0; i<sample_count; i++) {
        int64_t s_min, s_max;
        get_sample_bounds(&s_min, &s_max);
        selectivities.push_back((double)(s_max - s_min) / (double) data_range);
        sample_ranges.push_back({s_min, s_max});
    }

    long sample_dur = 0;
    long walker_dur = 0;
    long bounds_dur = 0;
    long buffer_dur = 0;
    long reject_dur = 0;

    for (size_t i=0; i<sample_count; i++) {
        tree->range_sample_bench((std::byte *) &sample_ranges[i].first,
                                 (std::byte *) &sample_ranges[i].second,
                                 sample_size, &total_rejections,
                                 &total_attempts, &buffer_dur, &bounds_dur,
                                 &walker_dur, &sample_dur, &reject_dur);
    }

    auto insert2_start = std::chrono::high_resolution_clock::now();
    for (auto idx : to_delete) {
        tree->insert((std::byte *) &inserted_keys[idx], (std::byte*) &inserted_values[idx]);
        insertion_count++;
    }
    auto insert2_stop = std::chrono::high_resolution_clock::now();

    auto insert1_duproportionn = std::chrono::duration_cast<std::chrono::nanoseconds>(insert_stop - insert_start).count();
    auto insert2_duproportionn = std::chrono::duration_cast<std::chrono::nanoseconds>(insert2_stop - insert2_start).count();

    auto insert_time = (insertion_count > 0) ? (insert1_duproportionn + insert2_duproportionn) / insertion_count : 0;
    auto delete_time = (deletes > 0) ? std::chrono::duration_cast<std::chrono::nanoseconds>(delete_stop - delete_start).count() / deletes : 0;
    auto sample_time = (sample_count > 0) ? sample_dur / sample_count : 0;
    auto bounds_time = (sample_count > 0) ? bounds_dur / sample_count : 0;
    auto walker_time = (sample_count > 0) ? walker_dur / sample_count : 0;
    auto buffer_time = (sample_count > 0) ? buffer_dur / sample_count : 0;

    auto rejection_rate = (double) total_rejections / (double) (total_attempts);

    if (!silent) {
        fprintf(stdout, "%10ld\t%10ld\t%ld\t%ld\t%ld\t%ld\t%ld\t%ld\t%ld\t%.4lf\t%ld\n", tree->record_count(), deleted, tree->depth(), insert_time, delete_time, 
                buffer_time, bounds_time, walker_time, sample_time, rejection_rate, tree->memory_utilization());
    }

}


int main(int argc, char **argv)
{
    if (argc < 5) {
        fprintf(stderr, "Usage: multistage_bench <filename> <0|1> <buffer_size> <scale_factor> <bloom_filters> <deletion_proportion> [max_deletion_proportion]\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    bool policy = atoi(argv[2]);
    size_t buffer_size = atoi(argv[3]);
    size_t scale_factor = atoi(argv[4]);
    bool bloom = atoi(argv[5]);
    double deletion_proportion = atof(argv[6]);
    double max_deletion_proportion = (argc == 0) ? atof(argv[7]) : 1.0;

    initialize_rng();

    std::unique_ptr<lsm::sampling::LSMTree> sampling_lsm;

    auto state = lsm::bench::bench_state();
    auto merge_policy = (policy) ? lsm::sampling::LEVELING : lsm::sampling::TIERING;

    sampling_lsm = lsm::sampling::LSMTree::create(buffer_size, scale_factor, std::move(state), merge_policy, bloom, false, max_deletion_proportion);

    size_t sample_size = 1000;
    size_t insertion_cnt = 1000000;

    std::fstream datafile;
    std::string line;
    datafile.open(filename, std::ios::in);

    load_data(&datafile);

    // Preload the tree with the first 1,000,000 records
    benchmarking_phase(sampling_lsm.get(), 1000000, 0, 0, true);

    fprintf(stdout, "Record Count\tNew Delete Count\tTree Height\tInsert Time (ns)\tDelete Time (ns)\tBuffer Processing Time (ns)\tLevel Bounds Calculation Time (ns)\tWalker Construction Time (ns)\tSampling Time (ns)\tRejection Rate (proportion)\tAuxiliary Memory (B)\n");
    // Throw away samples to get data into the cache
    size_t throwaway_samples = 2;
    int64_t s_min, s_max;
    for (size_t i=0; i<throwaway_samples; i++) {
        get_sample_bounds(&s_min, &s_max);
        sampling_lsm->range_sample((std::byte *) &s_min, (std::byte *) &s_max, sample_size);
    }

    size_t stage_count = 23; 
    for (size_t i=0; i<stage_count; i++) {
        benchmarking_phase(sampling_lsm.get(), insertion_cnt, deletion_proportion, sample_size);
    }

    exit(EXIT_SUCCESS);
}
