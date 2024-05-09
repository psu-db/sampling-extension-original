#define ENABLE_TIMER

#include "bench.h"

#include <vector>
#include <cstdlib>
#include <cstdint>
#include <string>

#include "gsl/gsl_rng.h"
#include "lsm/LsmTree.h"

struct irs_query {
    size_t k;
    uint64_t lower_bound;
    uint64_t upper_bound;
};

struct kvp {
    uint64_t key;
    uint64_t value;
};

static double delete_proportion = 0.05;

static void run_queries(lsm::LSMTree *extension, std::vector<irs_query> &queries, gsl_rng *rng) {
    lsm::record_t sample_buffer[queries[0].k];
    char *buffer1 = new char[4096];
    char *buffer2 = new char[4096];

    for (size_t i=0; i<queries.size(); i++) {
        extension->range_sample(sample_buffer, queries[i].lower_bound, queries[i].upper_bound,
                           queries[i].k, buffer1, buffer2, rng);
    }

    delete[] buffer1;
    delete[] buffer2;
}

static void insert_records(lsm::LSMTree *structure, size_t start, size_t stop, 
                           std::vector<kvp> &records, std::vector<size_t> &to_delete, 
                           size_t &delete_idx, bool delete_records, gsl_rng *rng) {

    size_t reccnt = 0;
    for (size_t i=start; i<stop; i++) {
        structure->append(records[i].key, records[i].value, false, rng);

        if (delete_records && gsl_rng_uniform(rng) <= 
            delete_proportion && to_delete[delete_idx] <= i) {

            structure->delete_record(records[to_delete[delete_idx]].key,
                                     records[to_delete[delete_idx]].value,
                                     rng);

            delete_idx++;
        }
    }
}



int main(int argc, char **argv)
{
    if (argc < 4) {
        fprintf(stderr, "Usage: vldb_irs_bench <record_count> <data_file> <query_file>\n");
        exit(EXIT_FAILURE);
    }

    size_t record_count = atol(argv[1]);
    std::string dfile = std::string(argv[2]);
    std::string qfile  = std::string(argv[3]);

    size_t memtable_size = 12000;
    size_t scale_factor = 6;
    double selectivity = .0001;
    size_t memory_levels = 1000;
    double max_delete_prop = 0.05;
	
	//double insert_batch = (argc == 10) ? atof(argv[9]) : 0.1;
	double insert_batch = 0.1;

    gsl_rng *rng = gsl_rng_alloc(gsl_rng_mt19937);

    std::string root_dir = "benchmarks/data/vldb_irs_bench";

    auto sampling_lsm = lsm::LSMTree(root_dir, memtable_size, memtable_size*0.05, scale_factor, memory_levels, max_delete_prop, rng);

    auto data = read_sosd_file<kvp>(dfile, record_count);
    std::vector<size_t> to_delete(record_count * delete_proportion);
    size_t j=0;
    for (size_t i=0; i<data.size() && j<to_delete.size(); i++) {
        if (gsl_rng_uniform(rng) <= delete_proportion) {
            to_delete[j++] = i;
        } 
    }

    auto queries = read_range_queries<irs_query>(qfile, selectivity);

    size_t delete_idx = 0;
    auto warmup = record_count * .1;
    insert_records(&sampling_lsm, 0, warmup, data, to_delete, delete_idx, true, rng);

    TIMER_INIT();

    TIMER_START();
    insert_records(&sampling_lsm, warmup, record_count, data, to_delete, delete_idx, true, rng);
    TIMER_STOP();

    auto insert_latency = TIMER_RESULT();
    size_t insert_throughput = (size_t) ((double) (record_count - warmup) / (double) insert_latency * 1e9);

    TIMER_START();
    run_queries(&sampling_lsm, queries, rng);
    TIMER_STOP();

    auto query_latency = TIMER_RESULT() / queries.size();

    auto ext_size = sampling_lsm.get_memory_utilization() + sampling_lsm.get_aux_memory_utilization();

    fprintf(stdout, "%ld\t%ld\t%ld\n", insert_throughput, query_latency, ext_size);


    exit(EXIT_SUCCESS);
}
