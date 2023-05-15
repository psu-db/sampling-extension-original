#define ENABLE_TIMER

#include "bench.h"
#include "ds/ts/builder.h"

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "Usage: isam_benching <filename> <record_count> [osm_data]\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    bool osm = (argc == 4) ? atoi(argv[3]) : false;

    init_bench_env(record_count, true, osm);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    lsm::MemTable table0 = lsm::MemTable(record_count/4, true, record_count, g_rng);
    lsm::MemTable table1 = lsm::MemTable(record_count/4, true, record_count, g_rng);
    lsm::MemTable table2 = lsm::MemTable(record_count/4, true, record_count, g_rng);
    lsm::MemTable table3 = lsm::MemTable(record_count/4, true, record_count, g_rng);
    fill_memtable(&datafile, &table0, record_count/4, 0);
    fill_memtable(&datafile, &table1, record_count/4, 0);
    fill_memtable(&datafile, &table2, record_count/4, 0);
    fill_memtable(&datafile, &table3, record_count/4, 0);

    lsm::CHTRun *mem_isam[4];

    for (size_t i=0; i<4; i++) {
        mem_isam[i] = new lsm::CHTRun(&table0, nullptr, g_rng);
    }
    TIMER_INIT();

    fprintf(stderr, "Building...\n");

    TIMER_START();
    lsm::CHTRun *test_run = new lsm::CHTRun(mem_isam, 4, nullptr, false);
    TIMER_STOP();

    auto build_time = TIMER_RESULT();

    fprintf(stderr, "Querying...\n");

    std::vector<lsm::key_t> results(query_keys.size());
    TIMER_START();
    for (size_t i=0; i<query_keys.size(); i++) {
        results[i] = test_run->get_lower_bound(query_keys[i]);
        if (i % query_keys.size() == .01 * query_keys.size()) {
            progress_update((double) i / (double) query_keys.size(), "Querying:");
        }
    }
    TIMER_STOP();

    auto query_time = TIMER_RESULT() / query_keys.size();

    printf("%ld\t%ld\t%ld\n", build_time, query_time, test_run->get_memory_utilization());

    for (size_t i=0; i<results.size(); i++) {
        if (results[i] < test_run->get_record_count()) 
            fprintf(stderr, "%ld %ld %ld\n", results[i], query_keys[i], test_run->get_record_at(results[i])->key);
    }
    /*
    TIMER_INIT();
    TIMER_START();
    auto bldr = ts::Builder<lsm::key_t>(g_min_key, g_max_key, 100);
    for (size_t i=0; i<mem_isam->get_record_count(); i++) {
        bldr.AddKey(mem_isam->get_record_at(i)->key);
    }
    auto cht = bldr.Finalize();
    TIMER_STOP();

    fprintf(stdout, "%ld\n", TIMER_RESULT());
    */

    delete_bench_env();

    for (size_t i=0; i<4; i++) {
        delete mem_isam[i];
    }

    delete test_run;
    exit(EXIT_SUCCESS);
}
