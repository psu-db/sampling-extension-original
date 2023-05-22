#define ENABLE_TIMER

#include "bench.h"
#include "ds/ts/builder.h"

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "Usage: cht_construction <filename> <record_count> [osm_data]\n");
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
    size_t errors[] = {2, 4, 8, 16, 128, 256};

    for (size_t i=0; i<sizeof(errors); i++) {
        TIMER_INIT();

        TIMER_START();
        lsm::CHTRun *test_run = new lsm::CHTRun(mem_isam, 4, nullptr, false, errors[i]);
        TIMER_STOP();

        auto build_time = TIMER_RESULT();

        std::vector<lsm::key_t> results(query_keys.size());
        TIMER_START();
        for (size_t i=0; i<query_keys.size(); i++) {
            results[i] = test_run->get_lower_bound(query_keys[i]);
        }
        TIMER_STOP();

        auto query_time = TIMER_RESULT() / query_keys.size();

        printf("%ld\t%ld\t%ld\t%ld\n", errors[i], build_time, query_time, test_run->get_memory_utilization());

        /*
        for (size_t i=0; i<results.size(); i++) {
            if (results[i] < test_run->get_record_count()) 
                fprintf(stderr, "%ld %ld %ld\n", results[i], query_keys[i], test_run->get_record_at(results[i])->key);
        }
        */

        delete test_run;
    }

    for (size_t i=0; i<4; i++) {
        delete mem_isam[i];
    }

    delete_bench_env();

    exit(EXIT_SUCCESS);
}
