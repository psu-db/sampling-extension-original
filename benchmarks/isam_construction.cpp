#define ENABLE_TIMER

#include "bench.h"
#include "ds/ts/builder.h"

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "Usage: isam_construction <filename> <record_count> [external]\n");
        exit(EXIT_FAILURE);
    }

    std::string filename = std::string(argv[1]);
    size_t record_count = atol(argv[2]);
    bool external = (argc == 4) ? atoi(argv[3]) : false;

    init_bench_env(record_count, true, false);

    std::fstream datafile;
    datafile.open(filename, std::ios::in);

    lsm::MemTable table = lsm::MemTable(record_count, true, record_count, g_rng);
    fill_memtable(&datafile, &table, record_count, 0);
    lsm::InMemRun *mem_isam = nullptr;
    lsm::ISAMTree *ext_isam = nullptr;

    mem_isam = new lsm::InMemRun(&table, nullptr, g_rng);

    if (external) {
        auto pfile = lsm::PagedFile::create("benchmarks/data/isam_construction/tree.dat");
        assert(pfile);
        ext_isam = new lsm::ISAMTree(pfile, g_rng, nullptr, &mem_isam, 1, nullptr, 0);
    }

    TIMER_INIT();
    TIMER_START();
    auto bldr = ts::Builder<lsm::key_t>(g_min_key, g_max_key, 100);
    for (size_t i=0; i<mem_isam->get_record_count(); i++) {
        bldr.AddKey(mem_isam->get_record_at(i)->key);
    }
    auto cht = bldr.Finalize();
    TIMER_STOP();

    fprintf(stdout, "%ld\n", TIMER_RESULT());

    delete_bench_env();
    delete mem_isam;
    delete ext_isam;
    exit(EXIT_SUCCESS);
}
