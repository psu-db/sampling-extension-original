#include <check.h>

#include "lsm/InMemRun.h"
#include "lsm/MemoryLevel.h"
#include "util/bf_config.h"

using namespace lsm;

gsl_rng *g_rng = gsl_rng_alloc(gsl_rng_mt19937);

START_TEST(t_memtable_init)
{
    auto mem_table = new MemTable(1024, true, 50, g_rng);
    for (uint64_t i = 512; i > 0; i--) {
        uint32_t v = i;
        mem_table->append((const char*)&i, (const char*)&v);
    }
    
    for (uint64_t i = 1; i <= 256; ++i) {
        uint32_t v = i;
        mem_table->append((const char*)&i, (const char*)&v, true);
    }

    for (uint64_t i = 257; i <= 512; ++i) {
        uint32_t v = i + 1;
        mem_table->append((const char*)&i, (const char*)&v);
    }

    BloomFilter* bf = new BloomFilter(BF_FPR, mem_table->get_tombstone_count(), BF_HASH_FUNCS, g_rng);
    InMemRun* run = new InMemRun(mem_table, bf);
    ck_assert_uint_eq(run->get_record_count(), 512);

    delete bf;
    delete mem_table;
    delete run;
}

Suite *unit_testing()
{
    Suite *unit = suite_create("InMemRun Unit Testing");
    TCase *create = tcase_create("lsm::InMemRun constructor Testing");

    tcase_add_test(create, t_memtable_init);
    tcase_set_timeout(create, 100);

    suite_add_tcase(unit, create);
    return unit;
}

int run_unit_tests()
{
    int failed = 0;
    Suite *unit = unit_testing();
    SRunner *unit_runner = srunner_create(unit);

    srunner_run_all(unit_runner, CK_NORMAL);
    failed = srunner_ntests_failed(unit_runner);
    srunner_free(unit_runner);

    return failed;
}


int main() 
{
    int unit_failed = run_unit_tests();

    return (unit_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
