#include <check.h>

#include "lsm/InMemRun.h"
#include "lsm/MemoryLevel.h"
#include "util/bf_config.h"

using namespace lsm;

gsl_rng *g_rng = gsl_rng_alloc(gsl_rng_mt19937);

std::string root_dir = "tests/data/memlevel_tests";

static MemTable *create_test_memtable(size_t cnt)
{
    auto mtable = new MemTable(cnt, true, 0, g_rng);

    for (size_t i = 0; i < cnt; i++) {
        key_type key = rand();
        value_type val = rand();

        mtable->append((char*) &key, (char*) &val);
    }

    return mtable;
}


static MemTable *create_double_seq_memtable(size_t cnt) 
{
    auto mtable = new MemTable(cnt, true, 0, g_rng);

    for (size_t i = 0; i < cnt / 2; i++) {
        key_type key = i;
        value_type val = i;

        mtable->append((char*) &key, (char *) &val);
    }

    for (size_t i = 0; i < cnt / 2; i++) {
        key_type key = i;
        value_type val = i + 1;

        mtable->append((char*) &key, (char *) &val);
    }

    return mtable;
}

START_TEST(t_memlevel_merge)
{
    auto tbl1 = create_test_memtable(100);
    auto tbl2 = create_test_memtable(100);

    auto base_level = new MemoryLevel(1, 1, root_dir);
    base_level->append_mem_table(tbl1, g_rng);
    ck_assert_int_eq(base_level->get_record_cnt(), 100);

    auto merging_level = new MemoryLevel(0, 1, root_dir);
    merging_level->append_mem_table(tbl2, g_rng);
    ck_assert_int_eq(merging_level->get_record_cnt(), 100);

    auto old_level = base_level;
    base_level = MemoryLevel::merge_levels(old_level, merging_level, g_rng);

    delete old_level;
    delete merging_level;
    ck_assert_int_eq(base_level->get_record_cnt(), 200);

    delete base_level;
    delete tbl1;
    delete tbl2;
}


Suite *unit_testing()
{
    Suite *unit = suite_create("MemoryLevel Unit Testing");

    TCase *merge = tcase_create("lsm::MemoryLevel::merge_level Testing");
    tcase_add_test(merge, t_memlevel_merge);
    suite_add_tcase(unit, merge);

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
