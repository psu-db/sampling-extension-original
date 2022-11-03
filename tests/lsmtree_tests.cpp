#include <check.h>

#include "lsm/LsmTree.h"

using namespace lsm;

gsl_rng *g_rng = gsl_rng_alloc(gsl_rng_mt19937);

std::string dir = "./tests/data/lsmtree";

START_TEST(t_create)
{
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, g_rng);

    ck_assert_ptr_nonnull(lsm);
    ck_assert_int_eq(lsm->get_record_cnt(), 0);
    ck_assert_int_eq(lsm->get_height(), 1);

    delete lsm;
}
END_TEST


START_TEST(t_append)
{
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, g_rng);

    key_type key = 0;
    value_type val = 0;
    for (size_t i=0; i<100; i++) {
        const char *key_ptr = (char *) &key;
        const char *val_ptr = (char *) &val;
        ck_assert_int_eq(lsm->append(key_ptr, val_ptr, 0, g_rng), 1);
        key++;
        val++;
    }

    ck_assert_int_eq(lsm->get_height(), 1);
    ck_assert_int_eq(lsm->get_record_cnt(), 100);

    delete lsm;
}
END_TEST


START_TEST(t_append_with_mem_merges)
{
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, g_rng);

    key_type key = 0;
    value_type val = 0;
    for (size_t i=0; i<300; i++) {
        const char *key_ptr = (char *) &key;
        const char *val_ptr = (char *) &val;
        ck_assert_int_eq(lsm->append(key_ptr, val_ptr, 0, g_rng), 1);
        key++;
        val++;
    }

    ck_assert_int_eq(lsm->get_record_cnt(), 300);
    ck_assert_int_eq(lsm->get_height(), 1);

    delete lsm;
}
END_TEST


START_TEST(t_append_with_disk_merges)
{
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, g_rng);

    key_type key = 0;
    value_type val = 0;
    for (size_t i=0; i<1000; i++) {
        const char *key_ptr = (char *) &key;
        const char *val_ptr = (char *) &val;
        ck_assert_int_eq(lsm->append(key_ptr, val_ptr, 0, g_rng), 1);
        key++;
        val++;
    }

    ck_assert_int_eq(lsm->get_record_cnt(), 1000);
    ck_assert_int_eq(lsm->get_height(), 3);

    delete lsm;
}
END_TEST


START_TEST(t_range_sample_memtable)
{
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, g_rng);

    key_type key = 0;
    value_type val = 0;
    for (size_t i=0; i<100; i++) {
        const char *key_ptr = (char *) &key;
        const char *val_ptr = (char *) &val;
        ck_assert_int_eq(lsm->append(key_ptr, val_ptr, 0, g_rng), 1);
        key++;
        val++;
    }

    key_type lower_bound = 20;
    key_type upper_bound = 50;

    char *buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);
    char *util_buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);
    char sample_set[100*record_size];

    lsm->range_sample(sample_set, (char*) &lower_bound, (char*) &upper_bound, 100, buf, util_buf, g_rng);

    for(size_t i=0; i<100; i++) {
        auto rec = sample_set + (record_size * i);
        auto s_key = *(key_type*) get_key(rec);
        auto s_val = *(value_type*) get_val(rec);

        ck_assert_int_le(s_key, upper_bound);
        ck_assert_int_ge(s_key, lower_bound);
    }

    free(buf);
    free(util_buf);

    delete lsm;
}
END_TEST


START_TEST(t_range_sample_memlevels)
{
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, g_rng);

    key_type key = 0;
    value_type val = 0;
    for (size_t i=0; i<300; i++) {
        const char *key_ptr = (char *) &key;
        const char *val_ptr = (char *) &val;
        ck_assert_int_eq(lsm->append(key_ptr, val_ptr, 0, g_rng), 1);
        key++;
        val++;
    }

    key_type lower_bound = 100;
    key_type upper_bound = 250;

    char *buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);
    char *util_buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);

    char sample_set[100*record_size];
    lsm->range_sample(sample_set, (char*) &lower_bound, (char*) &upper_bound, 100, buf, util_buf, g_rng);

    for(size_t i=0; i<100; i++) {
        auto rec = sample_set + (record_size * i);
        auto s_key = *(key_type*) get_key(rec);
        auto s_val = *(value_type*) get_val(rec);

        ck_assert_int_le(s_key, upper_bound);
        ck_assert_int_ge(s_key, lower_bound);
    }

    free(buf);
    free(util_buf);

    delete lsm;
}
END_TEST


START_TEST(t_range_sample_disklevels)
{
    auto lsm = new LSMTree(dir, 100, 100, 2, 1, g_rng);

    key_type key = 0;
    value_type val = 0;
    for (size_t i=0; i<1000; i++) {
        const char *key_ptr = (char *) &key;
        const char *val_ptr = (char *) &val;
        ck_assert_int_eq(lsm->append(key_ptr, val_ptr, 0, g_rng), 1);
        key++;
        val++;
    }

    key_type lower_bound = 0;
    key_type upper_bound = 1000;

    char *buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);
    char *util_buf = (char *) std::aligned_alloc(SECTOR_SIZE, PAGE_SIZE);

    char sample_set[100*record_size];
    lsm->range_sample(sample_set, (char*) &lower_bound, (char*) &upper_bound, 100, buf, util_buf, g_rng);

    for(size_t i=0; i<100; i++) {
        auto rec = sample_set + (record_size * i);
        auto s_key = *(key_type*) get_key(rec);
        auto s_val = *(value_type*) get_val(rec);

        ck_assert_int_le(s_key, upper_bound);
        ck_assert_int_ge(s_key, lower_bound);
    }

    free(buf);
    free(util_buf);

    delete lsm;
}
END_TEST


Suite *unit_testing()
{
    Suite *unit = suite_create("lsm::LSMTree Unit Testing");

    TCase *create = tcase_create("lsm::LSMTree::constructor Testing");
    tcase_add_test(create, t_create);

    suite_add_tcase(unit, create);

    TCase *append = tcase_create("lsm::LSMTree::append Testing");
    tcase_add_test(append, t_append);
    tcase_add_test(append, t_append_with_mem_merges);
    tcase_add_test(append, t_append_with_disk_merges);

    suite_add_tcase(unit, append);

    TCase *sampling = tcase_create("lsm::LSMTree::range_sample Testing");
    tcase_add_test(sampling, t_range_sample_memtable);
    tcase_add_test(sampling, t_range_sample_memlevels);
    tcase_add_test(sampling, t_range_sample_disklevels);

    suite_add_tcase(unit, sampling);

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
